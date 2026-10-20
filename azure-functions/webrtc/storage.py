import json
import random
import time
import uuid
from datetime import datetime, timezone

from azure.core import MatchConditions
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, ResourceModifiedError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

from .errors import ProvisioningError


def stamp():
    return datetime.now(timezone.utc).isoformat()


def entity_copy(entity):
    return {k: v for k, v in entity.items() if k not in {"Timestamp", "odata.etag"}}


class Store:
    def __init__(self, settings):
        self.settings = settings
        self.partition = settings.org_key
        if settings.storage_connection:
            service = TableServiceClient.from_connection_string(settings.storage_connection)
            self.queue = QueueClient.from_connection_string(
                settings.storage_connection, "webrtc-jobs",
                message_encode_policy=TextBase64EncodePolicy(),
            )
            self.scan_queue = QueueClient.from_connection_string(
                settings.storage_connection, "webrtc-scans",
                message_encode_policy=TextBase64EncodePolicy(),
            )
        else:
            credential = DefaultAzureCredential()
            service = TableServiceClient(
                "https://" + settings.storage_account + ".table.core.windows.net", credential
            )
            self.queue = QueueClient(
                "https://" + settings.storage_account + ".queue.core.windows.net",
                "webrtc-jobs", credential,
                message_encode_policy=TextBase64EncodePolicy(),
            )
            self.scan_queue = QueueClient(
                "https://" + settings.storage_account + ".queue.core.windows.net",
                "webrtc-scans", credential,
                message_encode_policy=TextBase64EncodePolicy(),
            )
        self.state = service.get_table_client("WebRtcState")
        self.control = service.get_table_client("WebRtcControl")
        self.members = service.get_table_client("WebRtcMembers")
        self.audit_table = service.get_table_client("WebRtcAudit")

    def read(self, table, row, partition=None):
        try:
            entity = table.get_entity(partition or self.partition, row)
            return entity_copy(entity), entity.metadata["etag"]
        except ResourceNotFoundError:
            return None, None

    def write(self, table, entity, etag):
        try:
            if etag is None:
                table.create_entity(entity)
            else:
                table.update_entity(
                    entity, mode=UpdateMode.REPLACE, etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
        except (ResourceExistsError, ResourceModifiedError):
            raise ProvisioningError("concurrent_state_change", True, 15) from None

    def get_user(self, user_id):
        row, etag = self.read(self.state, user_id)
        return (json.loads(row["data"]), etag) if row else (None, None)

    def save_user(self, doc, etag):
        row = {
            "PartitionKey": self.partition, "RowKey": doc["user_id"],
            "status": doc["status"], "due_at": float(doc.get("due_at", 0)),
            "lease_until": float(doc.get("lease_until", 0)),
            "updated_at": stamp(), "data": json.dumps(doc, separators=(",", ":")),
        }
        self.write(self.state, row, etag)
        return self.get_user(doc["user_id"])[1]

    def users(self):
        query = "PartitionKey eq '" + self.partition + "'"
        for row in self.state.query_entities(query):
            yield json.loads(row["data"])

    def schedule(self, user_id, force=False):
        now = time.time()
        doc, etag = self.get_user(user_id)
        changed_policy = doc and doc.get("fingerprint") != self.settings.fingerprint
        if doc:
            if doc.get("lease_until", 0) > now:
                return False
            if not force and not changed_policy:
                if doc["status"] in {"blocked", "failed"}:
                    return False
                if doc["status"] in {"pending", "retry", "running"}:
                    # Recovery is responsible for these jobs; preserve attempt counters.
                    return False
                if doc.get("verified_at", 0) > now - self.settings.reconcile_hours * 3600:
                    return False
        if not doc:
            doc = {"user_id": user_id, "attempts": 0, "steps": {}, "created_at": stamp()}
        if changed_policy or force or doc.get("status") in {
            "succeeded", "succeeded_with_warnings", "dry_run", "excluded", "cancelled",
        }:
            doc["attempts"] = 0
        doc.update(
            status="pending", generation=str(uuid.uuid4()),
            fingerprint=self.settings.fingerprint, due_at=now, lease_until=0,
            last_error="", enqueued_at=0, first_queued_at=now,
        )
        etag = self.save_user(doc, etag)
        self.enqueue(doc)
        doc["enqueued_at"] = now
        self.save_user(doc, etag)
        return True

    def enqueue(self, doc):
        delay = max(0, min(int(doc.get("due_at", 0) - time.time()), 604800))
        self.queue.send_message(json.dumps({
            "schemaVersion": 1, "userId": doc["user_id"],
            "generation": doc["generation"], "fingerprint": doc["fingerprint"],
        }), visibility_timeout=delay, time_to_live=-1)

    def claim(self, job):
        doc, etag = self.get_user(job["userId"])
        now = time.time()
        if not doc or doc.get("generation") != job["generation"]:
            return None
        if doc.get("fingerprint") != job["fingerprint"]:
            return None
        if doc["status"] not in {"pending", "retry", "running"}:
            return None
        if doc.get("lease_until", 0) > now or doc.get("due_at", 0) > now + 1:
            return None
        doc.update(status="running", lease_until=now + 900, owner=str(uuid.uuid4()))
        doc["attempts"] = doc.get("attempts", 0) + 1
        return doc, self.save_user(doc, etag)

    def recover(self):
        now = time.time()
        count = 0
        for snapshot in self.users():
            if count >= self.settings.max_jobs:
                break
            if snapshot["status"] not in {"pending", "retry", "running"}:
                continue
            if snapshot.get("lease_until", 0) > now or snapshot.get("due_at", 0) > now:
                continue
            if snapshot.get("enqueued_at", 0) > now - 240:
                continue
            doc, etag = self.get_user(snapshot["user_id"])
            if not doc or doc.get("generation") != snapshot.get("generation"):
                continue
            # Keep the durable outbox entry recoverable if sending fails.
            self.enqueue(doc)
            doc["enqueued_at"] = now
            self.save_user(doc, etag)
            count += 1
        return count

    def acquire_control(self, name, seconds=900):
        row, etag = self.read(self.control, name)
        now = time.time()
        if row and row.get("lease_until", 0) > now:
            return None
        owner = str(uuid.uuid4())
        row = row or {"PartitionKey": self.partition, "RowKey": name}
        row.update(lease_until=now + seconds, owner=owner)
        self.write(self.control, row, etag)
        return owner

    def release_control(self, name, owner):
        row, etag = self.read(self.control, name)
        if row and row.get("owner") == owner:
            row["lease_until"] = 0
            self.write(self.control, row, etag)

    def publish_members(self, users):
        scan_id = self.partition + "-" + str(uuid.uuid4())
        expiry = time.time() + 172800
        rows = [{
            "PartitionKey": scan_id, "RowKey": user_id, "expires_at": expiry,
        } for user_id in sorted(set(users))]
        for offset in range(0, len(rows), 100):
            self.members.submit_transaction([
                ("create", row) for row in rows[offset:offset + 100]
            ])
        old, etag = self.read(self.control, "membership")
        row = {
            "PartitionKey": self.partition, "RowKey": "membership",
            "scan_id": scan_id, "completed_at": time.time(), "count": len(rows),
        }
        self.write(self.control, row, etag)
        return scan_id

    def eligible(self, user_id):
        if self.settings.pilot_ids and user_id not in self.settings.pilot_ids:
            return False
        row, _ = self.read(self.control, "membership")
        if not row or row["completed_at"] < time.time() - self.settings.snapshot_max_age:
            raise ProvisioningError("membership_snapshot_stale", True, 60)
        member, _ = self.read(self.members, user_id, row["scan_id"])
        return member is not None

    def paused(self):
        row, _ = self.read(self.control, "pause")
        return self.settings.paused or bool(row and row.get("paused"))

    def set_pause(self, paused):
        row, etag = self.read(self.control, "pause")
        row = row or {"PartitionKey": self.partition, "RowKey": "pause"}
        row.update(paused=paused, updated_at=stamp())
        self.write(self.control, row, etag)

    def audit(self, user_id, operation, before, after, outcome, generation=""):
        self.audit_table.create_entity({
            "PartitionKey": self.partition, "RowKey": str(uuid.uuid4()),
            "user_id": user_id, "operation": operation, "outcome": outcome,
            "generation": generation,
            "created_at": stamp(), "expires_at": time.time() + 90 * 86400,
            "before": json.dumps(before, ensure_ascii=False),
            "after": json.dumps(after, ensure_ascii=False),
        })

    def enqueue_scan(self):
        request_id = str(uuid.uuid4())
        self.scan_queue.send_message(json.dumps({"requestId": request_id}))
        return request_id

    def list_status(self, status=None, limit=100):
        query = "PartitionKey eq '" + self.partition + "'"
        if status:
            query += " and status eq '" + status + "'"
        rows = []
        for row in self.state.query_entities(query):
            rows.append(json.loads(row["data"]))
            if len(rows) > limit:
                break
        return rows[:limit], len(rows) > limit

    def user_audit(self, user_id, limit=100):
        rows = []
        query = "PartitionKey eq '" + self.partition + "' and user_id eq '" + user_id + "'"
        for row in self.audit_table.query_entities(query):
            rows.append(entity_copy(row))
            if len(rows) >= limit:
                break
        return rows

    def cleanup(self, limit=500):
        count = 0
        for table in (self.members, self.audit_table):
            for row in table.query_entities("expires_at lt " + str(time.time())):
                # Only remove records owned by this client/organization.
                if not row["PartitionKey"].startswith(self.partition):
                    continue
                table.delete_entity(row["PartitionKey"], row["RowKey"])
                count += 1
                if count >= limit:
                    return count
        return count


class RateLimiter:
    """Shared quota reservation and cooldown across scanner and worker instances."""

    def __init__(self, store):
        self.store = store

    def acquire(self):
        for _ in range(10):
            row, etag = self.store.read(self.store.control, "rate")
            now = time.time()
            row = row or {"PartitionKey": self.store.partition, "RowKey": "rate"}
            wait = max(row.get("next_at", 0), row.get("blocked_until", 0)) - now
            if wait > 2:
                raise ProvisioningError("shared_api_cooldown", True, int(wait) + 1)
            if wait > 0:
                time.sleep(wait)
                continue
            row["next_at"] = now + self.store.settings.min_request_interval
            try:
                self.store.write(self.store.control, row, etag)
                return
            except ProvisioningError as error:
                if error.code != "concurrent_state_change":
                    raise
                time.sleep(random.uniform(0.02, 0.08))
        raise ProvisioningError("rate_reservation_busy", True, 5)

    def block(self, seconds):
        for _ in range(10):
            row, etag = self.store.read(self.store.control, "rate")
            row = row or {"PartitionKey": self.store.partition, "RowKey": "rate"}
            row["blocked_until"] = max(row.get("blocked_until", 0), time.time() + seconds)
            try:
                self.store.write(self.store.control, row, etag)
                return
            except ProvisioningError as error:
                if error.code != "concurrent_state_change":
                    raise
        raise ProvisioningError("rate_cooldown_write_busy", True, seconds)
