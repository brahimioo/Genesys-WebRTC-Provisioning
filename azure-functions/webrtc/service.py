import time
from uuid import UUID

from .config import Settings
from .errors import ProvisioningError
from .genesys import Genesys
from .provisioning import Provisioner
from .storage import RateLimiter, Store
from .telemetry import emit


class Service:
    def __init__(self, settings=None, api=None, store=None):
        self.settings = settings or Settings.from_env()
        self.store = store or Store(self.settings)
        self.api = api or Genesys(self.settings, RateLimiter(self.store))
        self.provisioner = Provisioner(self.settings, self.api, self.store)

    def scan(self, force=False):
        if self.store.paused():
            emit("scan_paused")
            return 0
        owner = self.store.acquire_control("scan")
        if not owner:
            return 0
        try:
            self.api.preflight()
            members = self.api.members()  # Complete inventory before publication or enqueue.
            users = sorted({
                str(UUID(u["id"])) for u in members if u.get("state") == "active"
                and (not self.settings.pilot_ids or u["id"] in self.settings.pilot_ids)
            })
            scan_id = self.store.publish_members(users)
            count = 0
            for user_id in users:
                if count >= self.settings.max_jobs:
                    break
                if self.store.schedule(user_id, force=force):
                    count += 1
            emit("scan_completed", scan_id=scan_id, eligible=len(users), scheduled=count,
                 dry_run=self.settings.dry_run)
            return count
        except Exception as error:
            emit("scan_failed", code=error.code if isinstance(error, ProvisioningError)
                 else type(error).__name__)
            raise
        finally:
            self.store.release_control("scan", owner)

    def maintenance(self):
        if self.store.paused():
            return
        recovered = self.store.recover()
        removed = self.store.cleanup()
        emit("maintenance_completed", recovered=recovered, expired_records_removed=removed)

    def request_user(self, user_id, reset_create_intent=False):
        user_id = str(UUID(user_id))
        if not self.store.eligible(user_id):
            raise ProvisioningError("user_outside_scope")
        if self.store.paused():
            raise ProvisioningError("provisioning_paused", True, 300)
        if reset_create_intent:
            doc, etag = self.store.get_user(user_id)
            if doc and doc.get("lease_until", 0) > time.time():
                raise ProvisioningError("user_busy", True, 60)
            if doc:
                # Explicit administrator acknowledgement; never reset automatically.
                self.store.audit(user_id, "reset_create_intent",
                                 doc.get("create_in_flight", False), False, "administrator")
                doc["create_in_flight"] = False
                self.store.save_user(doc, etag)
        return self.store.schedule(user_id, force=True)
