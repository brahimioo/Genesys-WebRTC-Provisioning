import random
import time

from .errors import ProvisioningError
from .telemetry import emit


def default_station_id(state):
    # associatedStation/effectiveStation are deliberately NOT accepted as defaults.
    value = state.get("defaultStation") if state else None
    return value.get("id") if isinstance(value, dict) else None


class Context:
    def __init__(self, store, doc, etag):
        self.store, self.doc, self.etag = store, doc, etag
        self.plan = []
        self.warnings = []

    def checkpoint(self):
        self.etag = self.store.save_user(self.doc, self.etag)

    def complete(self, step, value="verified"):
        self.doc.setdefault("steps", {})[step] = value
        self.checkpoint()

    def change(self, step, before, after, action):
        if self.store.paused():
            raise ProvisioningError("provisioning_paused", True, 300)
        self.store.audit(self.doc["user_id"], step, before, after, "intent",
                             generation=self.doc["generation"])
        self.doc.setdefault("steps", {})[step] = "intent"
        self.checkpoint()
        try:
            result = action()
        except ProvisioningError as error:
            outcome = "unknown" if error.code == "api_network_outcome_unknown" else "rejected_or_failed"
            self.store.audit(self.doc["user_id"], step, before, after, outcome,
                             generation=self.doc["generation"])
            raise
        self.store.audit(self.doc["user_id"], step, before, after, "accepted",
                             generation=self.doc["generation"])
        return result


class Provisioner:
    def __init__(self, settings, api, store):
        self.settings, self.api, self.store = settings, api, store

    def process(self, job):
        if self.store.paused():
            return  # Durable pending state is picked up by recovery after resume.
        claimed = self.store.claim(job)
        if not claimed:
            return
        ctx = Context(self.store, *claimed)
        doc = ctx.doc
        try:
            if doc["fingerprint"] != self.settings.fingerprint:
                doc["status"] = "cancelled"  # A fresh scan schedules the current policy.
                return
            if doc["attempts"] > self.settings.max_attempts:
                raise ProvisioningError("attempt_budget_exhausted")
            if not self.store.eligible(doc["user_id"]):
                doc["status"] = "excluded"
                return
            user = self.api.user(doc["user_id"])
            if not user or user.get("state") != "active":
                doc["status"] = "excluded"
                return
            if user.get("id") != doc["user_id"]:
                raise ProvisioningError("unexpected_user_response")
            self.ensure_user(ctx, user)
            doc.update(
                status="dry_run" if self.settings.dry_run else (
                    "succeeded_with_warnings" if ctx.warnings else "succeeded"
                ), verified_at=time.time(), last_error="",
                plan=ctx.plan, warnings=ctx.warnings,
            )
            emit("user_completed", user_id=doc["user_id"], status=doc["status"],
                 attempt=doc["attempts"], planned_changes=len(ctx.plan))
        except ProvisioningError as error:
            retry = error.retryable and doc["attempts"] < self.settings.max_attempts
            doc.update(
                status="retry" if retry else "blocked", last_error=error.code,
                due_at=time.time() + max(error.retry_after, min(
                    self.settings.retry_seconds * (2 ** min(doc["attempts"] - 1, 6)), 3600
                )) + random.randint(0, 10),
            )
            emit("user_retry" if retry else "user_failed",
                 user_id=doc["user_id"], code=error.code, attempt=doc["attempts"])
        except Exception as error:
            # Do not log raw exception messages, URLs, responses, tokens, or user profiles.
            retry = doc["attempts"] < self.settings.max_attempts
            doc.update(status="retry" if retry else "blocked",
                       last_error="unexpected_" + type(error).__name__,
                       due_at=time.time() + 300)
            emit("user_retry" if retry else "user_failed",
                 user_id=doc["user_id"], code=doc["last_error"], attempt=doc["attempts"])
        finally:
            doc["lease_until"] = 0
            doc["plan"] = ctx.plan
            doc["warnings"] = ctx.warnings
            ctx.checkpoint()
        if doc["status"] == "retry":
            # If enqueue fails, leave the persisted outbox for recovery.
            self.store.enqueue(doc)

    def ensure_user(self, ctx, user):
        user_id = user["id"]
        phone = self.api.phone(user_id)
        if not phone:
            ctx.plan.append({"operation": "create_phone"})
            if not self.settings.dry_run:
                if ctx.doc.get("create_in_flight"):
                    raise ProvisioningError("phone_creation_outcome_unknown", True, 120)
                ctx.doc["create_in_flight"] = True
                ctx.checkpoint()  # Persist intent before a non-idempotent POST.
                try:
                    phone = ctx.change("phone", None, {
                        "siteId": self.settings.site_id,
                        "phoneBaseSettingsId": self.settings.phone_base_id,
                    }, lambda: self.api.create_phone(user))
                except ProvisioningError as error:
                    if not error.uncertain:
                        # A definite rejection did not create a phone.
                        ctx.doc["create_in_flight"] = False
                        ctx.checkpoint()
                    raise
                if not phone.get("id"):
                    raise ProvisioningError("phone_create_response_missing_id", True, 60)
        if phone:
            if ctx.doc.get("create_in_flight"):
                ctx.doc["create_in_flight"] = False
            ctx.doc["phone_id"] = phone["id"]
            ctx.complete("phone")
            if ((phone.get("site") or {}).get("id") != self.settings.site_id or
                    (phone.get("phoneBaseSettings") or {}).get("id") != self.settings.phone_base_id):
                ctx.warnings.append("existing_phone_profile_differs")
        station_id = self.api.station(user_id) if phone else None
        if not station_id and not self.settings.dry_run:
            raise ProvisioningError("station_not_yet_available", True, 30)
        state = self.api.station_state(user_id)
        current_default = default_station_id(state)
        if station_id and current_default == station_id:
            ctx.complete("default_station")
        elif current_default and self.settings.default_policy == "if_missing":
            ctx.warnings.append("existing_default_preserved")
            ctx.complete("default_station", "preserved_existing")
        else:
            ctx.plan.append({"operation": "set_default_station",
                             "stationId": station_id or "<new-station>"})
            if not self.settings.dry_run:
                ctx.change("default_station", current_default, station_id,
                           lambda: self.api.set_default(user_id, station_id))
                if default_station_id(self.api.station_state(user_id)) != station_id:
                    raise ProvisioningError("default_station_not_verified", True, 30)
                ctx.complete("default_station")
        if station_id:
            ctx.doc["station_id"] = station_id
            ctx.checkpoint()
        for kind, resource_id, proficiency in (
            ("routingskills", self.settings.skill_id, self.settings.skill_proficiency),
            ("routinglanguages", self.settings.language_id, self.settings.language_proficiency),
        ):
            if not resource_id:
                continue
            assigned = self.api.routing(user_id, kind)
            if not any(x.get("id") == resource_id for x in assigned):
                ctx.plan.append({"operation": "add_" + kind, "id": resource_id,
                                 "proficiency": proficiency})
                if not self.settings.dry_run:
                    ctx.change(kind, None, {"id": resource_id, "proficiency": proficiency},
                               lambda: self.api.add_routing(user_id, kind, resource_id, proficiency))
                    if not any(x.get("id") == resource_id for x in self.api.routing(user_id, kind)):
                        raise ProvisioningError(kind + "_not_verified", True, 30)
            if not self.settings.dry_run:
                ctx.complete(kind)
        if self.settings.preferred_name_policy == "fill_if_empty":
            current = self.api.user(user_id)
            desired = self.settings.given_names.get(user_id, "")
            if not current.get("preferredName") and desired:
                ctx.plan.append({"operation": "fill_preferred_name"})
                if not self.settings.dry_run:
                    if not isinstance(current.get("version"), int):
                        raise ProvisioningError("user_version_missing")
                    ctx.change("preferred_name", None, desired,
                               lambda: self.api.set_preferred(current, desired))
                    if (self.api.user(user_id) or {}).get("preferredName") != desired:
                        raise ProvisioningError("preferred_name_not_verified", True, 30)
                    ctx.complete("preferred_name")
            elif not current.get("preferredName") and not desired:
                ctx.warnings.append("authoritative_given_name_missing")
