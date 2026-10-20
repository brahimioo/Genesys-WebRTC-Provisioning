import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from uuid import UUID


def uid(value):
    return str(UUID(str(value)))


def flag(value):
    value = str(value).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError("Boolean configuration must be true or false")
    return value == "true"


@dataclass(frozen=True)
class Settings:
    environment: str
    client_id: str
    client_secret: str
    group_id: str
    site_id: str
    phone_base_id: str
    line_base_id: str
    skill_id: str = ""
    language_id: str = ""
    skill_proficiency: float = 0
    language_proficiency: float = 0
    dry_run: bool = True
    paused: bool = False
    default_policy: str = "if_missing"
    preferred_name_policy: str = "disabled"
    pilot_ids: tuple = ()
    given_names: dict = field(default_factory=dict)
    max_jobs: int = 50
    max_attempts: int = 8
    retry_seconds: int = 60
    reconcile_hours: int = 24
    tenant_id: str = ""
    admin_ids: tuple = ()
    storage_account: str = ""
    storage_connection: str = ""
    min_request_interval: float = 0.4
    scan_max_pages: int = 1000
    snapshot_max_age: int = 3600

    @property
    def org_key(self):
        return hashlib.sha256(
            (self.environment + ":" + self.client_id).encode()
        ).hexdigest()[:24]

    @property
    def fingerprint(self):
        policy = {
            k: getattr(self, k)
            for k in (
                "group_id", "site_id", "phone_base_id", "line_base_id", "skill_id",
                "language_id", "skill_proficiency", "language_proficiency", "dry_run",
                "default_policy", "preferred_name_policy", "pilot_ids", "given_names",
            )
        }
        return hashlib.sha256(
            json.dumps(policy, sort_keys=True).encode()
        ).hexdigest()[:24]

    @classmethod
    def from_env(cls):
        def required(name):
            value = os.environ.get(name, "").strip()
            if not value or value.startswith("@Microsoft.KeyVault("):
                raise ValueError("Missing or unresolved setting: " + name)
            return value

        environment = required("GENESYS_ENVIRONMENT")
        if not re.fullmatch(
            r"(mypurecloud\.(com|de|ie|com\.au|jp)|[a-z0-9-]+\.pure\.cloud)",
            environment,
        ):
            raise ValueError("GENESYS_ENVIRONMENT must be a Genesys region hostname")
        ids = {}
        for field, name in (
            ("client_id", "GENESYS_CLIENT_ID"), ("group_id", "SOURCE_GROUP_ID"),
            ("site_id", "SITE_ID"), ("phone_base_id", "PHONE_BASE_SETTINGS_ID"),
            ("line_base_id", "LINE_BASE_SETTINGS_ID"), ("tenant_id", "ENTRA_TENANT_ID"),
        ):
            ids[field] = uid(required(name))
        for field, name in (
            ("skill_id", "ROUTING_SKILL_ID"), ("language_id", "ROUTING_LANGUAGE_ID"),
        ):
            ids[field] = uid(os.environ[name]) if os.environ.get(name) else ""
        default_policy = os.getenv("DEFAULT_STATION_POLICY", "if_missing")
        preferred_policy = os.getenv("PREFERRED_NAME_POLICY", "disabled")
        if default_policy not in {"if_missing", "enforce"}:
            raise ValueError("DEFAULT_STATION_POLICY must be if_missing or enforce")
        if preferred_policy not in {"disabled", "fill_if_empty"}:
            raise ValueError("PREFERRED_NAME_POLICY must be disabled or fill_if_empty")
        given_names = {uid(k): str(v).strip() for k, v in json.loads(
            os.getenv("GIVEN_NAMES_JSON", "{}")
        ).items()}
        if any(not v or len(v) > 100 for v in given_names.values()):
            raise ValueError("Invalid GIVEN_NAMES_JSON")
        settings = cls(
            environment=environment, client_secret=required("GENESYS_CLIENT_SECRET"),
            **ids,
            dry_run=flag(os.getenv("DRY_RUN", "true")),
            paused=flag(os.getenv("PROVISIONING_PAUSED", "false")),
            default_policy=default_policy, preferred_name_policy=preferred_policy,
            given_names=given_names,
            pilot_ids=tuple(sorted(
                uid(x.strip()) for x in os.getenv("PILOT_USER_IDS", "").split(",") if x.strip()
            )),
            admin_ids=tuple(
                uid(x.strip()) for x in required("ADMIN_OBJECT_IDS").split(",") if x.strip()
            ),
            storage_account=os.getenv("STORAGE_ACCOUNT_NAME", ""),
            storage_connection=os.getenv("AzureWebJobsStorage", ""),
            skill_proficiency=float(os.getenv("SKILL_PROFICIENCY", "0")),
            language_proficiency=float(os.getenv("LANGUAGE_PROFICIENCY", "0")),
            max_jobs=int(os.getenv("MAX_JOBS_PER_SCAN", "50")),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "8")),
            retry_seconds=int(os.getenv("RETRY_SECONDS", "60")),
            reconcile_hours=int(os.getenv("RECONCILE_HOURS", "24")),
            min_request_interval=float(os.getenv("MIN_REQUEST_INTERVAL", "0.4")),
        )
        if not settings.storage_account and not settings.storage_connection:
            raise ValueError("STORAGE_ACCOUNT_NAME is required for managed identity")
        if not 0 <= settings.skill_proficiency <= 5 or not 0 <= settings.language_proficiency <= 5:
            raise ValueError("Proficiency must be between 0 and 5")
        if not 1 <= settings.max_jobs <= 1000 or not 1 <= settings.max_attempts <= 50:
            raise ValueError("Invalid MAX_JOBS_PER_SCAN or MAX_ATTEMPTS")
        if settings.min_request_interval < 0.1 or settings.reconcile_hours < 1:
            raise ValueError("Invalid request interval or reconciliation interval")
        return settings
