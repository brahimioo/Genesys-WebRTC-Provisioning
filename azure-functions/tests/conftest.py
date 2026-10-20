import copy
import json
import re
from dataclasses import replace
from uuid import UUID

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, ResourceModifiedError

from webrtc.config import Settings
from webrtc.errors import ProvisioningError
from webrtc.storage import Store

USER = str(UUID(int=100))
SITE = str(UUID(int=101))
BASE = str(UUID(int=102))
LINE = str(UUID(int=103))
SKILL = str(UUID(int=104))
LANGUAGE = str(UUID(int=105))
PHONE = str(UUID(int=106))
STATION = str(UUID(int=107))
TENANT = str(UUID(int=108))
ADMIN = str(UUID(int=109))


class Entity(dict):
    def __init__(self, values, etag):
        super().__init__(copy.deepcopy(values))
        self.metadata = {"etag": str(etag)}


class MemoryTable:
    def __init__(self):
        self.rows = {}
        self.version = 0

    def create_entity(self, entity):
        key = entity["PartitionKey"], entity["RowKey"]
        if key in self.rows:
            raise ResourceExistsError("exists")
        self.version += 1
        self.rows[key] = Entity(entity, self.version)

    def get_entity(self, partition, row):
        try:
            value = self.rows[partition, row]
            return Entity(value, value.metadata["etag"])
        except KeyError:
            raise ResourceNotFoundError("missing") from None

    def update_entity(self, entity, mode=None, etag=None, match_condition=None):
        key = entity["PartitionKey"], entity["RowKey"]
        if key not in self.rows or self.rows[key].metadata["etag"] != etag:
            raise ResourceModifiedError("etag mismatch")
        self.version += 1
        self.rows[key] = Entity(entity, self.version)

    def submit_transaction(self, operations):
        for _, entity in operations:
            self.create_entity(entity)

    def query_entities(self, query):
        def matches(row):
            for field, op, literal in re.findall(r"(\w+) (eq|lt) ('[^']*'|[0-9.]+)", query):
                expected = literal[1:-1] if literal.startswith("'") else float(literal)
                actual = row.get(field)
                if actual is None or (op == "eq" and actual != expected) or (
                    op == "lt" and not actual < expected
                ):
                    return False
            return True
        return [Entity(v, v.metadata["etag"]) for v in self.rows.values() if matches(v)]

    def delete_entity(self, partition, row):
        del self.rows[partition, row]


class MemoryQueue:
    def __init__(self):
        self.messages = []
        self.fail = False

    def send_message(self, text, **kwargs):
        if self.fail:
            raise RuntimeError("queue unavailable")
        self.messages.append((json.loads(text), kwargs))


@pytest.fixture
def settings():
    return Settings(
        environment="mypurecloud.de", client_id=str(UUID(int=110)), client_secret="test-secret",
        group_id=str(UUID(int=111)), site_id=SITE, phone_base_id=BASE, line_base_id=LINE,
        skill_id=SKILL, language_id=LANGUAGE, dry_run=False,
        tenant_id=TENANT, admin_ids=(ADMIN,), given_names={USER: "Alex"},
    )


def memory_store(settings):
    store = object.__new__(Store)
    store.settings, store.partition = settings, settings.org_key
    store.state, store.control = MemoryTable(), MemoryTable()
    store.members, store.audit_table = MemoryTable(), MemoryTable()
    store.queue, store.scan_queue = MemoryQueue(), MemoryQueue()
    store.publish_members([USER])
    return store


@pytest.fixture
def store(settings):
    return memory_store(settings)


class FakeApi:
    def __init__(self, settings):
        self.settings = settings
        self.person = {"id": USER, "name": "Alex Example", "state": "active", "version": 1}
        self.existing_phone = None
        self.default = None
        self.skills, self.languages = [], []
        self.writes = []
        self.create_timeout = False
        self.create_timeout_without_commit = False
        self.phone_lookup_error = False
        self.language_failure = False
        self.default_no_change = False

    def preflight(self):
        pass

    def members(self):
        return [copy.deepcopy(self.person)]

    def user(self, user_id):
        return copy.deepcopy(self.person)

    def phone(self, user_id):
        if self.phone_lookup_error:
            raise ProvisioningError("api_http_403", status=403)
        return copy.deepcopy(self.existing_phone)

    def create_phone(self, user):
        self.writes.append("phone")
        if not self.create_timeout_without_commit:
            self.existing_phone = {
                "id": PHONE, "webRtcUser": {"id": USER},
                "site": {"id": SITE}, "phoneBaseSettings": {"id": BASE},
            }
        if self.create_timeout or self.create_timeout_without_commit:
            raise ProvisioningError("api_network_outcome_unknown", True, 60)
        return copy.deepcopy(self.existing_phone)

    def station(self, user_id):
        return STATION if self.existing_phone else None

    def station_state(self, user_id):
        return {
            "defaultStation": {"id": self.default} if self.default else None,
            "associatedStation": {"id": STATION},
            "effectiveStation": {"id": STATION},
        }

    def set_default(self, user_id, station_id):
        self.writes.append("default")
        if not self.default_no_change:
            self.default = station_id

    def routing(self, user_id, kind):
        return copy.deepcopy(self.skills if kind == "routingskills" else self.languages)

    def add_routing(self, user_id, kind, resource_id, proficiency):
        self.writes.append(kind)
        if kind == "routinglanguages" and self.language_failure:
            self.language_failure = False
            raise ProvisioningError("api_http_503", True, 60, 503)
        target = self.skills if kind == "routingskills" else self.languages
        target.append({"id": resource_id, "proficiency": proficiency})

    def set_preferred(self, user, value):
        self.writes.append("preferred_name")
        self.person["preferredName"] = value


@pytest.fixture
def api(settings):
    return FakeApi(settings)


def queued(store):
    store.schedule(USER)
    return store.queue.messages[-1][0]


def make_due(store):
    doc, etag = store.get_user(USER)
    doc["due_at"] = 0
    doc["enqueued_at"] = 0
    store.save_user(doc, etag)


def dry(settings):
    return replace(settings, dry_run=True)
