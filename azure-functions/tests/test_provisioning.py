from dataclasses import replace

import pytest

from conftest import (
    LANGUAGE, SKILL, STATION, USER, FakeApi, make_due, memory_store, queued,
)
from webrtc.errors import ProvisioningError
from webrtc.provisioning import Provisioner, default_station_id
from webrtc.service import Service


def test_full_provisioning_is_idempotent(settings, api, store):
    processor = Provisioner(settings, api, store)
    processor.process(queued(store))
    assert store.get_user(USER)[0]["status"] == "succeeded"
    first_writes = list(api.writes)
    assert first_writes == ["phone", "default", "routingskills", "routinglanguages"]
    store.schedule(USER, force=True)
    processor.process(store.queue.messages[-1][0])
    assert api.writes == first_writes
    assert store.get_user(USER)[0]["status"] == "succeeded"


def test_dry_run_has_no_genesys_writes(settings):
    settings = replace(settings, dry_run=True)
    store, api = memory_store(settings), FakeApi(settings)
    Provisioner(settings, api, store).process(queued(store))
    doc, _ = store.get_user(USER)
    assert doc["status"] == "dry_run"
    assert {x["operation"] for x in doc["plan"]} == {
        "create_phone", "set_default_station", "add_routingskills", "add_routinglanguages",
    }
    assert api.writes == []


def test_partial_failure_repairs_language_without_recreating_phone(settings, api, store):
    api.language_failure = True
    processor = Provisioner(settings, api, store)
    job = queued(store)
    processor.process(job)
    assert store.get_user(USER)[0]["status"] == "retry"
    make_due(store)
    processor.process(job)
    assert store.get_user(USER)[0]["status"] == "succeeded"
    assert api.writes.count("phone") == 1
    assert api.writes.count("routingskills") == 1
    assert api.writes.count("routinglanguages") == 2


def test_lost_create_response_does_not_duplicate_phone(settings, api, store):
    api.create_timeout = True
    processor = Provisioner(settings, api, store)
    job = queued(store)
    processor.process(job)
    assert store.get_user(USER)[0]["create_in_flight"] is True
    make_due(store)
    processor.process(job)
    assert store.get_user(USER)[0]["status"] == "succeeded"
    assert api.writes.count("phone") == 1


def test_unknown_create_outcome_stays_blocked_from_another_post(settings, api, store):
    api.create_timeout_without_commit = True
    job = queued(store)
    processor = Provisioner(settings, api, store)
    processor.process(job)
    make_due(store)
    processor.process(job)
    doc, _ = store.get_user(USER)
    assert doc["status"] == "retry"
    assert doc["last_error"] == "phone_creation_outcome_unknown"
    assert api.writes == ["phone"]


def test_failed_lookup_never_means_missing_phone(settings, api, store):
    api.phone_lookup_error = True
    Provisioner(settings, api, store).process(queued(store))
    assert store.get_user(USER)[0]["status"] == "blocked"
    assert api.writes == []


def test_default_must_be_verified_even_when_effective_station_matches(settings, api, store):
    api.default_no_change = True
    Provisioner(settings, api, store).process(queued(store))
    doc, _ = store.get_user(USER)
    assert doc["status"] == "retry"
    assert doc["last_error"] == "default_station_not_verified"


def test_associated_station_is_not_a_default():
    assert default_station_id({"associatedStation": {"id": STATION}}) is None
    assert default_station_id({"effectiveStation": {"id": STATION}}) is None


def test_existing_default_is_preserved(settings, api, store):
    api.default = "existing-other-station"
    Provisioner(settings, api, store).process(queued(store))
    assert api.default == "existing-other-station"
    assert "default" not in api.writes
    assert "existing_default_preserved" in store.get_user(USER)[0]["warnings"]


def test_existing_proficiency_is_preserved(settings, api, store):
    api.skills = [{"id": SKILL, "proficiency": 5}]
    api.languages = [{"id": LANGUAGE, "proficiency": 4}]
    Provisioner(settings, api, store).process(queued(store))
    assert api.skills[0]["proficiency"] == 5
    assert api.languages[0]["proficiency"] == 4
    assert "routingskills" not in api.writes


def test_pause_preserves_pending_work(settings, api, store):
    job = queued(store)
    store.set_pause(True)
    Provisioner(settings, api, store).process(job)
    assert store.get_user(USER)[0]["status"] == "pending"
    assert api.writes == []
    store.set_pause(False)
    Provisioner(settings, api, store).process(job)
    assert store.get_user(USER)[0]["status"] == "succeeded"


def test_user_removed_from_group_is_excluded(settings, api, store):
    job = queued(store)
    store.publish_members([])
    Provisioner(settings, api, store).process(job)
    assert store.get_user(USER)[0]["status"] == "excluded"
    assert api.writes == []


def test_inactive_user_is_excluded(settings, api, store):
    api.person["state"] = "inactive"
    Provisioner(settings, api, store).process(queued(store))
    assert store.get_user(USER)[0]["status"] == "excluded"
    assert api.writes == []


def test_attempt_budget_persists_across_scans(settings, api, store):
    settings = replace(settings, max_attempts=2)
    api.create_timeout_without_commit = True
    job = queued(store)
    processor = Provisioner(settings, api, store)
    processor.process(job)
    assert not store.schedule(USER)
    make_due(store)
    processor.process(job)
    assert store.get_user(USER)[0]["status"] == "blocked"
    assert not store.schedule(USER)


def test_preferred_name_only_filled_from_mapping(settings, api, store):
    settings = replace(settings, preferred_name_policy="fill_if_empty")
    store = memory_store(settings)
    Provisioner(settings, api, store).process(queued(store))
    assert api.person["preferredName"] == "Alex"


def test_existing_preferred_name_is_preserved(settings, api):
    settings = replace(settings, preferred_name_policy="fill_if_empty")
    api.person["preferredName"] = "My chosen name"
    store = memory_store(settings)
    Provisioner(settings, api, store).process(queued(store))
    assert api.person["preferredName"] == "My chosen name"
    assert "preferred_name" not in api.writes


def test_incomplete_scan_does_not_publish_members_or_enqueue(settings, api, store):
    before, _ = store.read(store.control, "membership")

    def fail_members():
        raise ProvisioningError("api_http_503", True)
    api.members = fail_members
    with pytest.raises(ProvisioningError):
        Service(settings, api, store).scan()
    after, _ = store.read(store.control, "membership")
    assert after["scan_id"] == before["scan_id"]
    assert store.queue.messages == []
