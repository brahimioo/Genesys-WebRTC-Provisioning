import time
from dataclasses import replace

import pytest

from conftest import USER, queued
from webrtc.errors import ProvisioningError


def test_duplicate_delivery_cannot_claim_same_user_twice(store):
    job = queued(store)
    assert store.claim(job) is not None
    assert store.claim(job) is None


def test_stale_generation_is_ignored(store):
    old_job = queued(store)
    store.schedule(USER, force=True)
    assert store.claim(old_job) is None


def test_queue_failure_leaves_recoverable_outbox(store):
    store.queue.fail = True
    with pytest.raises(RuntimeError):
        store.schedule(USER)
    assert store.get_user(USER)[0]["status"] == "pending"
    store.queue.fail = False
    assert store.recover() == 1
    assert store.queue.messages[-1][0]["userId"] == USER


def test_expired_worker_lease_can_be_recovered(store):
    job = queued(store)
    store.claim(job)
    doc, etag = store.get_user(USER)
    doc.update(lease_until=time.time() - 1, enqueued_at=0)
    store.save_user(doc, etag)
    assert store.recover() == 1
    assert store.claim(job) is not None


def test_etag_rejects_stale_updates(store):
    queued(store)
    first, etag = store.get_user(USER)
    second, same_etag = store.get_user(USER)
    first["last_error"] = "newer"
    store.save_user(first, etag)
    second["last_error"] = "stale"
    with pytest.raises(ProvisioningError, match="concurrent_state_change"):
        store.save_user(second, same_etag)


def test_changed_policy_reschedules_dry_run_for_live(settings, store):
    assert settings.fingerprint != replace(settings, dry_run=True).fingerprint
    queued(store)
    doc, etag = store.get_user(USER)
    doc.update(status="dry_run", verified_at=time.time(), fingerprint="old-dry-run-policy")
    store.save_user(doc, etag)
    assert store.schedule(USER)


def test_membership_snapshot_must_be_fresh(store):
    row, etag = store.read(store.control, "membership")
    row["completed_at"] = time.time() - 7200
    store.write(store.control, row, etag)
    with pytest.raises(ProvisioningError, match="membership_snapshot_stale"):
        store.eligible(USER)
