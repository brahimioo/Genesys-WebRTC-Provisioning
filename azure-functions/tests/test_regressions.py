from conftest import USER, make_due, queued
from webrtc.errors import ProvisioningError
from webrtc.provisioning import Provisioner


def test_pre_post_cooldown_does_not_leave_uncertain_create_intent(settings, api, store):
    def cooldown(user):
        raise ProvisioningError("shared_api_cooldown", True, 30)
    api.create_phone = cooldown
    Provisioner(settings, api, store).process(queued(store))
    doc, _ = store.get_user(USER)
    assert doc["status"] == "retry"
    assert doc["create_in_flight"] is False


def test_reactivated_user_can_be_scheduled(store):
    queued(store)
    doc, etag = store.get_user(USER)
    doc.update(status="excluded", attempts=8)
    store.save_user(doc, etag)
    assert store.schedule(USER)
    assert store.get_user(USER)[0]["attempts"] == 0


def test_definite_throttle_rejection_can_be_retried(settings, api, store):
    original = api.create_phone
    calls = []

    def throttled_once(user):
        if not calls:
            calls.append(True)
            raise ProvisioningError("api_throttled", True, 30, 429)
        return original(user)

    api.create_phone = throttled_once
    job = queued(store)
    processor = Provisioner(settings, api, store)
    processor.process(job)
    assert store.get_user(USER)[0]["create_in_flight"] is False
    make_due(store)
    processor.process(job)
    assert store.get_user(USER)[0]["status"] == "succeeded"
