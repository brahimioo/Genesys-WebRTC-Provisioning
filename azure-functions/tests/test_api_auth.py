import base64
import json
from types import SimpleNamespace

import pytest
import requests

from conftest import ADMIN, TENANT
from webrtc.auth import require_admin
from webrtc.errors import ProvisioningError
from webrtc.genesys import Genesys


def headers(tenant=TENANT, actor=ADMIN, roles=()):
    principal = {"auth_typ": "aad", "claims": [
        {"typ": "tid", "val": tenant}, {"typ": "oid", "val": actor},
        *[{"typ": "roles", "val": role} for role in roles],
    ]}
    return {"x-ms-client-principal": base64.b64encode(json.dumps(principal).encode()).decode()}


def test_admin_principal(settings):
    assert require_admin(headers(), settings) == ADMIN


@pytest.mark.parametrize("value", [{}, {"x-ms-client-principal": "not-base64"}])
def test_missing_or_invalid_principal_rejected(settings, value):
    with pytest.raises(ProvisioningError) as error:
        require_admin(value, settings)
    assert error.value.status == 401


def test_wrong_tenant_rejected(settings):
    with pytest.raises(ProvisioningError) as error:
        require_admin(headers(tenant="another-tenant"), settings)
    assert error.value.status == 403


def test_unlisted_user_rejected(settings):
    with pytest.raises(ProvisioningError):
        require_admin(headers(actor="other-user"), settings)


def test_assigned_admin_role_accepted(settings):
    assert require_admin(headers(actor="app-object", roles=["Provisioning.Admin"]), settings) == "app-object"


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.oauth_calls = 0

    def post(self, *args, **kwargs):
        self.oauth_calls += 1
        return result(200, {"access_token": "token", "expires_in": 3600})

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class Rate:
    def __init__(self):
        self.blocked = None

    def acquire(self):
        pass

    def block(self, delay):
        self.blocked = delay


def result(status, data, headers=None):
    return SimpleNamespace(status_code=status, json=lambda: data,
                           content=b"response", headers=headers or {})


def client(settings, responses):
    session, rate = FakeSession(responses), Rate()
    return Genesys(settings, rate, session_factory=lambda: session), session, rate


def test_pagination_error_cannot_return_partial_inventory(settings):
    api, session, _ = client(settings, [
        result(200, {"entities": [{"id": "first"}], "pageCount": 2}),
        result(403, {}),
    ])
    with pytest.raises(ProvisioningError):
        api.all("/users")
    assert len(session.calls) == 2


def test_401_refreshes_token_once(settings):
    api, session, _ = client(settings, [result(401, {}), result(200, {"id": "ok"})])
    assert api.request("GET", "/users/example")["id"] == "ok"
    assert session.oauth_calls == 2


def test_429_sets_shared_cooldown(settings):
    api, session, rate = client(settings, [result(429, {}, {"Retry-After": "42"})])
    with pytest.raises(ProvisioningError) as error:
        api.request("GET", "/users")
    assert error.value.retry_after == 42
    assert rate.blocked == 42
    assert len(session.calls) == 1


def test_non_idempotent_post_not_retried_after_timeout(settings):
    api, session, _ = client(settings, [requests.Timeout()])
    with pytest.raises(ProvisioningError, match="api_network_outcome_unknown"):
        api.request("POST", "/telephony/providers/edges/phones", {})
    assert len(session.calls) == 1
