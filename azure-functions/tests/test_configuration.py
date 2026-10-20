import pytest

from conftest import ADMIN, BASE, LINE, SITE, TENANT, USER
from webrtc.config import Settings


def environment(monkeypatch):
    values = {
        "GENESYS_ENVIRONMENT": "mypurecloud.de", "GENESYS_CLIENT_ID": USER,
        "GENESYS_CLIENT_SECRET": "test-secret", "SOURCE_GROUP_ID": USER,
        "SITE_ID": SITE, "PHONE_BASE_SETTINGS_ID": BASE,
        "LINE_BASE_SETTINGS_ID": LINE, "ENTRA_TENANT_ID": TENANT,
        "ADMIN_OBJECT_IDS": ADMIN, "STORAGE_ACCOUNT_NAME": "localtest",
        "DRY_RUN": "true",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_defaults_preserve_user_settings(monkeypatch):
    environment(monkeypatch)
    settings = Settings.from_env()
    assert settings.dry_run is True
    assert settings.default_policy == "if_missing"
    assert settings.preferred_name_policy == "disabled"


def test_unresolved_key_vault_secret_rejected(monkeypatch):
    environment(monkeypatch)
    monkeypatch.setenv("GENESYS_CLIENT_SECRET", "@Microsoft.KeyVault(SecretUri=missing)")
    with pytest.raises(ValueError, match="unresolved"):
        Settings.from_env()


def test_non_genesys_hostname_rejected(monkeypatch):
    environment(monkeypatch)
    monkeypatch.setenv("GENESYS_ENVIRONMENT", "example.com")
    with pytest.raises(ValueError, match="hostname"):
        Settings.from_env()
