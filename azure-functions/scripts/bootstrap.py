#!/usr/bin/env python3
"""Run from Azure Cloud Shell Bash; no third-party Python packages required."""
import argparse
import getpass
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
AZURE_CLI_APP_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
REQUIRED_UUIDS = (
    "subscriptionId", "genesysClientId", "sourceGroupId", "siteId",
    "phoneBaseSettingsId", "lineBaseSettingsId",
)
PARAMETERS = {
    "projectName", "stage", "location", "entraApiClientId", "adminObjectIds",
    "genesysEnvironment", "genesysClientId", "sourceGroupId", "siteId",
    "phoneBaseSettingsId", "lineBaseSettingsId", "routingSkillId", "routingLanguageId",
    "skillProficiency", "languageProficiency", "pilotUserIds", "dryRun",
    "provisioningPaused", "defaultStationPolicy", "preferredNamePolicy", "givenNames",
    "maxJobsPerScan", "scanSchedule", "recoverySchedule", "reconcileSchedule",
    "maxAttempts", "reconcileHours", "enableAlerts", "alertEmail",
}


def az(*args):
    result = subprocess.run(["az", *args, "--only-show-errors", "-o", "json"],
                            capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("Azure CLI failed: " + result.stderr[-2500:])
    return json.loads(result.stdout) if result.stdout.strip() else {}


def body_call(method, url, body):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        os.chmod(handle.name, 0o600)
        json.dump(body, handle)
        path = handle.name
    try:
        return az("rest", "--method", method, "--url", url, "--body", "@" + path)
    finally:
        os.unlink(path)


def validate(config):
    if "genesysClientSecret" in config:
        raise ValueError("Keep the Genesys secret out of config files; use the secure prompt.")
    unknown = set(config) - PARAMETERS - {"subscriptionId", "resourceGroupName"}
    if unknown:
        raise ValueError("Unknown configuration keys: " + ", ".join(sorted(unknown)))
    for key in REQUIRED_UUIDS:
        value = str(uuid.UUID(config[key]))
        if value == str(uuid.UUID(int=0)):
            raise ValueError("Replace the placeholder for " + key)
    for key in ("entraApiClientId", "routingSkillId", "routingLanguageId"):
        if config.get(key):
            uuid.UUID(config[key])
    for key in ("adminObjectIds", "pilotUserIds"):
        for value in config.get(key, []):
            uuid.UUID(value)
    for key in ("dryRun", "provisioningPaused", "enableAlerts"):
        if key in config and not isinstance(config[key], bool):
            raise ValueError(key + " must be a JSON boolean")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,19}", config["projectName"]):
        raise ValueError("projectName: 3-20 lowercase letters, numbers or hyphens")
    if not re.fullmatch(r"[A-Za-z0-9_.()-]{1,90}", config["resourceGroupName"]):
        raise ValueError("Invalid resourceGroupName")
    if config.get("stage") not in {"test", "prod"}:
        raise ValueError("stage must be test or prod")
    if config.get("preferredNamePolicy", "disabled") == "fill_if_empty" and not config.get("givenNames"):
        raise ValueError("fill_if_empty requires an authoritative givenNames mapping")


def create_api(config, config_path):
    scope_id = str(uuid.uuid4())
    role_id = str(uuid.uuid4())
    application = body_call("POST", "https://graph.microsoft.com/v1.0/applications", {
        "displayName": config["projectName"] + "-" + config["stage"] + "-api",
        "signInAudience": "AzureADMyOrg",
        "api": {
            "requestedAccessTokenVersion": 2,
            "oauth2PermissionScopes": [{
                "id": scope_id, "value": "user_impersonation", "type": "User",
                "isEnabled": True, "adminConsentDisplayName": "Use the WebRTC provisioning API",
                "adminConsentDescription": "Use the API; the app enforces its own administrator list.",
                "userConsentDisplayName": "Use the WebRTC provisioning API",
                "userConsentDescription": "Use the API subject to administrator authorization.",
            }],
            "preAuthorizedApplications": [{
                "appId": AZURE_CLI_APP_ID, "delegatedPermissionIds": [scope_id],
            }],
        },
        "appRoles": [{
            "id": role_id, "value": "Provisioning.Admin", "isEnabled": True,
            "displayName": "Provisioning administrator",
            "description": "Administer WebRTC provisioning",
            "allowedMemberTypes": ["User", "Application"],
        }],
    })
    config["entraApiClientId"] = application["appId"]
    # Save identity immediately so a later failure does not cause another app on retry.
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    body_call("PATCH", "https://graph.microsoft.com/v1.0/applications/" + application["id"], {
        "identifierUris": ["api://" + application["appId"]],
    })
    body_call("POST", "https://graph.microsoft.com/v1.0/servicePrincipals", {
        "appId": application["appId"],
    })
    return application["appId"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--create-entra-app", action="store_true",
                        help="Create the single-tenant API registration if no client ID is configured")
    parser.add_argument("--what-if", action="store_true")
    parser.add_argument("--outputs", type=Path, default=Path("deployment.local.json"))
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    validate(config)
    az("account", "set", "--subscription", config["subscriptionId"])
    account = az("account", "show")
    if not config.get("adminObjectIds"):
        config["adminObjectIds"] = [az("ad", "signed-in-user", "show")["id"]]
        args.config.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    if not config.get("entraApiClientId"):
        if not args.create_entra_app or args.what_if:
            raise ValueError("Configure entraApiClientId, or use --create-entra-app for initial setup.")
        create_api(config, args.config)
    if args.what_if:
        az("group", "show", "--name", config["resourceGroupName"])
    else:
        for provider in (
            "Microsoft.Web", "Microsoft.App", "Microsoft.Insights",
            "Microsoft.OperationalInsights", "Microsoft.KeyVault", "Microsoft.Storage",
        ):
            az("provider", "register", "--namespace", provider, "--wait")
        az("group", "create", "--name", config["resourceGroupName"], "--location", config["location"])
    secret = ("what-if-placeholder-not-a-real-secret" if args.what_if else (
        os.getenv("GENESYS_CLIENT_SECRET") or getpass.getpass("Genesys client secret (hidden): ")
    ))
    if not secret.strip():
        raise ValueError("A Genesys client secret is required")
    params = {key: {"value": value} for key, value in config.items() if key in PARAMETERS}
    params["genesysClientSecret"] = {"value": secret}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        os.chmod(handle.name, 0o600)
        json.dump({"parameters": params}, handle)
        param_path = handle.name
    try:
        operation = "what-if" if args.what_if else "create"
        result = az("deployment", "group", operation,
                    "--resource-group", config["resourceGroupName"],
                    "--name", "webrtc-" + config["stage"],
                    "--template-file", str(ROOT / "infra" / "main.bicep"),
                    "--parameters", "@" + param_path)
    finally:
        os.unlink(param_path)
        secret = ""
    if args.what_if:
        print(json.dumps(result, indent=2))
        return
    outputs = {key: item["value"] for key, item in result["properties"]["outputs"].items()}
    outputs.update(
        resourceGroupName=config["resourceGroupName"],
        subscriptionId=account["id"], tenantId=account["tenantId"],
    )
    args.outputs.write_text(json.dumps(outputs, indent=2) + "\n", encoding="utf-8")
    print("Infrastructure ready. Deployment outputs:", args.outputs)
    print("Function App:", outputs["functionAppName"])
    print("Next: python3 scripts/deploy_code.py --outputs", args.outputs)


if __name__ == "__main__":
    main()
