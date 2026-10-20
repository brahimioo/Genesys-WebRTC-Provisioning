#!/usr/bin/env python3
import argparse
import json
import re

from bootstrap import az


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs", required=True)
    parser.add_argument("--repository", required=True, help="owner/repository")
    parser.add_argument("--environment", required=True, choices=["webrtc-test", "webrtc-prod"])
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository):
        raise ValueError("Invalid repository")
    with open(args.outputs, encoding="utf-8") as handle:
        outputs = json.load(handle)
    az("account", "set", "--subscription", outputs["subscriptionId"])
    identity = az("identity", "create", "--resource-group", outputs["resourceGroupName"],
                  "--name", "id-webrtc-github-" + args.environment)
    az("identity", "federated-credential", "create",
       "--name", "github-environment", "--identity-name", identity["name"],
       "--resource-group", outputs["resourceGroupName"],
       "--issuer", "https://token.actions.githubusercontent.com",
       "--subject", "repo:" + args.repository + ":environment:" + args.environment,
       "--audiences", "api://AzureADTokenExchange")
    scope = "/subscriptions/" + outputs["subscriptionId"] + "/resourceGroups/" + outputs["resourceGroupName"]
    az("role", "assignment", "create",
       "--assignee-object-id", identity["principalId"], "--assignee-principal-type", "ServicePrincipal",
       "--role", "Website Contributor", "--scope", scope)
    print("Create GitHub environment:", args.environment)
    print("Add these environment VARIABLES (no Genesys secrets):")
    print(json.dumps({
        "AZURE_CLIENT_ID": identity["clientId"],
        "AZURE_TENANT_ID": outputs["tenantId"],
        "AZURE_SUBSCRIPTION_ID": outputs["subscriptionId"],
        "AZURE_FUNCTIONAPP_NAME": outputs["functionAppName"],
    }, indent=2))


if __name__ == "__main__":
    main()
