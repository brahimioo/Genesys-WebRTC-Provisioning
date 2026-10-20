#!/usr/bin/env python3
"""Authenticated operator CLI, using the Azure CLI account and custom API scope."""
import argparse
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from bootstrap import az


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs", required=True)
    parser.add_argument("operation", choices=["health", "scan", "status", "audit", "provision", "pause", "resume"])
    parser.add_argument("--user-id")
    parser.add_argument("--reset-create-intent", action="store_true")
    args = parser.parse_args()
    with open(args.outputs, encoding="utf-8") as handle:
        outputs = json.load(handle)
    az("account", "set", "--subscription", outputs["subscriptionId"])
    token = az("account", "get-access-token",
               "--resource", "api://" + outputs["entraApiClientId"])["accessToken"]
    operation = args.operation
    method, path, body = "GET", operation, None
    if operation == "scan":
        method = "POST"
    if operation in {"pause", "resume"}:
        method, path, body = "PATCH", "control", {"paused": operation == "pause"}
    if operation in {"provision", "audit"} and not args.user_id:
        parser.error("--user-id is required")
    if operation == "provision":
        method, body = "POST", {"userId": args.user_id, "resetCreateIntent": args.reset_create_intent}
    if operation in {"status", "audit"} and args.user_id:
        path += "/" + args.user_id
    request = Request(
        outputs["apiBaseUrl"] + "/" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=30) as result:
            print(json.dumps(json.load(result), indent=2))
    except HTTPError as error:
        print("HTTP", error.code, error.read().decode())
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
