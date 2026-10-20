#!/usr/bin/env python3
"""Read-only discovery from an existing Genesys WebRTC phone."""
import argparse
import base64
import getpass
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--template-phone", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--skill", default="_Voice")
    parser.add_argument("--language", default="Nederlands")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    region = config.get("genesysEnvironment", "mypurecloud.de")
    if not re.fullmatch(r"(mypurecloud\.(com|de|ie|com\.au|jp)|[a-z0-9-]+\.pure\.cloud)", region):
        raise ValueError("Invalid region")
    secret = os.getenv("GENESYS_CLIENT_SECRET") or getpass.getpass("Genesys client secret (hidden): ")
    auth = base64.b64encode((config["genesysClientId"] + ":" + secret).encode()).decode()
    request = Request("https://login." + region + "/oauth/token",
                      data=urlencode({"grant_type": "client_credentials"}).encode(),
                      headers={"Authorization": "Basic " + auth})
    try:
        with urlopen(request, timeout=30) as response:
            token = json.load(response)["access_token"]

        def get(path, params=None):
            url = "https://api." + region + "/api/v2" + path
            if params:
                url += "?" + urlencode(params)
            with urlopen(Request(url, headers={"Authorization": "Bearer " + token}), timeout=30) as response:
                return json.load(response)

        def listing(path, params=None):
            values = []
            for page in range(1, 1001):
                data = get(path, dict(params or {}, pageSize=100, pageNumber=page))
                items = data["entities"]
                values.extend(items)
                if page >= data.get("pageCount", page + 1) or (
                    "pageCount" not in data and len(items) < 100
                ):
                    return values
            raise ValueError("Inventory limit exceeded")

        def exact(values, name):
            matches = [v for v in values if v.get("name", "").casefold() == name.casefold()]
            if len(matches) != 1:
                raise ValueError("Expected one exact match: " + name)
            return matches[0]

        phone = exact(listing("/telephony/providers/edges/phones", {
            "name": args.template_phone, "sortBy": "id",
        }), args.template_phone)
        details = get("/telephony/providers/edges/phones/" + phone["id"])
        ids = {
            "siteId": details["site"]["id"],
            "phoneBaseSettingsId": details["phoneBaseSettings"]["id"],
            "lineBaseSettingsId": details["lines"][0]["lineBaseSettings"]["id"],
            "sourceGroupId": exact(listing("/groups"), args.group)["id"],
            "routingSkillId": exact(listing("/routing/skills"), args.skill)["id"],
            "routingLanguageId": exact(listing("/routing/languages"), args.language)["id"],
        }
    except HTTPError as error:
        raise SystemExit("Genesys discovery failed: HTTP " + str(error.code)) from None
    config.update(ids)
    args.config.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(ids, indent=2))


if __name__ == "__main__":
    main()
