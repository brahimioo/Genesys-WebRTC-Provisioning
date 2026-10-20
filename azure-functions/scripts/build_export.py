#!/usr/bin/env python3
"""Build source exports without local settings or credentials."""
import argparse
import json
from pathlib import Path
import zipfile

from bootstrap import PARAMETERS

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache",
            ".python_packages", "dist", "build"}


def included(path):
    return not any(p in EXCLUDED for p in path.relative_to(ROOT).parts) and (
        not path.name.endswith((".local.json", ".pyc", ".zip"))
        and path.name != "local.settings.json"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT / "config.example.json").read_text())
    params = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {k: {"value": v} for k, v in config.items() if k in PARAMETERS},
    }
    (args.out / "azuredeploy.parameters.example.json").write_text(
        json.dumps(params, indent=2) + "\n", encoding="utf-8"
    )
    with zipfile.ZipFile(args.out / "genesys-webrtc-azure-export.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(ROOT.rglob("*")):
            if path.is_file() and included(path):
                archive.write(path, "azure-functions/" + str(path.relative_to(ROOT)))
        for name in ("azuredeploy.json", "azuredeploy.parameters.example.json"):
            path = args.out / name
            if path.is_file():
                archive.write(path, "azure-functions/infra/" + name)
        archive.write(ROOT / "START_HIER.md", "START_HIER.md")
        workflows = ROOT.parent / ".github" / "workflows"
        if not workflows.exists():
            workflows = ROOT / ".github" / "workflows"
        for path in sorted(workflows.glob("azure-webrtc-*.yml")):
            archive.write(path, ".github/workflows/" + path.name)
    with zipfile.ZipFile(args.out / "functionapp-source.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ("function_app.py", "host.json", "requirements.txt"):
            archive.write(ROOT / name, name)
        for path in sorted((ROOT / "webrtc").glob("*.py")):
            archive.write(path, str(path.relative_to(ROOT)))
    print("Exports created in", args.out)


if __name__ == "__main__":
    main()
