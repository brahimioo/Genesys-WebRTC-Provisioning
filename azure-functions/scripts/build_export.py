#!/usr/bin/env python3
"""Create a standalone source export and a remote-build Function App ZIP."""
import argparse
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {
    ".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache",
    ".python_packages", "dist", "build",
}


def included(path):
    return not any(part in EXCLUDED for part in path.relative_to(ROOT).parts) and (
        not path.name.endswith((".local.json", ".pyc", ".zip"))
        and path.name != "local.settings.json"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.out / "genesys-webrtc-azure-export.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(ROOT.rglob("*")):
            if path.is_file() and included(path):
                archive.write(path, str(path.relative_to(ROOT)))
        for path in sorted((ROOT.parent / ".github" / "workflows").glob("azure-webrtc-*.yml")):
            archive.write(path, ".github/workflows/" + path.name)
    with zipfile.ZipFile(args.out / "functionapp-source.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ("function_app.py", "host.json", "requirements.txt"):
            archive.write(ROOT / name, name)
        for path in sorted((ROOT / "webrtc").glob("*.py")):
            archive.write(path, str(path.relative_to(ROOT)))
    config = json.loads((ROOT / "config.example.json").read_text())
    from bootstrap import PARAMETERS
    parameters = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {key: {"value": value} for key, value in config.items() if key in PARAMETERS},
    }
    # Required secure parameter is deliberately omitted; Azure asks for it at deployment.
    (args.out / "azuredeploy.parameters.example.json").write_text(
        json.dumps(parameters, indent=2) + "\n", encoding="utf-8"
    )
    print("Export files created in", args.out)


if __name__ == "__main__":
    main()
