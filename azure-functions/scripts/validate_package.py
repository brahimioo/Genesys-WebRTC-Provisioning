#!/usr/bin/env python3
import ast
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    files = 0
    for path in ROOT.rglob("*.py"):
        if any(p in {".venv", ".python_packages", "dist", "__pycache__"} for p in path.parts):
            continue
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        files += 1
    for path in ROOT.rglob("*.json"):
        if any(p in {".venv", ".python_packages", "dist"} for p in path.parts):
            continue
        if path.name.endswith(".local.json"):
            continue
        json.loads(path.read_text(encoding="utf-8"))
    for path in (ROOT.parent / ".github" / "workflows").glob("azure-webrtc-*.yml"):
        yaml.safe_load(path.read_text(encoding="utf-8"))
    host = json.loads((ROOT / "host.json").read_text())
    assert host["extensions"]["queues"]["batchSize"] == 1
    assert host["extensions"]["queues"]["newBatchThreshold"] == 0
    config = json.loads((ROOT / "config.example.json").read_text())
    assert config["dryRun"] is True
    assert config["defaultStationPolicy"] == "if_missing"
    assert "genesysClientSecret" not in config
    import function_app
    assert function_app.app.get_functions()
    print("Package validation passed:", files, "Python files and all JSON/workflow files.")


if __name__ == "__main__":
    main()
