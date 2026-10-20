#!/usr/bin/env python3
"""Deploy a Python source package using Azure's Flex remote build."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs", type=Path, required=True)
    args = parser.parse_args()
    outputs = json.loads(args.outputs.read_text())
    subprocess.run(["az", "account", "set", "--subscription", outputs["subscriptionId"]], check=True)
    subprocess.run([sys.executable, str(ROOT / "scripts" / "build_export.py")], check=True)
    package = ROOT / "dist" / "functionapp-source.zip"
    command = [
        "az", "functionapp", "deployment", "source", "config-zip",
        "--resource-group", outputs["resourceGroupName"],
        "--name", outputs["functionAppName"], "--src", str(package),
        "--build-remote", "true",
    ]
    # IAM and Key Vault references may need time after the initial deployment.
    for attempt in range(3):
        result = subprocess.run(command)
        if result.returncode == 0:
            break
        if attempt == 2:
            raise SystemExit("Deployment failed. Inspect Deployment Center and Azure IAM propagation.")
        print("Retrying initial deployment after a short propagation interval.")
        time.sleep(20)
    print("Deployed. API base:", outputs["apiBaseUrl"])
    print("Allow the first timer scan to complete, then inspect the Workbook and status API.")


if __name__ == "__main__":
    main()
