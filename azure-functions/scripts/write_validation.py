#!/usr/bin/env python3
import hashlib
import os
from pathlib import Path
from datetime import datetime, timezone
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def main():
    out = ROOT / "dist"
    tests = ET.parse(out / "test-results.xml").getroot()
    suites = tests.findall("testsuite") if tests.tag == "testsuites" else [tests]
    count = sum(int(s.get("tests", 0)) for s in suites)
    failures = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
    report = (
        "# Build validation\n\n"
        "UTC: " + datetime.now(timezone.utc).isoformat() + "\n\n"
        "Commit: " + os.getenv("GITHUB_SHA", "local") + "\n\n"
        "Automated tests: " + str(count) + "; failures/errors: " + str(failures) + ".\n\n"
        "Also checked: Python lint, JSON/workflow syntax, Function indexing, Bicep compilation.\n\n"
        "Not performed by this build: deployment to a live Azure subscription, Entra token issuance, "
        "Genesys tenant mutations, or an end-to-end test call. Complete the pilot checklist before live use.\n"
    )
    (out / "VALIDATION.md").write_text(report, encoding="utf-8")
    lines = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.name != "SHA256SUMS.txt":
            lines.append(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name)
    (out / "SHA256SUMS.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
