#!/usr/bin/env python3
"""Build local wheels and emit checksums plus a minimal SPDX 2.3 SBOM."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
IB_PACKAGE = ROOT / "packages" / "ibkr-paper"


def _version(distribution: str, default: str) -> str:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return default


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sdk-path", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("SOURCE_DATE_EPOCH", "1784044800")
    for package in (args.sdk_path.resolve(), IB_PACKAGE):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--no-deps",
                "--no-build-isolation",
                "--wheel-dir",
                str(output),
                str(package),
            ],
            check=True,
            env=env,
        )

    wheels = sorted(output.glob("*.whl"))
    sums = []
    for wheel in wheels:
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        sums.append(f"{digest}  {wheel.name}")
    (output / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8")

    namespace = f"https://github.com/winglight/algo-trader-broker-adapters/spdx/{uuid4()}"
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "algo-trader-broker-adapter-ibkr-paper-0.1.0",
        "documentNamespace": namespace,
        "creationInfo": {
            "created": "2026-07-15T00:00:00Z",
            "creators": ["Tool: build_verified_artifacts.py"],
        },
        "documentDescribes": ["SPDXRef-IBKRAdapter"],
        "packages": [
            {
                "name": "algo-trader-broker-adapter-ibkr-paper",
                "SPDXID": "SPDXRef-IBKRAdapter",
                "versionInfo": "0.1.0",
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
            },
            {
                "name": "algo-trader-broker-sdk",
                "SPDXID": "SPDXRef-BrokerSDK",
                "versionInfo": "1.0.0",
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
            },
            {
                "name": "ib-async",
                "SPDXID": "SPDXRef-IBAsync",
                "versionInfo": _version("ib_async", "2.x (resolved at install time)"),
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION",
            },
        ],
        "relationships": [
            {
                "spdxElementId": "SPDXRef-IBKRAdapter",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": "SPDXRef-BrokerSDK",
            },
            {
                "spdxElementId": "SPDXRef-IBKRAdapter",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": "SPDXRef-IBAsync",
            },
        ],
    }
    (output / "SBOM.spdx.json").write_text(
        json.dumps(sbom, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
