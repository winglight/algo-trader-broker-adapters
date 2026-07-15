from __future__ import annotations

from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_sdk import assert_manifest_compatible


def test_manifest_is_sdk_1_compatible() -> None:
    adapter = IBKRPaperAdapter({}, client=object(), supervisor=object())

    manifest = adapter.manifest()
    assert_manifest_compatible(manifest)
    assert manifest.adapter_id == "ibkr_paper"
    assert manifest.entrypoint == "algo_trader_broker_adapter_ibkr_paper:create_adapter"
    assert manifest.environment == "PAPER"


def test_package_source_has_no_main_repository_imports() -> None:
    from pathlib import Path

    source_root = Path(__file__).parents[1] / "src"
    offenders = []
    for path in source_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "from src" in text or "import src" in text:
            offenders.append(str(path.relative_to(source_root)))

    assert offenders == []
