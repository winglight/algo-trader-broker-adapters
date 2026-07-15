from __future__ import annotations

from pathlib import Path

from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_sdk import assert_manifest_compatible


class ContractBackend:
    async def get_account(self):
        return {"id": "paper-account", "equity": "100000", "last_equity": "99000"}

    async def get_positions(self):
        return []

    async def start_trade_updates(self, handler, failure_handler):
        self.handler = handler
        self.failure_handler = failure_handler


def test_manifest_is_sdk_1_compatible() -> None:
    adapter = AlpacaPaperAdapter(
        {"alpaca_api_key_id": "test-key", "alpaca_secret_key": "test-secret"},
        backend=ContractBackend(),
    )
    manifest = adapter.manifest()

    assert_manifest_compatible(manifest)
    assert manifest.adapter_id == "alpaca_paper"
    assert manifest.environment == "PAPER"
    assert manifest.capabilities.asset_classes == {"STK", "ETF"}
    assert manifest.capabilities.supports_futures is False
    assert manifest.capabilities.supports_options is False


def test_package_source_has_no_main_repository_or_ib_imports() -> None:
    source_root = Path(__file__).parents[1] / "src"
    offenders: list[str] = []
    for path in source_root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if any(
            marker in source
            for marker in (
                "from src",
                "import src",
                "import ib_async",
                "from ib_async",
            )
        ):
            offenders.append(str(path.relative_to(source_root)))

    assert offenders == []


def test_package_metadata_pins_official_sdk_and_does_not_depend_on_core() -> None:
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")

    assert '"alpaca-py==0.43.5"' in pyproject
    assert '"algo-trader-broker-sdk>=1,<2"' in pyproject
    assert "ib_async" not in pyproject
    assert '"algo-trader"' not in pyproject
