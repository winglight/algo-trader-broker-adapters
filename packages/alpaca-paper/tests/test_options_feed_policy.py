"""Native feed declaration and permission are identical across user-selected accounts."""
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_adapter_alpaca_paper.options_stream import NativeOptionQuote, normalize_quote
from algo_trader_broker_sdk.options import BrokerOptionBinding, SnapshotRequest
from .test_option_raw_events import VERIFIED


@pytest.mark.asyncio
@pytest.mark.parametrize("environment", ["paper", "live"])
async def test_default_feed_and_rest_stream_reference_permission_follow_adapter(environment):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    bound = replace(VERIFIED, scope=replace(VERIFIED.scope, environment=environment))
    binding = BrokerOptionBinding("alpaca_paper", "fixture", environment, bound.scope.account,
        "opt_"+"a"*64, "native-asset", "QQQ261016C00746000", None, now, "native-proof")
    raw = json.dumps(dict(snapshots={binding.local_symbol:dict(latestQuote=dict(t=now,bp="1",ap="1.1",bs=3,**{"as":3}))})).encode()
    async def resource(path, **kwargs):
        return json.dumps(dict(id=bound.native_account_ref)).encode() if path == "/v2/account" else raw
    backend = SimpleNamespace(get_option_resource=AsyncMock(side_effect=resource))
    adapter = AlpacaPaperAdapter(dict(alpaca_api_key_id="fixture", alpaca_secret_key="fixture", alpaca_environment=environment), backend=backend)
    adapter._connected, adapter._account_id = True, bound.native_account_ref
    adapter.bind_option_account(bound)
    adapter._option_catalog = {binding.broker_contract_id:(binding,None,None)}
    caps = await adapter.option_capabilities(bound.scope)
    assert caps.default_feed == "INDICATIVE"
    assert caps.default_history_feed == "provider_native" and caps.history_feeds == ("provider_native",)
    permissions = await adapter.option_account_permissions(bound.scope)
    assert permissions.approval_authority == permissions.trading_authority == "ACCOUNT_OBSERVATION"
    assert permissions.reason_codes == () and "OPRA_ENTITLEMENT_UNOBSERVED" in permissions.advisory_reason_codes
    request = SnapshotRequest(**asdict(bound.scope), bindings=(binding,), feed=caps.default_feed,
        purpose="ENTRY", max_age_ms=3000, max_leg_skew_ms=1000)
    snapshot = await adapter.option_snapshot(request)
    assert snapshot.quotes[0].quality == "RESEARCH_ONLY"
    assert snapshot.quotes[0].reference_pricing_allowed is True
    frame = NativeOptionQuote(b"fixture", dict(t=now,bp="1",ap="1.1",bs=3,**{"as":3}), now)
    stream = normalize_quote(frame, binding, request)
    assert stream.quality == "RESEARCH_ONLY" and stream.reference_pricing_allowed is True
