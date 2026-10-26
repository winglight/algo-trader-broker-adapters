from types import SimpleNamespace as N
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.options_orders import rejected_state
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from algo_trader_broker_sdk import BrokerOrderError, SubmissionGate, dataclass_to_payload
from algo_trader_broker_sdk.options import OptionOrderState, option_from_payload


@pytest.mark.asyncio
@pytest.mark.parametrize('status,code', [(422, 'broker_order_error'), (503, 'broker_order_outcome_unknown')])
async def test_native_rejection_reason_survives_without_reclassifying_unknown_outcomes(status, code):
    reason = 'account is not approved for options trading'
    seen = []
    def respond(request):
        seen.append(request)
        return httpx.Response(status, json={'message': reason})
    clients = AlpacaClients(AlpacaPaperSettings('synthetic-key', 'synthetic-secret'))
    clients._option_http_transport = httpx.MockTransport(respond)
    gate = SubmissionGate(AsyncMock(), Mock())
    with pytest.raises(BrokerOrderError) as caught:
        await clients.submit_option_order_raw({'symbol':'SPY261009C00600000'}, submission_gate=gate)
    assert gate.consumed and len(seen) == 1
    assert caught.value.code == code
    if status == 422:
        assert str(caught.value) == reason
        request = N(command_id='submit', client_order_id='submit', groups=1,
            legs=(N(leg_id='call', binding=N(canonical_id='opt_'+'a'*64), ratio=1),))
        payload = dataclass_to_payload(rejected_state(request, str(caught.value)))
        decoded = option_from_payload(OptionOrderState, payload)
        assert decoded.status == 'REJECTED' and decoded.rejection_reason == reason
        legacy = dataclass_to_payload(rejected_state(request))
        assert 'rejection_reason' not in legacy
        assert option_from_payload(OptionOrderState, legacy).rejection_reason is None
    else:
        assert reason not in str(caught.value)
