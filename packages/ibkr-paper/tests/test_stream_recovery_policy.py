import pytest

from algo_trader_broker_adapter_ibkr_paper.exceptions import IBMarketDataError
from algo_trader_broker_sdk.errors import is_transport_failure


@pytest.mark.parametrize('code,transport',[(317,True),(366,True),(10182,True),(10197,False),(354,False),(10089,False)])
def test_native_stream_recovery_is_projected_only_in_adapter(code,transport):
    error = IBMarketDataError(f'Error {code}: native message')
    assert is_transport_failure(error) is transport
    assert error.details['nativeCode'] == code
