"""Native Trading API session calendar for qualified US cash equities."""
from datetime import timedelta
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk.errors import BrokerCapabilityError, BrokerContractError
from .raw_stream import decode_native
from .options_calendar import wire_time


async def request_calendar(adapter, identity, *, symbol, session, session_profile=None, timestamp):
    if session != 'RTH':
        raise BrokerCapabilityError('Native calendar does not provide extended-hour session boundaries',code='session_calendar_not_supported')
    if session_profile not in (None,'','BROKER_LIQUID_HOURS'):
        raise BrokerContractError('BROKER_SESSION_PROFILE_MISMATCH')
    # A different provider's numeric identity cannot silently identify an
    # Alpaca asset merely because its display symbol happens to match.
    if any(identity.get(key) is not None for key in ('conId','con_id','contractId','contract_id')):
        raise BrokerContractError('BROKER_SESSION_CONTRACT_INVALID')
    qualified = await adapter.qualify_contract(identity)
    if qualified['symbol'] != symbol.upper() or identity.get('assetId') and identity['assetId'] != qualified.get('assetId'):
        raise BrokerContractError('BROKER_SESSION_CONTRACT_AMBIGUOUS')
    day = timestamp.astimezone(ZoneInfo('America/New_York')).date()
    first,last = day-timedelta(days=30),day+timedelta(days=14)
    raw = await adapter._backend.get_option_resource('/v2/calendar',params=dict(
        start=first.isoformat(),end=last.isoformat(),date_type='TRADING'))
    data = decode_native(raw)
    if not isinstance(data,list) or not data:
        raise BrokerContractError('BROKER_SESSION_CALENDAR_EMPTY')
    rows = []
    for native in data:
        if not isinstance(native,dict) or not first.isoformat() <= str(native.get('date') or '') <= last.isoformat():
            raise BrokerContractError('BROKER_SESSION_RESPONSE_SCOPE_MISMATCH')
        try:
            opened,closed = (wire_time(native['date'],native[key]) for key in ('open','close'))
        except (KeyError,ValueError) as exc:
            raise BrokerContractError('BROKER_SESSION_BOUNDARY_INVALID') from exc
        if opened >= closed:
            raise BrokerContractError('BROKER_SESSION_BOUNDARY_INVALID')
        rows.append(dict(symbol=symbol.upper(),date=native['date'],sessionOpen=opened,sessionClose=closed,
            startTime=opened,session='RTH',timezone='America/New_York',calendarSource='native_calendar',sessionProfile='BROKER_LIQUID_HOURS'))
    rows.sort(key=lambda row:row['sessionOpen'])
    if any(left['sessionClose'] > right['sessionOpen'] for left,right in zip(rows,rows[1:])):
        raise BrokerContractError('BROKER_SESSION_OVERLAP')
    return rows
