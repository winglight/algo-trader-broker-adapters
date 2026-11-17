"""Nonfinancial native API identity notices, decoded before qualification."""
import json
import re
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_events import OptionNativeOrderIdentity
from .settings import native_account_is_exact


def native_json(value):
    # Keep native sentinel fields outside interoperable financial values.
    def pairs(items):
        result={}
        for key,child in items:
            check(key not in result, 'Duplicate native JSON field')
            result[key]=child
        return result
    return json.loads(value,object_pairs_hook=pairs)


def pending_identity(event):
    try:
        return _pending_identity(event)
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        raise BrokerContractError('IB pending identity needs intact native evidence') from exc


def _pending_identity(event):
    if event.source not in {'IB_OPTION_CALLBACK','IB_OPTION_SUBMISSION'}:
        return None
    value=native_json(event.raw_payload)
    if event.source=='IB_OPTION_CALLBACK':
        if value.get('kind') not in {'openOrder','orderStatus'} or value.get('state',{}).get('status') not in {'ApiPending','PendingSubmit'}:
            return None
        native=value['order']
    else:
        observations=value.get('observations')
        if observations != [] and 'preparation' not in value:
            return None
        value=native_json(value['preparation'])
        check(value.get('source')=='IB_OPTION_PREPARATION_V1','Unknown native preparation notice')
        if observations != []:
            # Native warnings or PendingSubmit can end the initial wait before
            # an acceptance. Preserve this original send's API identity, just
            # as for an empty outcome; it supplies no acknowledgement or fills.
            # Exact request/vector validation must still reject contradictions.
            from types import SimpleNamespace
            from algo_trader_broker_sdk.options import OptionExecutionRequest,OptionScope,option_from_payload
            from algo_trader_broker_sdk.submission_gate import order_fingerprint
            from .options_orders import acknowledgement
            request=option_from_payload(OptionExecutionRequest,value['request'])
            scope=OptionScope(**{name:getattr(request,name) for name in OptionScope.__dataclass_fields__})
            check(scope==event.scope and value['request_fingerprint']==order_fingerprint(request),
                  'IB pending submission changed its observed scope or command')
            check(type(observations) is list and observations,'Native submission observations are invalid')
            outcome=acknowledgement(request,SimpleNamespace(**value['order']),observations)
            if outcome.parent_order_ref is not None and outcome.status!='SUBMISSION_UNKNOWN':
                return None
        native=value['order']
        if observations and outcome.parent_order_ref is not None:
            native={**native,'permId':int(outcome.parent_order_ref)}
    account=native.get('account')
    check(native_account_is_exact(account) and account==value.get('native_account_ref'),
          'Pending identity differs from its actual account')
    client,order,reference=native.get('clientId'),native.get('orderId'),native.get('orderRef')
    check(type(client) is int and client>=0 and type(order) is int and order>0 and
          type(reference) is str and re.fullmatch(r'atiopt_[0-9a-f]{40}',reference) is not None,
          'Pending identity needs its retained API client and order reference')
    permanent=native.get('permId',0)
    check(type(permanent) is int and permanent>=0,'Invalid native permanent identity')
    return OptionNativeOrderIdentity(event.scope,'IBKR',str(permanent) if permanent else f'api:{client}:{order}',
        'IBKR_PERM_ID' if permanent else f'IBKR_API_GENERATION_{event.scope.expected_generation}',reference)
