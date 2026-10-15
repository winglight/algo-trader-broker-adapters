"""Nonfinancial native API identity notices, decoded before qualification."""
import json
import re
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
    if event.source not in {'IB_OPTION_CALLBACK','IB_OPTION_SUBMISSION'}:
        return None
    value=native_json(event.raw_payload)
    if event.source=='IB_OPTION_CALLBACK':
        if value.get('kind') not in {'openOrder','orderStatus'} or value.get('state',{}).get('status') not in {'ApiPending','PendingSubmit'}:
            return None
        native=value['order']
    else:
        if value.get('observations')!=[]:
            return None
        value=native_json(value['preparation'])
        check(value.get('source')=='IB_OPTION_PREPARATION_V1','Unknown native preparation notice')
        native=value['order']
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
