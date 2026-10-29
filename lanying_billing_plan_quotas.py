"""Apply the paid plan's storage quotas without resetting usage or AI settings.

The authenticated Butler coordinator owns the paid order and App mutation guard.
Only these four quota fields are written. Main-Redis hosting period provisioning
is a separate recoverable step; its receipt alone never completes this operation.
"""
import hashlib
import json
import re
from decimal import Decimal

from redis.exceptions import WatchError
import lanying_config
import lanying_redis


class QuotaConflict(ValueError):
    pass


_FIELDS = ('lanying_connector.storage_limit', 'lanying_connector.storage_payg',
           'lanying_connector.grow_ai_website_storage_limit',
           'lanying_connector.grow_ai_website_traffic_limit')
_PREFIX = 'billing_plan_quota.'


def _terms(data):
    fields = {'app_id', 'order_id', 'order_sequence', 'environment',
              'knowledge_storage_mb', 'knowledge_payg', 'hosting_storage_mb',
              'hosting_traffic_gb', 'previous_knowledge_product_id'}
    if not isinstance(data, dict) or set(data) != fields:
        raise ValueError('invalid plan quota fields')
    for key, pattern in (('app_id', r'[a-z0-9]{1,64}'), ('order_id', r'[0-9a-f]{32}')):
        if not isinstance(data[key], str) or not re.fullmatch(pattern, data[key]):
            raise ValueError('invalid plan quota identity')
    for key, low, high in (('order_sequence', 1, 2**63 - 1),
                           ('knowledge_storage_mb', 0, 2**31 - 1), ('knowledge_payg', 0, 1),
                           ('hosting_storage_mb', 0, 2**31 - 1)):
        if type(data[key]) is not int or not low <= data[key] <= high:
            raise ValueError('invalid quota')
    previous = data['previous_knowledge_product_id']
    if previous is not None and (type(previous) is not int or not 9001 <= previous <= 9099):
        raise ValueError('invalid previous knowledge product')
    traffic = data['hosting_traffic_gb']
    if (not isinstance(traffic, str) or not re.fullmatch(r'[0-9]+(?:\.[0-9]{1,10})?', traffic)
            or Decimal(traffic) > 2**31 - 1 or data['environment'] not in ('TEST', 'LIVE')):
        raise ValueError('invalid plan quota terms')
    terms = dict(data)
    terms['hosting_traffic_gb'] = format(Decimal(traffic).normalize(), 'f')
    return terms


def apply(data, stack=None, main=None):
    terms = _terms(data)
    stack = stack if stack is not None else lanying_redis.get_redis_stack_connection()
    main = main if main is not None else lanying_redis.get_redis_connection()
    # Require the original hosting activation, not just a client-provided env flag.
    # Replay after a later order must never restore obsolete quotas.
    hosting = lanying_redis.redis_hgetall(main, 'lanying-connector:grow_ai:service_status:' + terms['app_id'])
    import lanying_billing_hosting
    if (hosting.get('billing_order_id') != terms['order_id']
            or hosting.get('billing_order_sequence') != str(terms['order_sequence'])
            or hosting.get('billing_environment') != terms['environment']
            or hosting.get('billing_writer') != 'ORDER' or hosting.get('status') != 'normal'
            or hosting.get('billing_state_hash') != lanying_billing_hosting._state_hash(hosting)
            or hosting.get('website_storage_limit') != str(terms['hosting_storage_mb'])
            or Decimal(hosting.get('website_traffic_limit', '-1')) != Decimal(terms['hosting_traffic_gb'])):
        raise QuotaConflict('original hosting activation is not current')
    values = dict(zip(_FIELDS, map(str, (terms['knowledge_storage_mb'], terms['knowledge_payg'],
                                        terms['hosting_storage_mb'], terms['hosting_traffic_gb']))))
    digest = hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    key = lanying_config.get_redis_app_config_key(terms['app_id'])
    try:
        with stack.pipeline() as pipe:
            pipe.watch(key)
            old = lanying_redis.redis_hgetall(pipe, key)
            if old.get(_PREFIX + 'order_id') == terms['order_id']:
                if old.get(_PREFIX + 'terms_hash') != digest or any(old.get(k) != v for k, v in values.items()):
                    raise QuotaConflict('quota terms or actual values changed')
                return dict(terms, status='applied')
            if int(old.get(_PREFIX + 'order_sequence', 0)) >= terms['order_sequence']:
                raise QuotaConflict('quota order has been superseded')
            if terms['environment'] == 'TEST':
                verified_test = (old.get(_PREFIX + 'environment') == 'TEST'
                                 and old.get(_PREFIX + 'state_hash') == _state_hash(old))
                # Previous product comes from the immutable, server-checked App
                # service snapshot, never the browser. Refuse standalone paid data.
                free_or_empty = (not old.get(_PREFIX + 'environment')
                                 and terms['previous_knowledge_product_id'] in (None, 9051)
                                 and old.get(_FIELDS[0], '0') in ('0', '5')
                                 and old.get(_FIELDS[1], '0') == '0')
                if not (verified_test or free_or_empty):
                    raise QuotaConflict('test order cannot replace live knowledge quotas')
            saved = dict(values, **{_PREFIX + 'order_id': terms['order_id'],
                                   _PREFIX + 'order_sequence': str(terms['order_sequence']),
                                   _PREFIX + 'environment': terms['environment'], _PREFIX + 'terms_hash': digest})
            saved[_PREFIX + 'state_hash'] = _state_hash(saved)
            pipe.multi()
            pipe.hset(key, mapping=saved)
            pipe.execute()
            return dict(terms, status='applied')
    except WatchError as error:
        raise QuotaConflict('quotas changed; retry original order') from error


def _state_hash(values):
    data = {k: str(values.get(k, '')) for k in _FIELDS}
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
