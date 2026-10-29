"""Internal, idempotent hosting-period state. This does not debit or set quotas.

The Butler delivery adapter must also synchronize actual storage/traffic limits
before completing HOSTING. Old open_service remains unchanged.
"""
import hashlib
import json
import re
import time
from datetime import date
from decimal import Decimal, InvalidOperation

from redis.exceptions import WatchError
import lanying_redis


class ProvisionConflict(ValueError):
    pass


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('invalid hosting value')
    return value


def _terms(data):
    fields = {'app_id', 'order_id', 'order_sequence', 'environment', 'product_id',
              'legacy_price_cny', 'storage_mb', 'traffic_gb', 'period_start',
              'period_end', 'preserve_usage_period'}
    if not isinstance(data, dict) or set(data) != fields:
        raise ValueError('invalid hosting fields')
    if not isinstance(data['app_id'], str) or not re.fullmatch(r'[a-z0-9]{1,64}', data['app_id']):
        raise ValueError('invalid app')
    if not isinstance(data['order_id'], str) or not re.fullmatch(r'[0-9a-f]{32}', data['order_id']):
        raise ValueError('invalid order')
    if data['environment'] not in ('TEST', 'LIVE') or type(data['preserve_usage_period']) is not bool:
        raise ValueError('invalid hosting scope')
    value = dict(data)
    _integer(value['order_sequence'], 1, 2**63 - 1)
    _integer(value['product_id'], 9801, 9899)
    _integer(value['legacy_price_cny'], 0, 2**31 - 1)
    _integer(value['storage_mb'], 0, 2**31 - 1)
    # Exact decimal text avoids float/NaN input changing the replay identity.
    if not isinstance(value['traffic_gb'], str) or not re.fullmatch(r'[0-9]+(?:\.[0-9]{1,6})?', value['traffic_gb']):
        raise ValueError('invalid traffic')
    try:
        traffic = Decimal(value['traffic_gb'])
    except InvalidOperation as error:
        raise ValueError('invalid traffic') from error
    if traffic > 2**31 - 1:
        raise ValueError('invalid traffic')
    value['traffic_gb'] = format(traffic.normalize(), 'f')
    for key in ('period_start', 'period_end'):
        if not isinstance(value[key], str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value[key]):
            raise ValueError('invalid billing period')
    start, end = date.fromisoformat(value['period_start']), date.fromisoformat(value['period_end'])
    if not 0 < (end - start).days <= 366:
        raise ValueError('invalid billing period')
    return value


def provision(data, redis=None):
    terms = _terms(data)
    digest = hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    redis = redis if redis is not None else lanying_redis.get_redis_connection()
    key = 'lanying-connector:grow_ai:service_status:' + terms['app_id']
    try:
        with redis.pipeline() as pipe:
            pipe.watch(key)
            old = lanying_redis.redis_hgetall(pipe, key)
            if old.get('billing_order_id') == terms['order_id']:
                if old.get('billing_terms_hash') != digest:
                    raise ProvisionConflict('original hosting terms differ')
                _verify_applied(old, terms)
                return _receipt(old)
            previous_sequence = int(old.get('billing_order_sequence', 0))
            if previous_sequence >= terms['order_sequence']:
                raise ProvisionConflict('hosting order has been superseded')
            if terms['environment'] == 'TEST' and old:
                verified_test = (old.get('billing_environment') == 'TEST' and old.get('billing_writer') == 'ORDER'
                                 and old.get('billing_state_hash') == _state_hash(old))
                legacy_free = (not old.get('billing_environment') and old.get('product_id') == '9851'
                               and old.get('price') == '0')
                if not (verified_test or legacy_free):
                    raise ProvisionConflict('test order cannot replace unverified or live hosting')
            if not date.fromisoformat(terms['period_start']) <= date.today() < date.fromisoformat(terms['period_end']):
                raise ProvisionConflict('hosting period is not active')
            start = terms['period_start']
            if terms['preserve_usage_period']:
                if (old.get('status') != 'normal' or int(old.get('product_id', 0)) != terms['product_id']
                        or int(old.get('website_storage_limit', -1)) != terms['storage_mb']
                        or Decimal(old.get('website_traffic_limit', '-1')) != Decimal(terms['traffic_gb'])):
                    raise ProvisionConflict('hosting service changed before reuse')
                start = old.get('pay_start_date', '')
                date.fromisoformat(start)  # Missing/corrupt state must not reset usage silently.
            next_state = {
                'app_id': terms['app_id'], 'create_time': old.get('create_time', int(time.time())),
                'status': 'normal', 'pay_start_date': start,
                'product_id': terms['product_id'], 'price': terms['legacy_price_cny'],
                'website_storage_limit': terms['storage_mb'], 'website_traffic_limit': terms['traffic_gb'],
                'billing_writer': 'ORDER', 'billing_order_id': terms['order_id'], 'billing_order_sequence': terms['order_sequence'],
                'billing_environment': terms['environment'], 'billing_terms_hash': digest,
                'billing_period_start': terms['period_start'], 'billing_period_end': terms['period_end'],
                'billing_usage_period_start': start,
            }
            next_state['billing_state_hash'] = _state_hash(next_state)
            pipe.multi()
            pipe.hset(key, mapping=next_state)
            pipe.execute()
            return _receipt(next_state)
    except WatchError as error:
        # A bounded visible retry, never an overwrite of a concurrent activation.
        raise ProvisionConflict('hosting changed; retry original order') from error


def _state_hash(state):
    # Detect legacy writes that retain TEST metadata. Usage counters are mutable
    # and deliberately excluded; service identity/limits/anchors are not.
    fields = ('app_id', 'create_time', 'status', 'pay_start_date', 'product_id', 'price',
              'website_storage_limit', 'website_traffic_limit', 'billing_order_id', 'billing_writer',
              'billing_order_sequence', 'billing_environment', 'billing_period_start',
              'billing_period_end', 'billing_usage_period_start')
    values = {key: str(state.get(key, '')) for key in fields}
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _verify_applied(state, terms):
    # Legacy close/open calls may leave billing metadata behind. Never report a
    # completed replay after the actual service has changed, or restore it here.
    expected = {'app_id': terms['app_id'], 'status': 'normal',
                'product_id': str(terms['product_id']), 'price': str(terms['legacy_price_cny']),
                'website_storage_limit': str(terms['storage_mb']),
                'billing_order_sequence': str(terms['order_sequence']),
                'billing_environment': terms['environment'],
                'billing_period_start': terms['period_start'], 'billing_period_end': terms['period_end']}
    if (state.get('billing_writer') != 'ORDER'
            or state.get('billing_state_hash') != _state_hash(state)
            or any(state.get(key) != value for key, value in expected.items())
            or Decimal(state.get('website_traffic_limit', '-1')) != Decimal(terms['traffic_gb'])
            or not state.get('billing_usage_period_start')
            or state.get('pay_start_date') != state['billing_usage_period_start']):
        raise ProvisionConflict('hosting service changed after recording')


def _receipt(state):
    return {'order_id': state['billing_order_id'], 'order_sequence': int(state['billing_order_sequence']),
            'environment': state['billing_environment'], 'product_id': int(state['product_id']),
            'period_start': state['billing_period_start'], 'period_end': state['billing_period_end'],
            'usage_period_start': state['pay_start_date'], 'status': 'recorded'}
