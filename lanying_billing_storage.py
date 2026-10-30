"""Verified deployment sizes and immutable closed-period storage receipts.

Legacy display counters are not billing evidence: they are written before the
upload succeeds. No traffic is measured or charged by this module.
"""
import json
import re
import time

from redis.exceptions import WatchError
import lanying_redis
from lanying_billing_hosting import _state_hash


def stage(app, site, code, size, redis=None):
    if type(size) is not int or not 0 <= size <= 2**63 - 1:
        raise ValueError('invalid release size')
    redis = redis or lanying_redis.get_redis_connection()
    site_key = f'lanying_connector:grow_ai:site:{app}:{site}'
    code_key = f'lanying_connector:grow_ai:deploy_code:{code}'
    with redis.pipeline() as pipe:
        pipe.watch(site_key, code_key)
        info = lanying_redis.redis_hgetall(pipe, site_key)
        raw = pipe.get(code_key)
        if not info or not raw:
            raise ValueError('deployment unavailable')
        data = json.loads(raw)
        if str(data.get('app_id')) != app or str(data.get('site_id')) != str(site):
            raise ValueError('deployment identity differs')
        if 'billing_size' in data and data['billing_size'] != size:
            raise ValueError('deployment size differs')
        if data.get('billing_outcome'):
            raise ValueError('deployment already finished')
        pending = info.get('billing_storage_pending')
        if pending and pending != code:
            # Never allow an overlapping deployment to produce apparently
            # verified storage. An expired callback requires a new deployment.
            if pipe.exists(f'lanying_connector:grow_ai:deploy_code:{pending}'):
                raise ValueError('another deployment is pending')
        ttl = pipe.ttl(code_key)
        if ttl <= 0:
            raise ValueError('deployment expired')
        data['billing_size'] = size
        pipe.multi()
        if pending and pending != code:
            pipe.hset(site_key, 'billing_storage_verified', '0')
        pipe.hset(site_key, 'billing_storage_pending', code)
        pipe.setex(code_key, ttl, json.dumps(data))
        pipe.execute()


def finish(app, site, code, success, redis=None):
    redis = redis or lanying_redis.get_redis_connection()
    site_key = f'lanying_connector:grow_ai:site:{app}:{site}'
    code_key = f'lanying_connector:grow_ai:deploy_code:{code}'
    with redis.pipeline() as pipe:
        pipe.watch(site_key, code_key)
        info = lanying_redis.redis_hgetall(pipe, site_key)
        raw = pipe.get(code_key)
        if not info or not raw:
            raise ValueError('deployment unavailable')
        data = json.loads(raw)
        outcome = 'success' if success else 'failed'
        if data.get('billing_outcome'):
            if data['billing_outcome'] != outcome:
                raise ValueError('deployment outcome differs')
            return
        ttl = pipe.ttl(code_key)
        if ttl <= 0:
            raise ValueError('deployment expired')
        verified = (info.get('billing_storage_pending') == code
                    and type(data.get('billing_size')) is int and data['billing_size'] >= 0)
        data['billing_outcome'] = outcome
        pipe.multi()
        if verified:
            if success:
                pipe.hset(site_key, mapping={'billing_storage_bytes': data['billing_size'],
                                            'billing_storage_verified': '1'})
            pipe.hdel(site_key, 'billing_storage_pending')
        else:
            # A callback from an old deployment cannot certify a newer one.
            pipe.hset(site_key, 'billing_storage_verified', '0')
        pipe.setex(code_key, ttl, json.dumps(data))
        pipe.execute()


def close(data, redis=None):
    if (not isinstance(data, dict) or set(data) != {'app_id', 'order_id', 'environment', 'period_end_epoch'}
            or not isinstance(data['app_id'], str) or not re.fullmatch(r'[a-z0-9]{1,64}', data['app_id'])
            or not isinstance(data['order_id'], str) or not re.fullmatch(r'[0-9a-f]{32}', data['order_id'])
            or data['environment'] not in ('TEST', 'LIVE') or type(data['period_end_epoch']) is not int
            or not 0 < data['period_end_epoch'] <= time.time()):
        raise ValueError('invalid closed period')
    redis = redis or lanying_redis.get_redis_connection()
    app = data['app_id']
    receipt_key = f"lanying_connector:billing:storage:{app}:{data['order_id']}"
    service_key = f'lanying-connector:grow_ai:service_status:{app}'
    list_key = f'lanying_connector:grow_ai:site_list:{app}'
    with redis.pipeline() as pipe:
        pipe.watch(receipt_key, service_key, list_key)
        old = pipe.get(receipt_key)
        if old:
            saved = json.loads(old)
            if any(saved.get(k) != v for k, v in data.items()):
                raise ValueError('receipt scope differs')
            return saved
        state = lanying_redis.redis_hgetall(pipe, service_key)
        if (state.get('billing_writer') != 'ORDER' or state.get('billing_order_id') != data['order_id']
                or state.get('billing_environment') != data['environment']
                or state.get('billing_period_end_epoch') != str(data['period_end_epoch'])
                or state.get('billing_state_hash') != _state_hash(state)):
            raise ValueError('hosting period differs')
        keys = [f'lanying_connector:grow_ai:site:{app}:{s}' for s in
                set(lanying_redis.redis_lrange(pipe, list_key, 0, -1))]
        if keys:
            pipe.watch(*keys)
        total, verified = 0, True
        for key in keys:
            site = lanying_redis.redis_hgetall(pipe, key)
            size = site.get('billing_storage_bytes', '')
            if (site.get('billing_storage_verified') != '1' or site.get('billing_storage_pending')
                    or not re.fullmatch(r'[0-9]+', size)):
                verified = False
            else:
                total += int(size)
        if total > 2**63 - 1:
            raise ValueError('storage overflow')
        saved = dict(data, status='verified' if verified else 'unverified',
                     storage_bytes=total if verified else None, captured_epoch=int(time.time()))
        pipe.multi()
        pipe.set(receipt_key, json.dumps(saved))
        pipe.execute()
        return saved
