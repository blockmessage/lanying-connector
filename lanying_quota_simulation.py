"""Operator-only synthetic usage. No HTTP endpoint and no model invocation.

In a Connector container with its normal environment, from /app:

    from lanying_quota_simulation import simulate_message_quota
    simulate_message_quota('your-app-id', 30, 'quota-test-001')

This consumes REAL quota and can trigger REAL automatic purchases. Free-plan
usage can affect the tenant's shared allowance. There is no automatic rollback.
Reuse the same simulation_id after a timeout; use a new ID for a new consumption.
Normal quota admission (including the daily fuse) still applies. This tests quota
accounting, not model permissions, provider costs, or actual model execution.
"""
import hashlib
import json
import logging
import re
import time

from sqlalchemy import func, select, update
import lanying_gift_quota as credits


def _service():
    from services import openai_service
    return openai_service


def reset_free_tenant_quota(app_id, tenant_id):
    """Reset this free App's shared tenant/period allowance for another test.

    Stop traffic for the test tenant and let in-flight requests finish first.
    Example: reset_free_tenant_quota('your-app-id', 12345)
    Grant balances, consumption receipts and per-App statistics are preserved.
    If either store fails, keep traffic stopped and repeat the same reset.
    """
    if type(tenant_id) is not int or tenant_id <= 0:
        raise ValueError('expected a positive numeric tenant_id')
    service = _service()
    service.lanying_config.init()
    if service.lanying_config.mode != 'etcd':
        raise ValueError('App-specific etcd configuration is required')
    config = service.lanying_config.get_lanying_connector(app_id)
    if not config or config.get('product_id') != 7001 or config.get('tenant_id') != tenant_id:
        raise ValueError('App must use the free message plan and belong to the specified tenant')
    keys = service.get_message_statistic_keys(config, app_id)
    key = keys[0]
    if not key.startswith(f'lanying:connector:statistics:message:pay_start_date:{tenant_id}:') or key in keys[1:]:
        raise ValueError('App does not have a distinct shared free quota key')
    redis = service.lanying_redis.get_redis_connection()
    if redis is None:
        raise RuntimeError('quota Redis unavailable')

    fields = {'message_count_quota': 0, 'message_count_quota_float': 0}
    if credits.enabled():
        # A queued settlement can exist before its SQL transaction starts.
        cursor = 0
        while True:
            cursor, rows = redis.hscan(credits.PENDING_KEY, cursor=cursor, count=100)
            if any(key in json.loads(raw)['keys'] for raw in rows.values()):
                raise RuntimeError('pending quota settlement for this period; retry after recovery')
            if not cursor:
                break
        with credits.engine().begin() as conn:
            if credits.purchases_enabled():
                conn.execute(update(credits.packages).where(credits.packages.c.statistic_key == key).values(used=0))
            offsets, usage = credits.offsets, credits.usage
            pending = select(usage.c.usage_id).join_from(usage, offsets, usage.c.usage_id == offsets.c.usage_id).where(
                offsets.c.statistic_key == key, usage.c.applied == False).limit(1)
            if conn.execute(pending).first():
                raise RuntimeError('pending quota settlement for this period; retry after recovery')
            # Billing subtracts these historical offsets from gross Redis usage.
            covered = conn.execute(select(func.coalesce(func.sum(offsets.c.gift_used + offsets.c.purchase_used), 0))
                                   .where(offsets.c.statistic_key == key)).scalar_one()
            fields['message_count_quota'], fields['message_count_quota_float'] = divmod(int(covered * 10000), 10000)
            redis.hset(key, mapping=fields)
    else:
        redis.hset(key, mapping=fields)
    logging.info('Reset free shared quota | app_id:%s tenant_id:%s period_key:%s', app_id, tenant_id, key)
    return dict(app_id=app_id, tenant_id=tenant_id, statistic_key=key, status='reset')


def simulate_message_quota(app_id, quota, simulation_id):
    """Consume up to 10,000 points once per App + operator-supplied test ID.

    Returns completed/already_completed or the normal admission rejection.
    Exceptions after settlement begins must be retried with the SAME test ID.
    """
    if not isinstance(app_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', app_id):
        raise ValueError('invalid app_id')
    if not isinstance(simulation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', simulation_id):
        raise ValueError('simulation_id must contain 1-64 letters, digits, underscores or hyphens')
    points = credits.amount(quota)
    if not 0 < points <= 10000:
        raise ValueError('quota must be positive and at most 10000')
    if not credits.purchases_enabled():
        raise ValueError('durable purchased-credit accounting must be enabled')
    usage_id = hashlib.sha256(('quota-simulation:' + app_id + ':' + simulation_id).encode()).hexdigest()
    service = _service()
    service.lanying_config.init()
    # Never use the shared sample env configuration for an arbitrary App ID.
    if service.lanying_config.mode != 'etcd':
        raise ValueError('App-specific etcd configuration is required')
    config = service.lanying_config.get_lanying_connector(app_id)
    if not config or not config.get('product_id'):
        raise ValueError('App has no active intelligent message service')
    redis = service.lanying_redis.get_redis_connection()
    if redis is None:
        raise RuntimeError('quota Redis unavailable')

    with credits.engine().connect() as conn:
        existing = conn.execute(select(credits.usage).where(credits.usage.c.usage_id == usage_id)).mappings().first()
    if existing and (existing['app_id'] != app_id or existing['quota'] != points):
        raise ValueError('simulation_id was already used with different arguments')
    if existing and existing['applied']:
        return dict(status='already_completed', usage_id=usage_id,
                    state=service.get_message_limit_state(app_id))

    pending = redis.hget(credits.PENDING_KEY, usage_id)
    if pending:
        payload = json.loads(pending)
        if payload['app_id'] != app_id or credits.amount(payload['quota']) != points:
            raise ValueError('simulation_id was already used with different arguments')
        # The existing settlement worker can deliver old-period pending entries.
        # Do not run today's billing callback for a previous period/day.
        if payload['keys'] != service.get_message_statistic_keys(config, app_id):
            raise ValueError('pending simulation belongs to an earlier period; wait for settlement recovery')
    else:
        if existing:
            raise RuntimeError('pending settlement payload missing; inspect original usage, do not create a new test ID')
        for check in (service.check_message_deduct_failed,
                      lambda app, cfg: service.check_deduct_message_quota(app, cfg, float(points))):
            result = check(app_id, config)
            if result['result'] != 'ok':
                return dict(status='rejected', usage_id=usage_id, reason=result)
        keys = service.get_message_statistic_keys(config, app_id)
        payload = dict(usage_id=usage_id, app_id=app_id, quota=float(points), keys=keys,
                       api_key_type='share', counters={}, origin='quota_simulation',
                       package_allowance=config.get('message_per_month', 0)
                       + int(redis.hget(keys[0], 'message_count_quota_buy') or 0),
                       occurred_epoch=int(time.time()))

    logging.warning('Simulating real quota consumption | app_id:%s | simulation_id:%s | quota:%s',
                    app_id, simulation_id, points)
    service._record_quota_statistics(redis, app_id, config, float(points), 'share',
                                     response={'_seenical_quota_settlement': (payload,)},
                                     origin='quota_simulation')
    return dict(status='completed', usage_id=usage_id,
                state=service.get_message_limit_state(app_id))
