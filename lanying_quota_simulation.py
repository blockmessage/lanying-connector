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

from sqlalchemy import select
import lanying_gift_quota as credits


def _service():
    from services import openai_service
    return openai_service


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
