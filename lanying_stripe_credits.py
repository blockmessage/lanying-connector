"""Stripe credit consent and grants; no prices, card calls, or renewal scheduler."""
import calendar
import logging
import os
import requests
import lanying_redis
import re
import time
from datetime import datetime, timezone
from sqlalchemy import BigInteger, Boolean, Column, MetaData, String, Table, select, update
import lanying_gift_quota as quota
import lanying_agent_tools_storage

metadata = MetaData()
settings = Table('seenical_stripe_credit_setting', metadata,
    Column('app_id', String(64), primary_key=True),
    Column('environment', String(16), nullable=False),
    Column('revision', BigInteger, nullable=False),
    Column('auto_recharge', Boolean, nullable=False))


def preference(app_id):
    if not lanying_agent_tools_storage.is_enabled():
        return None
    with quota.engine().connect() as conn:
        row = conn.execute(select(settings).where(settings.c.app_id == app_id)).mappings().first()
    return None if row is None else dict(row)


def set_preference(app_id, environment, revision, enabled):
    if (not isinstance(app_id, str) or not app_id or len(app_id) > 64
            or environment not in ('test', 'live') or type(enabled) is not bool
            or type(revision) is not int or revision < 1):
        raise ValueError('invalid Stripe credit setting')
    with quota.engine().begin() as conn:
        quota._ignore(conn, settings, dict(app_id=app_id, environment=environment,
            revision=revision, auto_recharge=enabled))
        row = conn.execute(select(settings).where(settings.c.app_id == app_id).with_for_update()).mappings().one()
        if row['environment'] != environment or (row['revision'] == revision and row['auto_recharge'] != enabled):
            raise ValueError('Stripe credit setting identity mismatch')
        if row['revision'] < revision:
            conn.execute(update(settings).where(settings.c.app_id == app_id).values(revision=revision, auto_recharge=enabled))
        return {'applied': row['revision'] <= revision}


def grant(business_id, app_id, points, source, now=None):
    if not quota.purchases_enabled():
        raise ValueError('purchased credits disabled')
    points = quota.amount(points)
    if (not re.fullmatch(r'seenical-stripe-credit:[0-9a-f]{32}', str(business_id))
            or not app_id or source not in ('manual', 'auto') or points <= 0 or points != int(points)):
        raise ValueError('invalid Stripe credit grant')
    now = int(time.time()) if now is None else int(now)
    start = datetime.fromtimestamp(now, timezone.utc)
    year, month = (start.year+1, 1) if start.month == 12 else (start.year, start.month+1)
    expiry = int(start.replace(year=year, month=month, day=min(start.day, calendar.monthrange(year, month)[1])).timestamp())
    with quota.engine().begin() as conn:
        quota._ignore(conn, quota.grants, dict(business_id=business_id, app_id=app_id, points=points,
            remaining=points, granted_epoch=now, expires_epoch=expiry, credit_source=source))
        row = conn.execute(select(quota.grants).where(quota.grants.c.business_id == business_id).with_for_update()).mappings().one()
        if row['app_id'] != app_id or row['points'] != points or row['credit_source'] != source:
            raise ValueError('Stripe grant identity mismatch')
        return {'granted': True, 'granted_epoch': row['granted_epoch'], 'expires_epoch': row['expires_epoch']}


def inspect(business_id, app_id):
    if not re.fullmatch(r'seenical-stripe-credit:[0-9a-f]{32}', str(business_id)):
        raise ValueError('invalid credit identity')
    with quota.engine().connect() as conn:
        row=conn.execute(select(quota.grants).where(quota.grants.c.business_id==business_id)).mappings().first()
    if row is None:
        return {'granted':False}
    if row['app_id']!=app_id:
        raise ValueError('credit identity mismatch')
    return {'granted':True,'points':float(row['points']),'remaining':float(row['remaining']),
            'consumed':float(row['points']-row['remaining']),'granted_epoch':row['granted_epoch'],
            'expires_epoch':row['expires_epoch']}


# Shared across Connector workers; this is notification coalescing, not a payment lock.
# The durable Butler operation remains the authority for payment and recovery.
def notify_purchase(app_id):
    key = 'seenical:stripe:credit-notify:' + str(app_id)
    try:
        redis = lanying_redis.get_redis_connection()
        if not redis.set(key, 'processing', nx=True, ex=30):
            cached = redis.get(key)
            return cached.decode() if isinstance(cached, bytes) else (cached or 'processing')
    except Exception:
        logging.warning('Stripe credit notification unavailable | app_id=%s', app_id)
        return 'unknown'
    status = 'unknown'
    try:
        endpoint = os.getenv('LANYING_BUTLER_ENDPOINT', 'https://butler.lanyingim.com')
        response = requests.post(endpoint + '/app/lanying_connector_event',
            headers={'app_id': app_id},
            json={'app_id': app_id, 'event': 'message_count_quota_reached', 'data': {}},
            timeout=10)
        body = response.json() if response.ok else {}
        data = body.get('data')
        if body.get('code') == 200 and isinstance(data, dict):
            received = data.get('credit_purchase_status')
            if received in {'processing', 'payment_required', 'complete', 'disabled', 'unknown'}:
                status = received
    except Exception:
        # Timeout is not evidence of a declined card or a missing order.
        logging.warning('Stripe credit notification result unknown | app_id=%s', app_id)
    try:
        redis.set(key, status, ex=30)
    except Exception:
        logging.warning('Stripe credit notification status unavailable | app_id=%s', app_id)
    return status


def purchase_error(status):
    if status == 'disabled':
        return None
    if status == 'payment_required':
        code = 'credit_payment_required'
        zh = '智能消息点数付款未完成，请管理员在订阅页面查看原购买记录并按页面提示处理后重试。'
        en = 'Credit payment is incomplete. Ask an administrator to open the existing purchase on the subscription page and follow the instructions before retrying.'
    elif status == 'unknown':
        code = 'credit_payment_unconfirmed'
        zh = '暂时无法确认智能消息点数的付款或到账结果，请稍后重试，或由管理员查看原购买记录；请勿重复购买。'
        en = 'Credit payment or delivery could not be confirmed. Retry later or ask an administrator to check the existing purchase; do not buy again.'
    else:
        code = 'credit_purchase_processing'
        zh = '智能消息点数正在补充，请稍后重试；若持续未到账，请管理员查看原购买记录。'
        en = 'AI message credits are being replenished. Retry shortly; if delivery remains pending, ask an administrator to check the existing purchase.'
    return {'result': 'error', 'code': code, 'msg': zh, 'message': en}
