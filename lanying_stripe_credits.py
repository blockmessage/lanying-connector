"""Stripe credit consent and grants; no prices, card calls, or renewal scheduler."""
import calendar
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
