"""Stripe entitlement projection. Payment, invoices and renewal belong to Stripe/Butler.

MySQL is authoritative. Redis is a shared cache, never the only entitlement record.
The caller supplies a monotonically increasing revision, not a document/payment ID
that a client may edit. Only the existing authenticated service endpoint can apply it.
"""
import json
import time
from decimal import Decimal
from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, MetaData, String, Table, Text, select, update
from redis.exceptions import WatchError
import lanying_agent_tools_storage
import lanying_gift_quota
import lanying_redis

metadata = MetaData()
entitlements = Table('seenical_stripe_entitlement', metadata,
    Column('app_id', String(64), primary_key=True),
    Column('environment', String(16), nullable=False),
    Column('revision', BigInteger, nullable=False),
    Column('value_json', Text, nullable=False))

FIELDS = {'app_id', 'environment', 'revision', 'status', 'subscription_id',
          'period_start', 'period_end', 'product_id', 'message_allowance',
          'storage_limit', 'hosting_product_id', 'website_storage_limit',
          'website_traffic_limit', 'max_domain_num'}


def _key(app_id):
    return 'seenical:stripe:entitlement:' + app_id


def _cache(redis, app_id, value):
    """An older concurrent read must never replace a newer committed projection."""
    key = _key(app_id)
    for _ in range(5):
        with redis.pipeline() as pipe:
            try:
                pipe.watch(key)
                raw = pipe.get(key)
                current = json.loads(raw) if raw else None
                if current and int(current['revision']) >= int((value or {}).get('revision', 0)):
                    return current
                pipe.multi()
                pipe.set(key, json.dumps(value, separators=(',', ':')), ex=300 if value else 30)
                pipe.execute()
                return value
            except WatchError:
                continue
    raise RuntimeError('stripe entitlement cache contention')


def state(app_id):
    if not lanying_agent_tools_storage.is_enabled():
        return None
    redis = lanying_redis.get_redis_connection()
    raw = redis.get(_key(app_id))
    if raw is not None:
        return json.loads(raw)
    with lanying_gift_quota.engine().connect() as conn:
        row = conn.execute(select(entitlements.c.value_json)
                           .where(entitlements.c.app_id == app_id)).scalar()
    return _cache(redis, app_id, json.loads(row) if row else None)


def validate(data):
    if not isinstance(data, dict) or set(data) != FIELDS:
        raise ValueError('invalid stripe entitlement fields')
    if (not isinstance(data['app_id'], str) or not data['app_id'] or len(data['app_id']) > 64
            or data['environment'] not in ('test', 'live') or data['status'] not in ('active', 'paused', 'free')
            or not isinstance(data['subscription_id'], str) or not data['subscription_id'].startswith('sub_')):
        raise ValueError('invalid stripe entitlement identity')
    for field in ('revision', 'period_start', 'period_end', 'product_id', 'hosting_product_id',
                  'storage_limit', 'website_storage_limit', 'max_domain_num'):
        if type(data[field]) is not int or data[field] < 0:
            raise ValueError('invalid stripe entitlement number')
    if data['revision'] < 1 or data['period_end'] <= data['period_start']:
        raise ValueError('invalid stripe period')
    lanying_gift_quota.amount(data['message_allowance'])
    if not Decimal(str(data['website_traffic_limit'])).is_finite() or Decimal(str(data['website_traffic_limit'])) < 0:
        raise ValueError('invalid traffic allowance')


def apply(data):
    validate(data)
    encoded = json.dumps(data, sort_keys=True, separators=(',', ':'))
    with lanying_gift_quota.engine().begin() as conn:
        lanying_gift_quota._ignore(conn, entitlements, dict(app_id=data['app_id'],
            environment=data['environment'], revision=data['revision'], value_json=encoded))
        row = conn.execute(select(entitlements).where(entitlements.c.app_id == data['app_id'])
                           .with_for_update()).mappings().one()
        if row['environment'] != data['environment']:
            raise ValueError('stripe environment mismatch')
        if row['revision'] == data['revision'] and row['value_json'] != encoded:
            raise ValueError('stripe revision content mismatch')
        if row['revision'] < data['revision']:
            conn.execute(update(entitlements).where(entitlements.c.app_id == data['app_id'])
                         .values(revision=data['revision'], value_json=encoded))
            current = data
        else:
            current = json.loads(row['value_json'])
    _cache(lanying_redis.get_redis_connection(), data['app_id'], current)
    return {'revision': current['revision'], 'applied': current['revision'] == data['revision']}


def overlay(app_id, config):
    current = state(app_id)
    if current is None or config is None:
        return config
    result = dict(config)
    result.update(product_id=current['product_id'], message_per_month=float(current['message_allowance']),
                  stripe_billing=current, enable_extra_price=False)
    return result


def paused(config, now=None):
    current = (config or {}).get('stripe_billing')
    if not current or current['status'] == 'free':
        return False
    return current['status'] != 'active' or (time.time() if now is None else now) >= current['period_end']


def quota_period(current):
    from datetime import datetime, timezone
    return {'start': datetime.fromtimestamp(current['period_start'], timezone.utc).isoformat(),
            'end': datetime.fromtimestamp(current['period_end'], timezone.utc).isoformat(),
            'expires_epoch': current['period_end']}


def config_limit(app_id, key):
    fields = {'lanying_connector.storage_limit': 'storage_limit',
              'lanying_connector.grow_ai_website_storage_limit': 'website_storage_limit',
              'lanying_connector.storage_payg': None}
    if key not in fields:
        return None
    current = state(app_id)
    if current is None:
        return None
    return int(current[fields[key]]) if fields[key] else 0


def hosting(app_id, legacy):
    current = state(app_id)
    if current is None:
        return legacy
    result = dict(legacy or {})
    result.update(product_id=current['hosting_product_id'],
                  website_storage_limit=current['website_storage_limit'],
                  website_traffic_limit=current['website_traffic_limit'],
                  create_time=current['period_start'],
                  pay_start_date=datetime.fromtimestamp(current['period_start'], timezone.utc).strftime('%Y-%m-%d'),
                  stripe_billing=current,
                  status='stopped' if paused({'stripe_billing': current}) else 'normal')
    return result


def site_access(site_id, hostname):
    """Billing-only decision for an authenticated edge; never grants private-site authorization."""
    if (not isinstance(site_id, str) or not site_id or len(site_id) > 128
            or not isinstance(hostname, str) or not hostname or len(hostname) > 253
            or any(c in hostname for c in '/:@\\\n\r')):
        raise ValueError('invalid site')
    try:
        redis = lanying_redis.get_redis_connection()
        app_id = redis.hget('lanying_connector:grow_ai:all_site_list', site_id)
        if isinstance(app_id, bytes):
            app_id = app_id.decode('utf-8')
        if not app_id:
            return {'allowed': False, 'reason': 'unknown_site'}
        current = state(app_id)
        blocked = paused({'stripe_billing': current})
        if blocked:
            return {'allowed': False, 'reason': 'subscription_paused'}
        if current:
            import lanying_grow_ai
            site = lanying_grow_ai.get_site(app_id, site_id)
            if not site:
                return {'allowed': False, 'reason': 'unknown_site'}
            domain_id = site.get('domain_id')
            domain = lanying_grow_ai.get_custom_domain_info(app_id, site_id, domain_id) if domain_id else {}
            if domain and hostname.lower() == domain.get('domain_name', '').lower():
                # Preserve the existing domain-limit ordering; directories are not access boundaries.
                domains = [item for item in lanying_grow_ai.get_site_list(app_id)['data']['list'] if item.get('domain_id')]
                allowed = site_id in [str(item['site_id']) for item in domains[:current['max_domain_num']]]
                return {'allowed': allowed, 'reason': 'allowed' if allowed else 'domain_limit'}
        return {'allowed': True, 'reason': 'allowed'}
    except Exception:
        import logging
        logging.warning('Site billing access check unavailable | site_id=%s', site_id)
        return {'allowed': True, 'reason': 'check_unavailable'}
