"""Durable invitation credits. Redis holds only settlement delivery receipts.

Existing gross statistics remain untouched; billing subtracts MySQL offsets.
No grant or consumption is stored solely in process memory or Redis.
"""
import hashlib
import json
import os
import time
import logging
from decimal import Decimal, ROUND_HALF_EVEN

from sqlalchemy import (BigInteger, Boolean, Column, MetaData, Numeric, String,
                        Table, insert, select, update, func)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import lanying_agent_tools_storage
from redis.exceptions import WatchError

metadata = MetaData()
grants = Table('message_quota_gift', metadata,
    Column('business_id', String(96), primary_key=True),
    Column('app_id', String(64), nullable=False),
    Column('points', Numeric(20, 4), nullable=False),
    Column('remaining', Numeric(20, 4), nullable=False),
    Column('granted_epoch', BigInteger, nullable=False),
    Column('expires_epoch', BigInteger, nullable=False))
usage = Table('message_quota_gift_usage', metadata,
    Column('usage_id', String(64), primary_key=True),
    Column('app_id', String(64), nullable=False),
    Column('quota', Numeric(20, 4), nullable=False),
    Column('occurred_epoch', BigInteger, nullable=False),
    Column('arguments_hash', String(64), nullable=False),
    Column('gift_used', Numeric(20, 4), nullable=False),
    Column('applied', Boolean, nullable=False, default=False))
offsets = Table('message_quota_gift_offset', metadata,
    Column('usage_id', String(64), primary_key=True),
    Column('statistic_key', String(255), primary_key=True),
    Column('gift_used', Numeric(20, 4), nullable=False))

def enabled():
    return os.getenv('SEENICAL_GIFT_QUOTA_ENABLED', 'off').lower() in ('on', 'true', '1')

def engine():
    value = lanying_agent_tools_storage.get_engine()
    if value is None:
        raise RuntimeError('gift quota MySQL not configured')
    return value

def amount(value):
    if isinstance(value, bool):
        raise ValueError('invalid quota')
    result = Decimal(str(value))
    if not result.is_finite() or result < 0 or result > 1000000000:
        raise ValueError('invalid quota')
    return result.quantize(Decimal('.0001'), rounding=ROUND_HALF_EVEN)

def _ignore(conn, table, values):
    # Dialect-native inserts keep the same uniqueness rule in SQLite unit tests.
    stmt = (mysql_insert(table).prefix_with('IGNORE') if conn.dialect.name == 'mysql'
            else sqlite_insert(table).on_conflict_do_nothing())
    return conn.execute(stmt.values(**values)).rowcount

def grant(business_id, app_id, points, now=None):
    if not enabled():
        raise ValueError('gift quota feature disabled')
    points = amount(points)
    if not str(business_id).startswith('seenical-invitation:') or len(str(business_id)) > 96 or not app_id or points <= 0 or points != int(points):
        raise ValueError('invalid grant')
    now = int(time.time()) if now is None else int(now)
    with engine().begin() as conn:
        _ignore(conn, grants, dict(business_id=business_id, app_id=app_id, points=points,
            remaining=points, granted_epoch=now, expires_epoch=now+30*86400))
        row = conn.execute(select(grants).where(grants.c.business_id == business_id)).mappings().one()
        if row['app_id'] != app_id or row['points'] != points:
            raise ValueError('grant identity mismatch')
        return dict(business_id=business_id, expires_epoch=row['expires_epoch'])

def status(app_id, now=None):
    if not enabled():
        return dict(remaining=None, expires_epoch=None, enabled=False)
    now = int(time.time()) if now is None else int(now)
    with engine().connect() as conn:
        rows = conn.execute(select(grants).where(grants.c.app_id == app_id,
            grants.c.expires_epoch > now, grants.c.remaining > 0)).mappings().all()
    return dict(remaining=float(sum((r['remaining'] for r in rows), Decimal(0))),
                expires_epoch=min((r['expires_epoch'] for r in rows), default=None))

def billable_used(statistic_key, gross):
    if not enabled():
        return gross
    with engine().connect() as conn:
        offset = conn.execute(select(func.coalesce(func.sum(offsets.c.gift_used), 0))
            .where(offsets.c.statistic_key == statistic_key)).scalar_one()
    # Legacy counters and Butler expect integer message quota; preserve rounding.
    return max(0, int(Decimal(str(gross)) - Decimal(str(offset))))

def settle(payload):
    """Commit a consumption once; Redis delivery may then be retried safely."""
    used = amount(payload['quota'])
    arguments_hash = hashlib.sha256(json.dumps(payload, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()
    with engine().begin() as conn:
        inserted = _ignore(conn, usage, dict(usage_id=payload['usage_id'],
            app_id=payload['app_id'], quota=used, occurred_epoch=payload['occurred_epoch'],
            arguments_hash=arguments_hash, gift_used=0, applied=False))
        row = conn.execute(select(usage).where(usage.c.usage_id == payload['usage_id'])
            .with_for_update()).mappings().one()
        if row['arguments_hash'] != arguments_hash:
            raise ValueError('usage identity mismatch')
        if not inserted:
            return dict(gift_used=float(row['gift_used']), applied=row['applied'])
        now = int(payload['occurred_epoch'])
        rows = conn.execute(select(grants).where(grants.c.app_id == payload['app_id'],
            grants.c.granted_epoch <= now, grants.c.expires_epoch > now,
            grants.c.remaining > 0).order_by(grants.c.expires_epoch)
            .with_for_update()).mappings().all()
        gift = Decimal(0)
        for r in rows:
            take = min(used-gift, r['remaining'])
            if take <= 0:
                break
            conn.execute(update(grants).where(grants.c.business_id == r['business_id'])
                .values(remaining=r['remaining']-take))
            gift += take
        conn.execute(update(usage).where(usage.c.usage_id == payload['usage_id']).values(gift_used=gift))
        for key in set(payload['keys']):
            conn.execute(insert(offsets).values(usage_id=payload['usage_id'], statistic_key=key, gift_used=gift))
        return dict(gift_used=float(gift), applied=False)

def mark_applied(usage_id):
    with engine().begin() as conn:
        conn.execute(update(usage).where(usage.c.usage_id == usage_id).values(applied=True))

PENDING_KEY = 'seenical:gift_quota:pending_settlements'

def apply_statistics(redis, payload):
    """Queue first, then commit the credit and atomically deliver gross counters.

    Retrying a interrupted delivery cannot re-spend the credit or re-increment
    Redis. The receipt bridges the MySQL commit/Redis EXEC acknowledgement gap.
    """
    serialized = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    usage_id = payload['usage_id']
    new_pending = bool(redis.hsetnx(PENDING_KEY, usage_id, serialized))
    if not new_pending:
        existing = redis.hget(PENDING_KEY, usage_id)
        if isinstance(existing, bytes): existing = existing.decode()
        if existing != serialized:
            raise ValueError('pending usage identity mismatch')
    result = settle(payload)
    if result['applied']:
        removed = redis.hdel(PENDING_KEY, usage_id)
        # An existing pending row means a previous final acknowledgement was
        # interrupted. Only its remover emits the recovered billing callback.
        return bool(removed and not new_pending)
    receipt = 'seenical:gift_quota:applied:' + usage_id
    fields = ['message_count_quota', 'message_count_quota_' + payload['api_key_type']]
    capsule_entries = payload.get('capsule_statistics', [])
    watched_keys = set(payload['keys'])
    for entry in capsule_entries:
        watched_keys.update((entry['key'], entry['app_ids_key']))
    if payload.get('origin'):
        fields.append(payload['origin'])
    for _ in range(5):
        try:
            with redis.pipeline() as pipe:
                pipe.watch(receipt, *watched_keys)
                if pipe.exists(receipt):
                    break
                values = {}
                units = int(amount(payload['quota'])*10000)
                for key in payload['keys']:
                    current = {k.decode() if isinstance(k, bytes) else k:
                               int(v) for k, v in pipe.hgetall(key).items()}
                    changes = {}
                    for field in set(fields):
                        integer, fraction = divmod(current.get(field+'_float', 0)+units, 10000)
                        changes[field] = current.get(field, 0)+integer
                        changes[field+'_float'] = fraction
                    for field, count in payload.get('counters', {}).items():
                        changes[field] = current.get(field, 0)+int(count)
                    values[key] = changes
                # Validate all numeric inputs before MULTI. Redis transactions
                # do not roll back runtime command errors: precomputed HSETs
                # avoid writing the receipt after a failed income increment.
                for entry in capsule_entries:
                    key, field = entry['key'], entry['field']
                    changes = values.setdefault(key, {})
                    current = changes.get(field)
                    if current is None:
                        current = pipe.hget(key, field) or '0'
                        if isinstance(current, bytes): current = current.decode()
                    income = Decimal(str(current)) + amount(payload['quota'])
                    if not income.is_finite():
                        raise ValueError('invalid capsule quota statistic')
                    changes[field] = str(income)
                    ids_key, owner = entry['app_ids_key'], entry['capsule_app_id']
                    counts = values.setdefault(ids_key, {})
                    if owner not in counts:
                        counts[owner] = int(pipe.hget(ids_key, owner) or 0)
                    counts[owner] += 1
                pipe.multi()
                for key, changes in values.items():
                    pipe.hset(key, mapping=changes)
                pipe.setex(receipt, 365*86400, '1')
                pipe.execute()
                break
        except WatchError:
            continue
    else:
        raise RuntimeError('quota statistics busy; settlement remains pending')
    mark_applied(usage_id)
    return bool(redis.hdel(PENDING_KEY, usage_id))

def retry_pending(redis, limit=100, on_applied=None):
    if not enabled():
        return
    cursor = 0
    handled = 0
    while handled < limit:
        cursor, rows = redis.hscan(PENDING_KEY, cursor=cursor, count=min(100, limit-handled))
        for raw in rows.values():
            if handled >= limit:
                return
            handled += 1
            try:
                payload = json.loads(raw)
                if int(time.time()) - int(payload['occurred_epoch']) < 60:
                    continue  # Let the originating request finish its normal settlement.
                if apply_statistics(redis, payload) and on_applied:
                    on_applied(payload['app_id'])
            except Exception as e:
                logging.warning('gift quota settlement retry failed | error_type=%s', type(e).__name__)
        if not cursor:
            break
