"""App-scoped purchased credits and explicit auto-purchase preferences."""
import lanying_gift_quota
import lanying_redis
from sqlalchemy import select, update

SETTING = 'lanying_connector.auto_recharge'

def preference_key(app_id):
    # Quota checking already requires this Redis. Do not introduce a Redis
    # Stack dependency into legacy chat-only deployments.
    return 'lanying_connector:message_credit_settings:' + app_id

def preference(app_id):
    raw = None
    if app_id:
        redis = lanying_redis.get_redis_connection()
        raw = lanying_redis.redis_hget(redis, preference_key(app_id), 'auto_recharge')
        if lanying_gift_quota.purchases_enabled():
            with lanying_gift_quota.engine().connect() as conn:
                saved = conn.execute(select(lanying_gift_quota.settings.c.auto_recharge)
                    .where(lanying_gift_quota.settings.c.app_id == app_id)).scalar()
            if saved is not None:
                value = str(bool(saved)).lower()
                if raw != value:
                    redis.hset(preference_key(app_id), 'auto_recharge', value)
                return value
    return raw if raw in ('true', 'false') else None

def has_preference(app_id):
    return preference(app_id) is not None

def auto_recharge(app_id, config):
    raw = preference(app_id)
    if raw in ('true', 'false'):
        return raw == 'true'
    extra = config.get('enable_extra_price', config.get('product_id', 0) > 7001)
    return config.get('product_id', 0) > 7001 and str(extra).lower() in ('true', '1', 'on')

def set_preference(app_id, enabled):
    if not isinstance(enabled, bool):
        raise ValueError('auto_recharge must be boolean')
    table = lanying_gift_quota.settings
    with lanying_gift_quota.engine().begin() as conn:
        lanying_gift_quota._ignore(conn, table, dict(app_id=app_id, auto_recharge=enabled))
        conn.execute(update(table).where(table.c.app_id == app_id).values(auto_recharge=enabled))
    redis = lanying_redis.get_redis_connection()
    redis.hset(preference_key(app_id), 'auto_recharge', str(enabled).lower())
    return {'auto_recharge': enabled}

def remaining(app_id):
    value = lanying_gift_quota.status(app_id, purchased=True)
    return value['remaining'] or 0
