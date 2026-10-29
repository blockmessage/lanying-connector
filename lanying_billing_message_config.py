"""Version check for the existing etcd Connector configuration write path.

Billing writes carry an immutable original-order identity. Legacy configurations
remain supported until an App is explicitly owned by a new billing order.
"""
import json
import re


class ConfigConflict(ValueError):
    pass


PROTECTED = ('product_id', 'message_per_month', 'extra_price_per_100_msg_when_no_token',
             'extra_price_per_100_msg_when_providing_token', 'enable_extra_price',
             'start_date', 'tenant_id', 'billing_period_start_epoch', 'billing_period_end_epoch',
             'billing_period_end', 'billing_free_start_date')
IDENTITY = ('billing_order_id', 'billing_order_sequence', 'billing_environment')


def _billing(value):
    if not isinstance(value, dict) or 'billing_order_id' not in value:
        return None
    if (not isinstance(value.get('billing_order_id'), str) or not re.fullmatch(r'[0-9a-f]{32}', value['billing_order_id'])
            or type(value.get('billing_order_sequence')) is not int or value['billing_order_sequence'] <= 0
            or value.get('billing_environment') not in ('TEST', 'LIVE')):
        raise ConfigConflict('invalid billing configuration identity')
    for key in PROTECTED:
        if key not in value:
            raise ConfigConflict('incomplete billing configuration')
    if (type(value['product_id']) is not int or not 7001 <= value['product_id'] < 8000
            or type(value['tenant_id']) is not int or value['tenant_id'] <= 0
            or type(value['enable_extra_price']) is not int or value['enable_extra_price'] not in (0, 1)):
        raise ConfigConflict('invalid message product')
    for key in ('message_per_month', 'extra_price_per_100_msg_when_no_token', 'extra_price_per_100_msg_when_providing_token'):
        if type(value[key]) is not int or not 0 <= value[key] <= 2**63-1:
            raise ConfigConflict('invalid message quota')
    from datetime import date
    if not isinstance(value['start_date'], str):
        raise ConfigConflict('invalid message period')
    date.fromisoformat(value['start_date'])
    date.fromisoformat(value['billing_period_end'])
    date.fromisoformat(value['billing_free_start_date'])
    if (type(value['billing_period_start_epoch']) is not int or type(value['billing_period_end_epoch']) is not int
            or not 0 < value['billing_period_start_epoch'] < value['billing_period_end_epoch']):
        raise ConfigConflict('invalid message billing period')
    return value


def save(client, key, raw):
    try:
        new = json.loads(raw)
    except (ValueError, TypeError) as error:
        raise ConfigConflict('invalid Connector configuration') from error
    if not isinstance(new, dict):
        raise ConfigConflict('invalid Connector configuration')
    encoded, meta = client.get(key)
    old = json.loads(encoded) if encoded is not None else {}
    incoming, previous = _billing(new), _billing(old)
    if previous:
        if not incoming:
            raise ConfigConflict('billing-owned configuration requires its original order')
        if incoming['billing_order_id'] == previous['billing_order_id'] and incoming['billing_order_sequence'] != previous['billing_order_sequence']:
            raise ConfigConflict('original message order sequence differs')
        if incoming['billing_order_sequence'] < previous['billing_order_sequence']:
            raise ConfigConflict('message order has been superseded')
        if incoming['billing_order_sequence'] == previous['billing_order_sequence']:
            if any(incoming.get(k) != previous.get(k) for k in IDENTITY + PROTECTED):
                raise ConfigConflict('original message terms differ')
    if incoming and incoming['billing_environment'] == 'TEST':
        if previous and previous['billing_environment'] != 'TEST':
            raise ConfigConflict('test order cannot replace live message service')
        if not previous and old and old.get('product_id') != 7001:
            raise ConfigConflict('test order cannot replace a paid legacy message service')
    if old == new:
        return
    # No process-local lock or blind PUT: both primary writes and mirrored /config
    # writes use the same etcd revision check. Concurrent changes are visible errors.
    comparison = client.transactions.mod(key) == meta.mod_revision if meta else client.transactions.version(key) == 0
    succeeded, _ = client.transaction(compare=[comparison], success=[client.transactions.put(key, raw)], failure=[])
    if not succeeded:
        raise ConfigConflict('Connector configuration changed; retry original order')
