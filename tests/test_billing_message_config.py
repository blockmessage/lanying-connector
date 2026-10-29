import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from lanying_billing_message_config import save, ConfigConflict


def config(**changes):
    value = dict(billing_order_id='a'*32, billing_order_sequence=1, billing_environment='TEST',
                 product_id=7002, message_per_month=3000, extra_price_per_100_msg_when_no_token=99,
                 extra_price_per_100_msg_when_providing_token=25, enable_extra_price=1,
                 start_date='2026-10-29', billing_free_start_date='2026-10-01',
                 billing_period_start_epoch=1793203200, billing_period_end_epoch=1795881600,
                 billing_period_end='2026-11-29', tenant_id=42, preset='Keep this', admin_token='not logged')
    value.update(changes)
    return value


class MessageConfigTest(unittest.TestCase):
    def client(self, old=None):
        client = Mock()
        client.get.return_value = (None, None) if old is None else (json.dumps(old).encode(), SimpleNamespace(mod_revision=8))
        client.transaction.return_value = (True, [])
        return client

    def test_ordinary_legacy_configuration_still_writes(self):
        client = self.client({'product_id': 7001})
        raw = json.dumps({'product_id': 7002, 'custom': True})
        save(client, '/app/config', raw)
        client.transactions.put.assert_called_once_with('/app/config', raw)
        client.transactions.mod.assert_called_once_with('/app/config')

    def test_first_free_takeover_and_exact_replay(self):
        client = self.client({'product_id': 7001, 'admin_token': 'old'})
        value = config()
        save(client, '/app/config', json.dumps(value))
        client.transactions.put.assert_called_once_with('/app/config', json.dumps(value))
        client = self.client(value)
        save(client, '/app/config', json.dumps(value))
        client.transaction.assert_not_called()

    def test_preferences_can_change_but_original_paid_terms_cannot(self):
        old = config()
        save(self.client(old), '/app/config', json.dumps(config(preset='new')))
        for key, value in [('message_per_month', 9999), ('product_id', 7003), ('start_date', '2026-11-01'),
                           ('billing_environment', 'LIVE'), ('billing_order_sequence', 2)]:
            client = self.client(old)
            with self.subTest(key=key), self.assertRaises(ConfigConflict):
                save(client, '/app/config', json.dumps(config(**{key: value})))
            client.transaction.assert_not_called()

    def test_old_order_or_legacy_cannot_override_new_order(self):
        for value in (config(), {'product_id': 7001}):
            client = self.client(config(billing_order_id='b'*32, billing_order_sequence=2))
            with self.assertRaises(ConfigConflict):
                save(client, '/app/config', json.dumps(value))
            client.transaction.assert_not_called()

    def test_environment_boundary_and_new_order(self):
        for old in ({'product_id': 7002}, config(billing_environment='LIVE')):
            client = self.client(old)
            with self.assertRaises(ConfigConflict):
                save(client, '/app/config', json.dumps(config(billing_order_id='b'*32, billing_order_sequence=2)))
            client.transaction.assert_not_called()
        save(self.client(config()), '/app/config', json.dumps(config(
            billing_order_id='b'*32, billing_order_sequence=2, billing_environment='LIVE')))

    def test_conflict_is_visible_and_not_retried_over_new_value(self):
        client = self.client({'product_id': 7001})
        client.transaction.return_value = (False, [])
        with self.assertRaises(ConfigConflict):
            save(client, '/app/config', json.dumps(config()))
        self.assertEqual(1, client.transaction.call_count)

    def test_invalid_billing_fields_never_write(self):
        for changes in ({'message_per_month': -1}, {'enable_extra_price': True}, {'start_date': 'bad'},
                        {'billing_order_sequence': True}, {'tenant_id': 0}):
            client = self.client()
            with self.subTest(changes=changes), self.assertRaises((ValueError, ConfigConflict)):
                save(client, '/app/config', json.dumps(config(**changes)))
            client.transaction.assert_not_called()
