import unittest

import lanying_masked_config


MASKED = lanying_masked_config.MASKED_VALUE


class AiPluginMaskedUpdateTest(unittest.TestCase):
    def test_masked_plugin_values_keep_existing_values(self):
        current = {
            'headers': {
                'Authorization': 'Bearer old-secret',
                'X-Region': {'type': 'value', 'value': 'cn'},
            },
            'auth': {
                'type': 'basic',
                'username': 'old-user',
                'password': 'old-password',
            },
        }
        submitted = {
            'headers': {
                'Authorization': MASKED,
                'X-Region': {'type': 'value', 'value': 'global'},
            },
            'auth': {
                'type': 'basic',
                'username': MASKED,
                'password': 'new-password',
            },
        }

        self.assertEqual(lanying_masked_config.restore_masked_values(
            submitted, current), {
                'headers': {
                    'Authorization': 'Bearer old-secret',
                    'X-Region': {'type': 'value', 'value': 'global'},
                },
                'auth': {
                    'type': 'basic',
                    'username': 'old-user',
                    'password': 'new-password',
                },
            })


    def test_omitted_plugin_map_entry_is_removed(self):
        self.assertEqual(lanying_masked_config.restore_masked_values(
            {'Authorization': MASKED},
            {'Authorization': 'secret', 'X-Removed': 'old'},
        ), {'Authorization': 'secret'})


    def test_masked_new_plugin_value_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'no existing value'):
            lanying_masked_config.restore_masked_values(
                {'X-New': MASKED}, {'Authorization': 'secret'})


    def test_empty_plugin_value_explicitly_clears_value(self):
        self.assertEqual(lanying_masked_config.restore_masked_values(
            {'Authorization': ''}, {'Authorization': 'secret'}), {
                'Authorization': ''
            })

    def test_plugin_configuration_requires_an_object(self):
        for value in [None, [], [['Authorization', 'secret']], 'secret', 1]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, 'must be an object'):
                    lanying_masked_config.restore_masked_config_map(
                        value, {'Authorization': 'old-secret'})


if __name__ == '__main__':
    unittest.main()
