import logging
import unittest
from unittest import mock

import lanying_logging
import lanying_utils


class LoggingRedactionTests(unittest.TestCase):
    def test_url_for_log_drops_query_and_fragment(self):
        sanitized = lanying_utils.url_for_log(
            'https://user:password@example.com/callback'
            '?custom_secret=value#fragment')

        self.assertEqual(sanitized, 'https://example.com/callback')

    def test_invalid_url_for_log_does_not_raise(self):
        self.assertEqual(
            lanying_utils.url_for_log('https://[broken'),
            '[invalid url]')
        self.assertFalse(
            lanying_utils.is_valid_public_url('https://[broken'))

    def test_redacts_nested_credentials_and_url_query(self):
        value = {
            'headers': {
                'Authorization': 'Bearer bearer-value',
                'Cookie': 'session-value',
                'X-API-Key': 'custom-secret',
            },
            'url': (
                'https://example.com/token?client_secret=secret-value'
                '&access_token=access-value&safe=value'),
            'body': {'password': 'password-value', 'prompt': 'hello'},
        }

        redacted = lanying_logging.redact_sensitive_log_value(value)

        self.assertEqual(redacted['headers']['Authorization'], '[REDACTED]')
        self.assertEqual(redacted['headers']['Cookie'], '[REDACTED]')
        self.assertEqual(redacted['headers']['X-API-Key'], '[REDACTED]')
        self.assertNotIn('secret-value', redacted['url'])
        self.assertNotIn('access-value', redacted['url'])
        self.assertIn('safe=value', redacted['url'])
        self.assertEqual(redacted['body']['password'], '[REDACTED]')
        self.assertEqual(redacted['body']['prompt'], '[REDACTED]')

    def test_init_logging_disables_urllib3_debug_request_urls(self):
        root_logger = logging.getLogger()
        urllib3_logger = logging.getLogger('urllib3.connectionpool')
        root_level = root_logger.level
        urllib3_level = urllib3_logger.level
        self.addCleanup(root_logger.setLevel, root_level)
        self.addCleanup(urllib3_logger.setLevel, urllib3_level)
        with (
            mock.patch.object(lanying_logging.os, 'makedirs'),
            mock.patch.object(lanying_logging.logging, 'FileHandler'),
            mock.patch.object(root_logger, 'addHandler'),
        ):
            lanying_logging.init_logging()

        self.assertGreaterEqual(urllib3_logger.level, logging.INFO)

    def test_formatter_redacts_third_party_log_messages(self):
        formatter = lanying_logging.RedactingFormatter('%(message)s')
        record = logging.LogRecord(
            'urllib3.connectionpool', logging.DEBUG, __file__, 1,
            'POST https://example.com?client_secret=%s Authorization: Bearer token',
            ('secret-value',), None)

        formatted = formatter.format(record)

        self.assertNotIn('secret-value', formatted)
        self.assertNotIn('token', formatted)
        self.assertIn('client_secret=[REDACTED]', formatted)
        self.assertIn('Bearer [REDACTED]', formatted)


if __name__ == '__main__':
    unittest.main()
