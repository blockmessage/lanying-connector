import logging
import unittest
from unittest import mock

from flask import Flask

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

    def test_format_log_value_redacts_common_api_credentials(self):
        value = {
            'app_id': 'app-1',
            'prompt': 'write an article',
            'github_token': 'github-value',
            'temporary_password': 'password-value',
            'file_sign': 'download-value',
            'max_tokens': 1024,
        }

        formatted = lanying_logging.format_log_value(value)

        self.assertIn('write an article', formatted)
        self.assertIn('"max_tokens":1024', formatted)
        self.assertNotIn('github-value', formatted)
        self.assertNotIn('password-value', formatted)
        self.assertNotIn('download-value', formatted)

    def test_format_log_value_truncates_large_values(self):
        formatted = lanying_logging.format_log_value(
            {'content': 'x' * 1000}, max_chars=300)

        self.assertLess(len(formatted), 360)
        self.assertIn('[truncated', formatted)

    def test_format_log_value_redacts_credentials_in_serialized_json(self):
        formatted = lanying_logging.format_log_value({
            'key': 'lanying_connector',
            'value': '{"access_token":"access-value",'
                     '"lanying_admin_token":"admin-value",'
                     '"name":"example"}',
        })

        self.assertNotIn('access-value', formatted)
        self.assertNotIn('admin-value', formatted)
        self.assertIn('example', formatted)

    def test_http_logging_records_json_request_and_response(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.post('/service/test')
        def api():
            from flask import jsonify, request
            return jsonify({'code': 200, 'data': request.get_json()})

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().post('/service/test', json={
                'app_id': 'app-1',
                'name': 'example',
                'access_token': 'secret-value',
            })

        logs = '\n'.join(captured.output)
        self.assertEqual(200, response.status_code)
        self.assertIn('connector http request', logs)
        self.assertIn('connector http response', logs)
        self.assertIn('"name":"example"', logs)
        self.assertNotIn('secret-value', logs)

    def test_http_logging_ignores_successful_root_health_checks(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.route('/', methods=['GET', 'HEAD'])
        def health():
            return ''

        with mock.patch.object(lanying_logging.logging, 'info') as info, mock.patch.object(
                lanying_logging.logging, 'warning') as warning:
            head_response = app.test_client().head('/')
            get_response = app.test_client().get('/')

        self.assertEqual(200, head_response.status_code)
        self.assertEqual(200, get_response.status_code)
        info.assert_not_called()
        warning.assert_not_called()

    def test_http_logging_keeps_failed_root_head_health_check(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.route('/', methods=['HEAD'])
        def health():
            return '', 503

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().head('/')

        logs = '\n'.join(captured.output)
        self.assertEqual(503, response.status_code)
        self.assertIn('connector http request', logs)
        self.assertIn('connector http response', logs)
        self.assertIn('status:503', logs)

    def test_http_logging_does_not_consume_streaming_response(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.get('/service/stream')
        def stream():
            from flask import Response
            return Response(iter(['first', 'second']), mimetype='text/plain')

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().get('/service/stream')

        self.assertEqual('firstsecond', response.get_data(as_text=True))
        self.assertIn('streaming response', '\n'.join(captured.output))

    def test_http_logging_uses_route_template_for_sensitive_path_parameter(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.post('/wechat/<string:token>/messages')
        def callback(token):
            return {'ok': bool(token)}

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().post('/wechat/path-secret/messages')

        logs = '\n'.join(captured.output)
        self.assertEqual(200, response.status_code)
        self.assertIn('/wechat/<string:token>/messages', logs)
        self.assertNotIn('path-secret', logs)

    def test_http_logging_keeps_non_sensitive_path_parameter(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.get('/service/<string:service>/status')
        def status(service):
            return {'service': service}

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().get('/service/openai/status')

        self.assertEqual(200, response.status_code)
        self.assertIn('/service/openai/status', '\n'.join(captured.output))

    def test_http_logging_does_not_parse_multipart_for_logging(self):
        app = Flask(__name__)
        lanying_logging.register_http_logging(app)

        @app.post('/upload')
        def upload():
            from flask import request
            return {'name': request.form['name']}

        with self.assertLogs(level='INFO') as captured:
            response = app.test_client().post(
                '/upload', data={'name': 'example'},
                content_type='multipart/form-data')

        self.assertEqual({'name': 'example'}, response.get_json())
        self.assertIn('multipart request body', '\n'.join(captured.output))

    def test_init_logging_disables_urllib3_debug_request_urls(self):
        root_logger = logging.getLogger()
        urllib3_logger = logging.getLogger('urllib3.connectionpool')
        root_level = root_logger.level
        urllib3_level = urllib3_logger.level
        self.addCleanup(root_logger.setLevel, root_level)
        self.addCleanup(urllib3_logger.setLevel, urllib3_level)
        with (
            mock.patch.object(lanying_logging.os, 'makedirs'),
            mock.patch.object(
                lanying_logging, 'ConcurrentTimedRotatingFileHandler')
                as file_handler_class,
            mock.patch.object(lanying_logging.logging, 'StreamHandler')
                as stream_handler_class,
            mock.patch.object(root_logger, 'addHandler'),
        ):
            lanying_logging.init_logging()

        file_handler_class.assert_called_once_with(
            filename=mock.ANY,
            when='midnight',
            interval=1,
            backupCount=365,
            use_gzip=True,
            encoding='utf-8')
        file_handler_class.return_value.setLevel.assert_called_once_with(
            logging.DEBUG)
        stream_handler_class.return_value.setLevel.assert_called_once_with(
            logging.DEBUG)
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
