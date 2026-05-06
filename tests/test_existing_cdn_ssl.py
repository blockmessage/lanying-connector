import unittest
from unittest import mock

import lanying_existing_cdn_ssl
from acme import errors as acme_errors


class ExistingCdnSSLTests(unittest.TestCase):
    def test_try_issue_cert_with_retry_reuses_same_order(self):
        with mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'get_acme_client') as mock_get_client, \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'new_csr_comp', return_value=(b'key', b'csr')), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'select_http01_chall', return_value='challb'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'perform_http01') as mock_perform, \
             mock.patch.object(lanying_existing_cdn_ssl.time, 'sleep'):
            client = mock.Mock()
            client.new_order.return_value = 'orderr'
            mock_get_client.return_value = client
            mock_perform.side_effect = [
                acme_errors.ValidationError([]),
                acme_errors.ValidationError([]),
                mock.Mock(fullchain_pem='fullchain')
            ]

            result = lanying_existing_cdn_ssl.try_issue_cert_with_retry('docs.example.com', max_attempts=3, retry_delay_sec=1)

            self.assertEqual('ok', result['result'])
            client.new_order.assert_called_once_with(b'csr')
            self.assertEqual(3, mock_perform.call_count)

    def test_issue_and_bind_ssl_success(self):
        with mock.patch.object(lanying_existing_cdn_ssl, 'precheck_http01_reachable', return_value={'result': 'ok'}), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_http01_challenge_route'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'get_acme_client') as mock_get_client, \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'new_csr_comp', return_value=(b'key', b'csr')), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'select_http01_chall', return_value='challb'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'perform_http01') as mock_perform, \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_domain_cert') as mock_set_cert:
            client = mock.Mock()
            client.new_order.return_value = 'orderr'
            mock_get_client.return_value = client
            mock_perform.return_value = mock.Mock(fullchain_pem='fullchain')

            result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('docs.example.com')

            self.assertEqual('ok', result['result'])
            mock_set_cert.assert_called_once_with('docs.example.com', 'fullchain', 'key')

    def test_issue_and_bind_ssl_domain_invalid(self):
        result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('invalid_domain')
        self.assertEqual({'result': 'error', 'message': 'domain_name_invalid'}, result)

    def test_issue_and_bind_ssl_route_config_failed(self):
        with mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_http01_challenge_route', side_effect=Exception('x')):
            result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('docs.example.com')
            self.assertEqual({'result': 'error', 'message': 'challenge_route_config_failed'}, result)

    def test_issue_and_bind_ssl_http_not_ready(self):
        with mock.patch.object(lanying_existing_cdn_ssl, 'precheck_http01_reachable', return_value={'result': 'error', 'message': 'http_not_ready'}), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_http01_challenge_route'):
            result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('docs.example.com')
            self.assertEqual({'result': 'error', 'message': 'http_not_ready'}, result)

    def test_issue_and_bind_ssl_cert_not_ready(self):
        with mock.patch.object(lanying_existing_cdn_ssl, 'precheck_http01_reachable', return_value={'result': 'ok'}), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_http01_challenge_route'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'get_acme_client', side_effect=Exception('x')):
            result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('docs.example.com')
            self.assertEqual({'result': 'error', 'message': 'cert_not_ready', 'reason': 'x'}, result)

    def test_issue_and_bind_ssl_cert_config_not_ready(self):
        with mock.patch.object(lanying_existing_cdn_ssl, 'precheck_http01_reachable', return_value={'result': 'ok'}), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_http01_challenge_route'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'get_acme_client') as mock_get_client, \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'new_csr_comp', return_value=(b'key', b'csr')), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'select_http01_chall', return_value='challb'), \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cert, 'perform_http01') as mock_perform, \
             mock.patch.object(lanying_existing_cdn_ssl.lanying_cdn, 'set_cdn_domain_cert', side_effect=Exception('x')):
            client = mock.Mock()
            client.new_order.return_value = 'orderr'
            mock_get_client.return_value = client
            mock_perform.return_value = mock.Mock(fullchain_pem='fullchain')

            result = lanying_existing_cdn_ssl.issue_and_bind_ssl_for_existing_cdn('docs.example.com')
            self.assertEqual({'result': 'error', 'message': 'cert_config_not_ready'}, result)


if __name__ == '__main__':
    unittest.main()
