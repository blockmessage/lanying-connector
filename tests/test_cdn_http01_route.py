import unittest
from unittest import mock

import lanying_cdn


class CdnHttp01RouteTests(unittest.TestCase):
    def test_has_cdn_http01_challenge_route_true(self):
        fake_result = mock.Mock()
        fake_result.to_map.return_value = {
            'body': {
                'domain_configs': {
                    'domain_config': [
                        {
                            'function_name': 'edge_function',
                            'function_args': [
                                {
                                    'arg_name': 'rule',
                                    'arg_value': "if match_re($uri, '^/\\.well-known/acme-challenge') { rewrite(concat('https://connector.lanyingim.com',$uri), 'redirect') }"
                                }
                            ]
                        }
                    ]
                }
            }
        }
        with mock.patch.object(lanying_cdn, 'desc_cdn_config', return_value=fake_result):
            self.assertTrue(lanying_cdn.has_cdn_http01_challenge_route('docs.example.com'))

    def test_has_cdn_http01_challenge_route_false_for_http_only_rule(self):
        fake_result = mock.Mock()
        fake_result.to_map.return_value = {
            'body': {
                'domain_configs': {
                    'domain_config': [
                        {
                            'function_name': 'edge_function',
                            'function_args': [
                                {
                                    'arg_name': 'rule',
                                    'arg_value': "if match_re($uri, '^/\\.well-known/acme-challenge') { rewrite(concat('http://connector.lanyingim.com',$uri), 'redirect') }"
                                }
                            ]
                        }
                    ]
                }
            }
        }
        with mock.patch.object(lanying_cdn, 'desc_cdn_config', return_value=fake_result):
            self.assertFalse(lanying_cdn.has_cdn_http01_challenge_route('docs.example.com'))

    def test_set_cdn_http01_challenge_route_skips_when_exists(self):
        with mock.patch.object(lanying_cdn, 'has_cdn_http01_challenge_route', return_value=True), \
             mock.patch.object(lanying_cdn, 'create_client') as mock_create_client:
            result = lanying_cdn.set_cdn_http01_challenge_route('docs.example.com')
            self.assertEqual({'result': 'ok', 'message': 'already_exists'}, result)
            mock_create_client.assert_not_called()

    def test_set_cdn_http01_challenge_route_creates_when_missing(self):
        fake_response = mock.Mock()
        fake_response.to_map.return_value = {'ok': True}
        fake_client = mock.Mock()
        fake_client.batch_set_cdn_domain_config.return_value = fake_response
        with mock.patch.object(lanying_cdn, 'has_cdn_http01_challenge_route', return_value=False), \
             mock.patch.object(lanying_cdn, 'create_client', return_value=fake_client):
            lanying_cdn.set_cdn_http01_challenge_route('docs.example.com')
            fake_client.batch_set_cdn_domain_config.assert_called_once()


if __name__ == '__main__':
    unittest.main()
