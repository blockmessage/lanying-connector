import json
import unittest
from unittest import mock

import lanying_im_sender


class _Response:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class LanyingImSenderTests(unittest.TestCase):
    def test_send_message_preserves_existing_im_request(self):
        response = _Response({'msg_ids': [123]})
        with mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_lanying_admin_token', return_value='admin-token'), mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_lanying_api_endpoint', return_value='https://im.example'), mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_message_antispam', return_value='prompt'), mock.patch.object(
                lanying_im_sender.requests, 'post', return_value=response) as post:
            message_id = lanying_im_sender.send_message(
                'app-1', '10', '20', 'hello', {'key': 'value'})

        self.assertEqual(123, message_id)
        post.assert_called_once_with(
            'https://im.example/message/send',
            headers={'app_id': 'app-1', 'access-token': 'admin-token'},
            json={
                'type': 1,
                'from_user_id': '10',
                'targets': ['20'],
                'content_type': 0,
                'content': 'hello',
                'config': json.dumps(
                    {'antispam_prompt': 'prompt'}, ensure_ascii=False),
                'ext': json.dumps({'key': 'value'}, ensure_ascii=False),
            })

    def test_send_message_without_admin_token_does_not_call_im_api(self):
        with mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_lanying_admin_token', return_value=None), mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_lanying_api_endpoint', return_value='https://im.example'), mock.patch.object(
                lanying_im_sender.lanying_config,
                'get_message_antispam', return_value='prompt'), mock.patch.object(
                lanying_im_sender.requests, 'post') as post:
            result = lanying_im_sender.send_message(
                'app-1', '10', '20', 'hello')

        self.assertIsNone(result)
        post.assert_not_called()


if __name__ == '__main__':
    unittest.main()
