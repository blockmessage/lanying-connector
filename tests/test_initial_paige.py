import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import MagicMock, patch

from redis.exceptions import WatchError

import lanying_chatbot as chatbot


class InitialPaigeTests(unittest.TestCase):
    def test_retry_does_not_publish_before_profile_is_confirmed(self):
        app_id, capsule_id = 'app', '949256'
        receipt = chatbot.initial_paige_key(app_id)
        redis = MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.get.side_effect = lambda key: published[key].encode() if key in published else None
        pipe.hget.return_value = None
        redis.hget.return_value = None
        pending_profile = Event()
        release_profile = Event()
        published = {}

        def commit():
            target, fields = pipe.hmset.call_args.args
            published[target] = fields
            key, agent_id = pipe.set.call_args.args
            published[key] = agent_id

        pipe.execute.side_effect = commit
        redis.get.side_effect = lambda key: published[key].encode() if key in published else None

        def write_profile(*args, **kwargs):
            if not pending_profile.is_set():
                pending_profile.set()
                if not release_profile.wait(5):
                    raise TimeoutError('test profile was not released')
            return {'code': 200, 'data': True}

        source = {
            'name': 'writer', 'desc': '', 'preset': {'messages': []},
            'history_msg_count_max': 10, 'history_msg_count_min': 1, 'history_msg_size_max': 1024,
            'message_per_month_per_user': 300,
        }
        capsule = {'status': 'normal', 'password': '', 'month_price': 0, 'app_id': 'source',
                   'chatbot_id': 'writer', 'preset_protect': 'on'}
        with patch.object(chatbot.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(chatbot.lanying_ai_capsule, 'get_capsule', return_value=capsule), \
                patch.object(chatbot.lanying_ai_capsule, 'generate_capsule_id', return_value='new-capsule'), \
                patch.object(chatbot, 'get_chatbot_with_profile', return_value=source), \
                patch.object(chatbot, 'get_name_chatbot_id', return_value=None), \
                patch.object(chatbot, 'generate_chatbot_id', side_effect=['original-agent', 'contender']), \
                patch.object(chatbot.lanying_config, 'get_lanying_admin_token', return_value='test-token'), \
                patch.object(chatbot.lanying_im_api, 'set_user_profile', side_effect=write_profile) as profile, \
                patch.object(chatbot.lanying_im_api, 'set_user_avatar'), ThreadPoolExecutor(max_workers=1) as pool:
            def import_for(user_id):
                return chatbot.create_chatbot_from_capsule(app_id, capsule_id, '', 'month', 0, user_id, 'link',
                                                           initial_paige=True)

            first = pool.submit(import_for, 123)
            try:
                self.assertTrue(pending_profile.wait(5))
                self.assertFalse(first.done())
                self.assertNotIn(receipt, published)
                pipe.execute.assert_not_called()
                self.assertEqual(import_for(123)['data']['id'], 'original-agent')
                self.assertEqual(published[receipt], 'original-agent')
                fields = published[chatbot.get_chatbot_key(app_id, 'original-agent')]
                self.assertEqual(fields['linked_capsule_id'], capsule_id)
                self.assertEqual(fields['user_id'], 123)
                self.assertEqual(import_for(456)['data']['id'], 'original-agent')
            finally:
                release_profile.set()
            self.assertEqual(first.result(timeout=5)['data']['id'], 'original-agent')
            self.assertEqual([call.args[1] for call in profile.call_args_list], [123, 123])

        pipe.rpush.assert_called_once_with(chatbot.get_chatbot_ids_key(app_id), 'original-agent')
        pipe.execute.assert_called_once()

    def test_initial_profile_reads_etcd_even_when_cache_is_empty(self):
        config = chatbot.lanying_config
        with patch.multiple(config, mode='etcd', prefix='apps.', configs={}), \
                patch.object(config, 'etcd') as etcd, \
                patch.object(chatbot.lanying_im_api.requests, 'post') as post:
            etcd.get.return_value = (b'{"enable":false,"lanying_admin_token":"test-token"}', None)
            post.return_value.json.return_value = {'code': 200, 'data': True}
            self.assertTrue(chatbot.prepare_initial_paige_im_profile('app', 123, 'Intro', 'Paige', '', {}, 'avatar-url'))

        etcd.get.assert_called_once_with('apps.app.lanying_connector')
        self.assertEqual(len(post.call_args_list), 2)
        for call in post.call_args_list:
            self.assertEqual(call.kwargs['headers']['access-token'], 'test-token')
            self.assertEqual(call.kwargs['headers']['user_id'], '123')

    def test_unchanged_profile_ack_requires_matching_existing_values(self):
        with patch.object(chatbot.lanying_config, 'get_lanying_admin_token', return_value='test-token'), \
                patch.object(chatbot.lanying_im_api, 'set_user_profile', return_value={'code': 200, 'data': False}), \
                patch.object(chatbot.lanying_im_api, 'get_user_profile_with_token') as read:
            read.return_value = {'code': 200, 'data': {'nick_name': 'Paige', 'description': 'Intro'}}
            self.assertTrue(chatbot.prepare_initial_paige_im_profile('app', 123, 'Intro', 'Paige', '', {}, ''))
            self.assertFalse(chatbot.prepare_initial_paige_im_profile('app', 123, 'Different', 'Paige', '', {}, ''))

    def test_unconfirmed_profile_does_not_allocate_agent(self):
        with patch.object(chatbot, 'get_user_chatbot_id', return_value=None), \
                patch.object(chatbot, 'get_name_chatbot_id', return_value=None), \
                patch.object(chatbot.lanying_config, 'get_lanying_admin_token', return_value='test-token'), \
                patch.object(chatbot.lanying_im_api, 'set_user_profile', return_value={'code': 503}), \
                patch.object(chatbot, 'generate_chatbot_id') as allocate:
            result = chatbot.create_chatbot(
                'app', 'writer', 'Paige', 'Intro', '', 123, '', {}, 10, 1, 1024, 300, [],
                '', 'capsule', '', 'all', 'off', 'off', '', {}, 'on', initial_capsule_id='949256')
        self.assertEqual(result['result'], 'error')
        allocate.assert_not_called()

    def test_initial_identity_conflict_returns_completed_import(self):
        with patch.object(chatbot, 'get_user_chatbot_id', return_value='winner'), \
                patch.object(chatbot.lanying_redis, 'get_redis_connection') as connect:
            connect.return_value.get.return_value = b'winner'
            result = chatbot.create_chatbot(
                'app', 'writer', 'Paige', 'Intro', '', 123, '', {}, 10, 1, 1024, 300, [],
                '', 'capsule', '', 'all', 'off', 'off', '', {}, 'on', initial_capsule_id='949256')
        self.assertEqual(result, {'result': 'ok', 'data': {'id': 'winner'}})

    def test_conflicting_transaction_returns_the_winning_agent(self):
        redis = MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.get.return_value = None
        pipe.hget.return_value = None
        pipe.execute.side_effect = WatchError()
        redis.get.return_value = b'winner'
        result = chatbot.store_initial_paige(redis, 'app', 'contender', {'user_id': 123, 'name': 'writer'}, '949256')
        self.assertEqual(result, {'result': 'ok', 'data': {'id': 'winner'}})
        redis.get.assert_called_once_with(chatbot.initial_paige_key('app'))


if __name__ == '__main__':
    unittest.main()
