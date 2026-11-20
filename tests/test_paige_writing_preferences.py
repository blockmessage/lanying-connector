import json
import unittest
from unittest.mock import patch, Mock

import lanying_paige as paige
import lanying_seenical_writing_preferences as preferences


class PaigeTests(unittest.TestCase):
    def test_identity_uses_the_builtin_capsule_from_butler(self):
        self.assertFalse(paige.is_writer({'name': 'grow-ai-text-generator', 'linked_capsule_id': 'another'}))
        for key in ('capsule_id', 'linked_capsule_id', 'linked_publish_capsule_id'):
            self.assertTrue(paige.is_writer({key: '949256', 'nickname': 'Custom'}))
        self.assertTrue(paige.is_official_capsule(949256))


class WritingPreferencesTests(unittest.TestCase):
    def setUp(self):
        self.actor = {'im_user_id': '43'}
        self.data = {
            'seenical_session_id': 'group:123',
            'current_prompt': 'Use a neutral voice.\nAdd a checklist.',
            'user_messages': ['Write a test article about the Widget 2000. It costs $900.',
                              '以后不要检查清单，采用决策对照表。'],
        }
        self.response = {'result': 'ok', 'reply': json.dumps({'edits': [
            {'source': 1, 'quote': '不要检查清单，采用决策对照表',
             'line': 1, 'instruction': '采用决策对照表。'},
        ]})}

    @patch.object(preferences.lanying_ai_capsule, 'get_capsule')
    @patch.object(preferences, '_complete')
    @patch.object(preferences.lanying_chatbot, 'get_chatbot')
    @patch.object(preferences.lanying_seenical_history, '_authorize')
    def test_only_verified_writing_delta_returned(self, authorize, get_chatbot, complete, get_capsule):
        authorize.return_value = {'chatbot_id': 'agent'}
        get_capsule.return_value = {'app_id': 'publisher', 'chatbot_id': 'writer'}
        protected = 'protected preset content\n1. Include a short summary. Use plain words.'
        get_chatbot.side_effect = [
            {'linked_capsule_id': '949256'},
            {'preset': {'messages': [{'role': 'system', 'content': protected}]}},
        ]
        self.data['user_messages'].append('Please include a short summary in each article.')
        output = json.loads(self.response['reply'])
        output['edits'].append({
            'source': 2, 'quote': 'include a short summary in each article',
            'line': None, 'instruction': 'Include a short summary.',
        })
        self.response['reply'] = json.dumps(output)
        complete.return_value = self.response
        result = preferences.handle('app', self.actor, self.data)
        authorize.assert_called_once_with('app', self.actor, 'group:123')
        self.assertEqual([('app', 'agent'), ('publisher', 'writer')], [call.args for call in get_chatbot.call_args_list])
        get_capsule.assert_called_once_with('949256')
        request = complete.call_args.args[-1]
        self.assertNotIn('protected preset content', str(request))
        self.assertEqual({'current', 'user_messages'}, set(json.loads(request[1]['content'])))
        self.assertEqual(result['data'], {'changed': True, 'prompt': 'Use a neutral voice.\n采用决策对照表。'})

    def test_mixed_instructions_keep_only_the_new_sentences(self):
        writer = {'capsule_id': '949256', 'preset': {'messages': [
            {'role': 'system', 'content': '一、控制句子长度。每节列举一个示例；保持口吻平实。'},
        ]}}
        rules = preferences._inherited_writer_rules(writer)
        user = '保持口吻平实，结束时留一个供读者自检的问题。'
        output = json.dumps({'edits': [{
            'source': 0, 'quote': user, 'line': None,
            'instruction': '保持口吻平实。结束时留一个供读者自检的问题。',
        }]})
        self.assertEqual('结束时留一个供读者自检的问题。', preferences._merge_reply(output, '', [user], rules))

    def test_returning_to_capsule_default_removes_the_custom_override(self):
        writer = {'capsule_id': '949256', 'preset': {'messages': [
            {'role': 'system', 'content': '2、采用决策对照表。'},
        ]}}
        rules = preferences._inherited_writer_rules(writer)
        prompt = preferences._merge_reply(self.response['reply'], self.data['current_prompt'],
                                          self.data['user_messages'], rules)
        self.assertEqual('Use a neutral voice.', prompt)

    @patch.object(preferences, '_complete')
    @patch.object(preferences.lanying_seenical_history, '_authorize', side_effect=ValueError)
    def test_unowned_conversation_never_calls_model(self, authorize, complete):
        self.assertEqual(preferences.handle('app', self.actor, self.data)['result'], 'error')
        complete.assert_not_called()

    def test_no_delta_and_unverified_output(self):
        current = self.data['current_prompt']
        messages = self.data['user_messages']
        self.assertEqual(preferences._merge_reply('{"edits":[]}', current, messages), current)
        output = json.loads(self.response['reply'])
        output['edits'][0]['quote'] = 'a protected prompt the user did not provide'
        with self.assertRaises(ValueError):
            preferences._merge_reply(json.dumps(output), current, messages)

    def test_reference_only_resolves_user_confirmation(self):
        message = {'text': '以后选第二种写法', 'reference': '2. 用表格开头'}
        self.assertEqual(('', [message]), preferences._input({'user_messages': [message]}))
        def edit(quote):
            return json.dumps({'edits': [
                {'source': 0, 'quote': quote, 'line': None, 'instruction': '用表格开头'},
            ]})
        self.assertEqual('用表格开头', preferences._merge_reply(edit(message['text']), '', [message]))
        with self.assertRaises(ValueError):
            preferences._merge_reply(edit(message['reference']), '', [message])

    def test_completion_is_bounded_and_preserves_capsule_accounting(self):
        completion = {'result': 'ok', 'reply': '{"edits":[]}'}
        ai = Mock()
        ai.check_message_limit.return_value = {'result': 'ok'}
        ai.check_message_per_month_per_user.return_value = {'result': 'ok'}
        ai.get_preset_auth_info.return_value = {'api_key': 'test-key'}
        ai.chat_or_force_function_call.return_value = completion
        chatbot = {'chatbot_id': 'agent', 'preset': {'vendor': 'openai', 'model': 'writer'},
                   'linked_publish_capsule_id': 'official', 'linked_capsule_id': 'original'}
        with patch('services.openai_service', ai, create=True), \
             patch.object(preferences.lanying_config, 'get_lanying_connector', return_value={'product_id': 1}), \
             patch.object(preferences.lanying_vendor, 'get_chat_model_config', return_value={'token_limit': 8192}), \
             patch.object(preferences.lanying_vendor, 'prepare_chat', return_value={}) as prepare:
            self.assertEqual(completion, preferences._complete('app', self.actor, chatbot, []))
        auth_info = prepare.call_args.args[2]
        self.assertEqual(45, auth_info['request_timeout_seconds'])
        self.assertIs(auth_info['single_attempt'], True)
        config = ai.add_message_statistic.call_args.args[1]
        self.assertEqual('official', config['linked_publish_capsule_id'])
        self.assertEqual('original', config['linked_capsule_id'])
