import importlib.util
import pathlib
import sys
import types
import unittest


def _load_sync_validation_module():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw_sync_validation.py"
    module_name = "lanying_openclaw_sync_validation_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
        "lanying_config": types.SimpleNamespace(),
        "lanying_im_api": types.SimpleNamespace(),
        "lanying_openclaw": types.SimpleNamespace(
            get_session_last_message_time=lambda *args, **kwargs: 0,
            get_session_mapping_by_session=lambda *args, **kwargs: None,
            list_session_mappings_for_node=lambda *args, **kwargs: [],
        ),
    }
    original_modules = {}
    for name, stub in stub_modules.items():
        original_modules[name] = sys.modules.get(name)
        sys.modules[name] = stub
    try:
        spec.loader.exec_module(module)
    finally:
        for name, original in original_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
    return module


v = _load_sync_validation_module()


class OpenClawSyncValidationTests(unittest.TestCase):
    def test_normalize_scenarios_includes_ai_dynamic_aliases(self):
        result = v.normalize_scenarios(scenario='ai_dynamic')
        self.assertEqual(result['invalid_names'], [])
        self.assertEqual(result['requested_scenarios'], [
            'group_openclaw_ai_dynamic',
            'group_chatbot_ai_dynamic',
            'direct_openclaw_ai_dynamic',
            'direct_chatbot_ai_dynamic',
            'group_openclaw_subagent_ai_dynamic',
            'group_chatbot_subagent_ai_dynamic',
            'direct_openclaw_subagent_ai_dynamic',
            'direct_chatbot_subagent_ai_dynamic',
        ])

    def test_build_scenario_definition_marks_ai_dynamic(self):
        runtime = {
            'sender_user_id': 'sender-user',
            'openclaw_user_id': 'openclaw-user',
            'chatbot_user_id': 'chatbot-user',
            'group_openclaw_id': 'group-openclaw',
            'group_chatbot_id': 'group-chatbot',
        }
        basic = v.build_scenario_definition(runtime, 'direct_openclaw_ai_dynamic')
        subagent = v.build_scenario_definition(runtime, 'group_chatbot_subagent_ai_dynamic')

        self.assertTrue(basic['expect_ai_dynamic'])
        self.assertFalse(bool(basic.get('expect_root_and_sub_sessions')))
        self.assertTrue(subagent['expect_ai_dynamic'])
        self.assertTrue(subagent['expect_root_and_sub_sessions'])

    def test_build_trigger_text_for_ai_dynamic_basic_requires_tool_before_reply(self):
        text = v.build_trigger_text({
            'name': 'direct_openclaw_ai_dynamic',
            'expect_ai_dynamic': True,
        }, 123456)
        self.assertIn('必须先调用至少一个可用工具', text)
        self.assertIn('pwd', text)
        self.assertIn('SYNC_OK_direct_openclaw_ai_dynamic_123456', text)

    def test_find_ai_dynamic_messages_filters_by_request_msg_id(self):
        messages = [
            {
                'msg_id': '1',
                'timestamp': 1000,
                'content': 'Tool output\n...',
                'ext': {
                    'ai': {
                        'is_debug_msg': True,
                        'stream': True,
                        'request_msg_id': 'request-1',
                        'finish': False,
                    },
                },
            },
            {
                'msg_id': '2',
                'timestamp': 1001,
                'content': '[蓝莺AI][10:00:00.000] 处理完成',
                'ext': {
                    'ai': {
                        'is_debug_msg': True,
                        'stream': True,
                        'request_msg_id': 'request-2',
                        'finish': True,
                    },
                },
            },
            {
                'msg_id': '3',
                'timestamp': 1002,
                'content': 'final visible reply',
                'ext': {
                    'ai': {
                        'is_debug_msg': False,
                    },
                },
            },
        ]
        matched = v.find_ai_dynamic_messages(messages, 999, 'request-1')
        self.assertEqual([item['msg_id'] for item in matched], ['1'])

    def test_merge_message_snapshots_keeps_latest_version_for_same_msg_id(self):
        primary = [{
            'msg_id': 'debug-1',
            'timestamp': 1000,
            'content': 'Tool output\n...',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'request_msg_id': 'request-1',
                    'finish': False,
                },
            },
        }]
        secondary = [{
            'msg_id': 'debug-1',
            'timestamp': 1005,
            'content': 'Tool output\n...\n\n[蓝莺AI][10:00:00.000] 处理完成',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'request_msg_id': 'request-1',
                    'finish': True,
                },
            },
        }]

        merged = v.merge_message_snapshots(primary, secondary)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['content'], 'Tool output\n...\n\n[蓝莺AI][10:00:00.000] 处理完成')
        self.assertTrue(v.is_ai_dynamic_finish_message(merged[0]))

    def test_find_replies_excludes_ai_dynamic_message(self):
        messages = [
            {
                'msg_id': 'debug-1',
                'timestamp': 1000,
                'from_user_id': 'openclaw-user',
                'content': 'Tool output\n...\n\n[蓝莺AI][10:00:00.000] 处理完成',
                'ext': {
                    'ai': {
                        'is_debug_msg': True,
                        'stream': True,
                        'request_msg_id': 'request-1',
                        'finish': False,
                    },
                    'openclaw': {
                        'type': 'session_sync_delivery',
                        'request_msg_id': 'request-1',
                    },
                },
            },
            {
                'msg_id': 'final-1',
                'timestamp': 1001,
                'from_user_id': 'openclaw-user',
                'content': 'SYNC_OK_xxx',
                'ext': {
                    'ai': {
                        'ai_generate': False,
                        'role': 'ai',
                    },
                    'openclaw': {
                        'type': 'session_sync_delivery',
                        'request_msg_id': 'request-1',
                    },
                },
            },
        ]
        matched = v.find_replies(messages, 'openclaw-user', 999, 'request-1')
        self.assertEqual([item['msg_id'] for item in matched], ['final-1'])

    def test_find_duplicate_visible_replies_by_content_excludes_ai_dynamic_message(self):
        primary_reply = {
            'msg_id': 'final-1',
            'timestamp': 1001,
            'from_user_id': 'openclaw-user',
            'content': 'SYNC_OK_xxx',
            'ext': {
                'openclaw': {
                    'type': 'session_sync_delivery',
                    'request_msg_id': 'request-1',
                },
            },
        }
        messages = [
            {
                'msg_id': 'debug-1',
                'timestamp': 1000,
                'from_user_id': 'openclaw-user',
                'content': 'SYNC_OK_xxx',
                'ext': {
                    'ai': {
                        'is_debug_msg': True,
                        'stream': True,
                        'request_msg_id': 'request-1',
                    },
                },
            },
            primary_reply,
        ]
        matched = v.find_duplicate_visible_replies_by_content(messages, primary_reply, 'openclaw-user', 999)
        self.assertEqual([item['msg_id'] for item in matched], ['final-1'])

    def test_ai_dynamic_message_has_intermediate_content_recognizes_expected_hints(self):
        self.assertTrue(v.ai_dynamic_message_has_intermediate_content({
            'content': 'Tool input\n```json\n{"command":"pwd"}\n```',
        }))
        self.assertTrue(v.ai_dynamic_message_has_intermediate_content({
            'content': '[蓝莺AI][10:00:00.000] 处理完成',
        }))
        self.assertFalse(v.ai_dynamic_message_has_intermediate_content({
            'content': 'plain final answer only',
        }))

    def test_is_ai_dynamic_finish_message_accepts_final_snapshot_content(self):
        self.assertTrue(v.is_ai_dynamic_finish_message({
            'content': 'Tool output\n...\n\n[蓝莺AI][10:00:00.000] 处理完成',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'finish': True,
                },
            },
        }))
        self.assertTrue(v.is_ai_dynamic_finish_message({
            'content': 'Tool output\n...\n\n[蓝莺AI][10:00:00.000] 处理完成',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'finish': False,
                },
            },
        }))
        self.assertFalse(v.is_ai_dynamic_finish_message({
            'content': 'Tool output only',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'finish': False,
                },
            },
        }))

    def test_is_ai_dynamic_history_snapshot_only_ok_accepts_single_intermediate_snapshot(self):
        messages = [{
            'msg_id': 'debug-1',
            'timestamp': 1000,
            'content': 'Tool output\n...\n\nYield\n...',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'stream': True,
                    'finish': False,
                },
            },
        }]
        self.assertTrue(v.is_ai_dynamic_history_snapshot_only_ok(messages, True))

    def test_is_ai_dynamic_history_snapshot_only_ok_rejects_non_reply_or_non_intermediate(self):
        non_intermediate_messages = [{
            'msg_id': 'debug-1',
            'timestamp': 1000,
            'content': 'plain final answer only',
            'ext': {
                'ai': {
                    'is_debug_msg': True,
                    'stream': True,
                    'finish': False,
                },
            },
        }]
        self.assertFalse(v.is_ai_dynamic_history_snapshot_only_ok(non_intermediate_messages, True))
        self.assertFalse(v.is_ai_dynamic_history_snapshot_only_ok(non_intermediate_messages, False))


if __name__ == '__main__':
    unittest.main()
