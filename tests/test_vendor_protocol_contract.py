import importlib
import json
import os
import sys
import types
import unittest
from unittest import mock


def _install_fake_tiktoken_if_needed():
    if 'tiktoken' in sys.modules:
        return
    mod = types.ModuleType('tiktoken')

    class _Encoding:
        def encode(self, text, disallowed_special=()):
            return list(str(text).encode('utf-8'))

    def encoding_for_model(_model):
        return _Encoding()

    def get_encoding(_name):
        return _Encoding()

    mod.encoding_for_model = encoding_for_model
    mod.get_encoding = get_encoding
    sys.modules['tiktoken'] = mod


def _install_fake_requests_if_needed():
    if 'requests' in sys.modules:
        return
    mod = types.ModuleType('requests')

    def _not_implemented(*args, **kwargs):
        raise RuntimeError('fake requests module: please mock requests.request/post in tests')

    mod.request = _not_implemented
    mod.post = _not_implemented
    sys.modules['requests'] = mod


def _install_fake_redis_if_needed():
    if 'redis' in sys.modules:
        return
    mod = types.ModuleType('redis')

    class StrictRedis:
        pass

    class ConnectionPool:
        @classmethod
        def from_url(cls, _url):
            return cls()

    mod.StrictRedis = StrictRedis
    mod.ConnectionPool = ConnectionPool
    sys.modules['redis'] = mod


def _install_fake_anthropic_if_needed():
    if 'anthropic' in sys.modules and 'anthropic.types' in sys.modules:
        return

    anthropic_mod = types.ModuleType('anthropic')
    types_mod = types.ModuleType('anthropic.types')

    class Stream:
        pass

    class Message:
        pass

    class TextBlock:
        pass

    class ToolUseBlock:
        pass

    class RawMessageStartEvent:
        pass

    class RawContentBlockDeltaEvent:
        pass

    class RawContentBlockStartEvent:
        pass

    class RawMessageDeltaEvent:
        pass

    class RawContentBlockStopEvent:
        pass

    class Anthropic:
        def __init__(self, *args, **kwargs):
            self.messages = types.SimpleNamespace(create=lambda **k: None)

    class AnthropicBedrock:
        def __init__(self, *args, **kwargs):
            self.messages = types.SimpleNamespace(create=lambda **k: None)

    anthropic_mod.Stream = Stream
    anthropic_mod.Anthropic = Anthropic
    anthropic_mod.AnthropicBedrock = AnthropicBedrock

    types_mod.RawMessageStartEvent = RawMessageStartEvent
    types_mod.RawContentBlockDeltaEvent = RawContentBlockDeltaEvent
    types_mod.RawContentBlockStartEvent = RawContentBlockStartEvent
    types_mod.RawMessageDeltaEvent = RawMessageDeltaEvent
    types_mod.RawContentBlockStopEvent = RawContentBlockStopEvent
    types_mod.Message = Message
    types_mod.TextBlock = TextBlock
    types_mod.ToolUseBlock = ToolUseBlock

    sys.modules['anthropic'] = anthropic_mod
    sys.modules['anthropic.types'] = types_mod


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self):
        return self._payload


class VendorProtocolContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_fake_tiktoken_if_needed()
        _install_fake_requests_if_needed()
        _install_fake_redis_if_needed()
        _install_fake_anthropic_if_needed()

    def test_baidu_chat_returns_tool_calls_contract(self):
        m = importlib.import_module('lanying_vendor_baidu')

        payload = {
            'result': '天气晴',
            'usage': {'completion_tokens': 2, 'prompt_tokens': 3, 'total_tokens': 5},
            'function_call': {'name': 'get_weather', 'arguments': '{"city":"Shanghai"}'},
            'finish_reason': 'function_call'
        }

        with mock.patch.object(m, 'get_access_token', return_value='token'), \
             mock.patch.object(m.requests, 'request', return_value=_FakeResponse(200, payload)):
            out = m.chat({'api_key': 'k', 'secret_key': 's'}, {'messages': [], 'stream': False}, {'url': 'http://dummy'})

        self.assertEqual(out['result'], 'ok')
        self.assertIn('tool_calls', out)
        self.assertTrue(len(out['tool_calls']) > 0)
        self.assertEqual(out['tool_calls'][0]['function']['name'], 'get_weather')

    def test_minimax_chat_returns_tool_calls_contract(self):
        m = importlib.import_module('lanying_vendor_minimax')

        payload = {
            'base_resp': {'status_code': 0},
            'reply': '',
            'function_call': {'name': 'get_weather', 'arguments': '{"city":"Shanghai"}', 'id': 'call_1'},
            'usage': {'completion_tokens': 2, 'prompt_tokens': 3, 'total_tokens': 5},
            'choices': [{'finish_reason': 'function_call'}]
        }

        with mock.patch.object(m.requests, 'request', return_value=_FakeResponse(200, payload)):
            out = m.chat({'api_key': 'k', 'api_group_id': 'g', 'bot_name': 'AI助手', 'user_name': '用户'}, {'model': 'abab6-chat', 'messages': [], 'stream': False}, {'function_call': True})

        self.assertEqual(out['result'], 'ok')
        self.assertIn('tool_calls', out)
        self.assertTrue(len(out['tool_calls']) > 0)
        self.assertEqual(out['tool_calls'][0]['function']['name'], 'get_weather')

    def test_azure_chat_preserves_tool_calls_contract(self):
        m = importlib.import_module('lanying_vendor_azure')

        payload = {
            'choices': [{
                'message': {
                    'content': '',
                    'tool_calls': [{
                        'id': 'call_2',
                        'type': 'function',
                        'function': {'name': 'lookup', 'arguments': '{}'}
                    }]
                },
                'finish_reason': 'tool_calls'
            }],
            'usage': {'completion_tokens': 2, 'prompt_tokens': 3, 'total_tokens': 5}
        }

        with mock.patch.object(m.requests, 'request', return_value=_FakeResponse(200, payload)):
            out = m.chat({'api_key': 'k'}, {'model': 'gpt-4', 'messages': [], 'stream': False}, {'api_type': 'openai'})

        self.assertEqual(out['result'], 'ok')
        self.assertIn('tool_calls', out)
        self.assertEqual(out['tool_calls'][0]['function']['name'], 'lookup')

    def test_claude_chat_returns_tool_calls_contract(self):
        m = importlib.import_module('lanying_vendor_claude')

        class FakeUsage:
            input_tokens = 3
            output_tokens = 5

        class FakeMessage(m.Message):
            pass

        class FakeTextBlock(m.TextBlock):
            def __init__(self, text):
                self.text = text

        class FakeToolUseBlock(m.ToolUseBlock):
            def __init__(self, name, input_value, block_id):
                self.name = name
                self.input = input_value
                self.id = block_id

        fake_message = FakeMessage()
        fake_message.usage = FakeUsage()
        fake_message.content = [FakeTextBlock('ok'), FakeToolUseBlock('lookup', {'q': 'x'}, 'call_3')]
        fake_message.stop_reason = 'tool_use'

        class FakeClient:
            def __init__(self):
                self.messages = types.SimpleNamespace(create=lambda **k: fake_message)

        with mock.patch.object(m.anthropic, 'Anthropic', return_value=FakeClient()):
            out = m.chat({'api_key': 'k'}, {'model': 'claude-3-5-haiku-20241022', 'messages': [], 'stream': False}, {'function_call': True})

        self.assertEqual(out['result'], 'ok')
        self.assertIn('tool_calls', out)
        self.assertEqual(out['tool_calls'][0]['function']['name'], 'lookup')

    def test_claude_format_preset_accepts_tools_and_tool_choice(self):
        m = importlib.import_module('lanying_vendor_claude')
        preset = {
            'model': 'claude-3-5-haiku-20241022',
            'messages': [{'role': 'user', 'content': 'hello'}],
            'tools': [
                {
                    'type': 'function',
                    'function': {
                        'name': 'lookup',
                        'description': 'lookup info',
                        'parameters': {'type': 'object'}
                    }
                }
            ],
            'tool_choice': {'type': 'function', 'function': {'name': 'lookup'}}
        }
        out = m.format_preset(preset, {'function_call': True})
        self.assertIn('tools', out)
        self.assertEqual(out['tools'][0]['name'], 'lookup')
        self.assertEqual(out.get('tool_choice', {}).get('type'), 'tool')
        self.assertEqual(out.get('tool_choice', {}).get('name'), 'lookup')

    def test_aws_chat_returns_tool_calls_contract(self):
        m = importlib.import_module('lanying_vendor_aws')

        class FakeUsage:
            input_tokens = 3
            output_tokens = 5

        class FakeMessage(m.Message):
            pass

        class FakeTextBlock(m.TextBlock):
            def __init__(self, text):
                self.text = text

        class FakeToolUseBlock(m.ToolUseBlock):
            def __init__(self, name, input_value, block_id):
                self.name = name
                self.input = input_value
                self.id = block_id

        fake_message = FakeMessage()
        fake_message.usage = FakeUsage()
        fake_message.content = [FakeTextBlock('ok'), FakeToolUseBlock('lookup', {'q': 'x'}, 'call_4')]
        fake_message.stop_reason = 'tool_use'

        class FakeClient:
            def __init__(self):
                self.messages = types.SimpleNamespace(create=lambda **k: fake_message)

        with mock.patch.object(m, 'AnthropicBedrock', return_value=FakeClient()):
            out = m.chat({'aws_access_key': 'k', 'aws_secret_key': 's', 'region': 'us-west-2'}, {'model': 'anthropic.claude-3-5-sonnet-20241022-v2:0', 'messages': [], 'stream': False}, {'function_call': True})

        self.assertEqual(out['result'], 'ok')
        self.assertIn('tool_calls', out)
        self.assertEqual(out['tool_calls'][0]['function']['name'], 'lookup')

    def test_aws_format_preset_accepts_tools_and_tool_choice(self):
        m = importlib.import_module('lanying_vendor_aws')
        preset = {
            'model': 'anthropic.claude-3-5-sonnet-20241022-v2:0',
            'messages': [{'role': 'user', 'content': 'hello'}],
            'tools': [
                {
                    'type': 'function',
                    'function': {
                        'name': 'lookup',
                        'description': 'lookup info',
                        'parameters': {'type': 'object'}
                    }
                }
            ],
            'tool_choice': {'type': 'function', 'function': {'name': 'lookup'}}
        }
        out = m.format_preset(preset, {'function_call': True})
        self.assertIn('tools', out)
        self.assertEqual(out['tools'][0]['name'], 'lookup')
        self.assertEqual(out.get('tool_choice', {}).get('type'), 'tool')
        self.assertEqual(out.get('tool_choice', {}).get('name'), 'lookup')


if __name__ == '__main__':
    unittest.main()
