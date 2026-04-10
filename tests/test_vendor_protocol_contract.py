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


def _install_fake_zhipuai_if_needed():
    if 'zhipuai' in sys.modules and 'zhipuai.core._errors' in sys.modules:
        return

    zhipuai_mod = types.ModuleType('zhipuai')
    core_mod = types.ModuleType('zhipuai.core')
    errors_mod = types.ModuleType('zhipuai.core._errors')

    class ZhipuAI:
        def __init__(self, *args, **kwargs):
            self.chat = types.SimpleNamespace(completions=types.SimpleNamespace(create=lambda **k: None))

    zhipuai_mod.ZhipuAI = ZhipuAI
    core_mod._errors = errors_mod

    sys.modules['zhipuai'] = zhipuai_mod
    sys.modules['zhipuai.core'] = core_mod
    sys.modules['zhipuai.core._errors'] = errors_mod


def _install_fake_etcd3_if_needed():
    if 'etcd3' in sys.modules:
        return

    etcd3_mod = types.ModuleType('etcd3')

    class PutEvent:
        pass

    class DeleteEvent:
        pass

    class _Events:
        pass

    _Events.PutEvent = PutEvent
    _Events.DeleteEvent = DeleteEvent

    class _FakeClient:
        def get_prefix(self, _prefix):
            return []

        def add_watch_prefix_callback(self, _prefix, _callback):
            return None

    def client(*args, **kwargs):
        return _FakeClient()

    etcd3_mod.client = client
    etcd3_mod.events = _Events
    sys.modules['etcd3'] = etcd3_mod


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
        _install_fake_zhipuai_if_needed()
        _install_fake_etcd3_if_needed()

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

    def test_aliyun_format_preset_keeps_stream_with_tools(self):
        m = importlib.import_module('lanying_vendor_aliyun')
        preset = {
            'model': 'qwen-plus',
            'stream': True,
            'messages': [{'role': 'user', 'content': 'hello'}],
            'tools': [
                {
                    'type': 'function',
                    'function': {
                        'name': 'exec',
                        'description': 'run command',
                        'parameters': {'type': 'object'}
                    }
                }
            ],
            'tool_choice': {'type': 'function', 'function': {'name': 'exec'}}
        }
        out = m.format_preset(preset)
        self.assertTrue(out.get('stream', False))
        self.assertEqual(out.get('stream_options', {}).get('include_usage'), True)
        self.assertIn('tools', out)

    def test_aliyun_model_configs_include_current_common_qwen_models(self):
        m = importlib.import_module('lanying_vendor_aliyun')
        models = {item.get('model'): item for item in m.model_configs()}

        self.assertIn('qwen3.5-flash', models)
        self.assertIn('qwen3.6-plus', models)
        self.assertIn('qwen3-max', models)
        self.assertIn('qwen-flash', models)
        self.assertIn('qwen-plus', models)
        self.assertIn('qwen-max', models)
        self.assertIn('qwen-plus-latest', models)
        self.assertIn('qwen-max-latest', models)
        self.assertNotIn('qwen-turbo', models)
        self.assertNotIn('qwen-max-longcontext', models)
        self.assertEqual(models['qwen3.5-flash'].get('is_default'), True)
        self.assertEqual(models['qwen3.5-flash'].get('quota'), 0.8)
        self.assertEqual(models['qwen3.5-flash'].get('currency'), 'CNY')
        self.assertEqual(models['qwen3.5-flash'].get('input_price'), 0.0012)
        self.assertEqual(models['qwen3.5-flash'].get('output_price'), 0.012)
        self.assertEqual(models['qwen3.6-plus'].get('reasoning'), True)
        self.assertEqual(models['qwen3.6-plus'].get('quota'), 2.89)
        self.assertEqual(models['qwen3.6-plus'].get('input_price'), 0.008)
        self.assertEqual(models['qwen3.6-plus'].get('output_price'), 0.048)
        self.assertEqual(models['qwen3-max'].get('quota'), 2.04)
        self.assertEqual(models['qwen3-max'].get('input_price'), 0.007)
        self.assertEqual(models['qwen3-max'].get('output_price'), 0.028)
        self.assertEqual(models['qwen3-max'].get('function_call'), True)
        self.assertEqual(models['qwen-plus'].get('quota'), 3.13)
        self.assertEqual(models['qwen-plus'].get('output_price'), 0.064)
        self.assertEqual(models['qwen-max'].get('quota'), 0.84)
        self.assertEqual(models['qwen-max-latest'].get('function_call'), True)

    def test_openai_format_preset_keeps_tools_for_modern_reasoning_models(self):
        m = importlib.import_module('lanying_vendor_openai')
        preset = {
            'model': 'o4-mini',
            'stream': True,
            'max_tokens': 4096,
            'messages': [{'role': 'system', 'content': 'You are helpful'}, {'role': 'user', 'content': 'hello'}],
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
        out = m.format_preset(preset, {'reasoning': True})
        self.assertEqual(out.get('max_completion_tokens'), 4096)
        self.assertNotIn('max_tokens', out)
        self.assertTrue(out.get('stream', False))
        self.assertEqual(out.get('stream_options', {}).get('include_usage'), True)
        self.assertEqual(out.get('messages', [])[0].get('role'), 'developer')
        self.assertEqual(out.get('tool_choice', {}).get('function', {}).get('name'), 'lookup')

    def test_openai_format_preset_updates_o1_to_latest_contract(self):
        m = importlib.import_module('lanying_vendor_openai')
        preset = {
            'model': 'o1',
            'stream': True,
            'max_tokens': 8192,
            'messages': [{'role': 'system', 'content': 'Follow policy'}, {'role': 'user', 'content': 'hello'}],
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
        out = m.format_preset(preset, {'reasoning': True, 'function_call': True})
        self.assertEqual(out.get('max_completion_tokens'), 8192)
        self.assertNotIn('max_tokens', out)
        self.assertTrue(out.get('stream', False))
        self.assertEqual(out.get('stream_options', {}).get('include_usage'), True)
        self.assertEqual(out.get('messages', [])[0].get('role'), 'developer')
        self.assertEqual(out.get('tool_choice', {}).get('function', {}).get('name'), 'lookup')

    def test_openai_format_preset_strips_stop_for_gpt5_reasoning_models(self):
        m = importlib.import_module('lanying_vendor_openai')
        preset = {
            'model': 'gpt-5-mini',
            'stream': True,
            'max_tokens': 1024,
            'temperature': 0.9,
            'top_p': 1,
            'presence_penalty': 0.6,
            'frequency_penalty': 0,
            'stop': [' Human:', ' AI:'],
            'messages': [{'role': 'system', 'content': 'Follow policy'}, {'role': 'user', 'content': 'hello'}],
        }
        out = m.format_preset(preset, {'reasoning': True, 'function_call': True})
        self.assertEqual(out.get('max_completion_tokens'), 1024)
        self.assertNotIn('max_tokens', out)
        self.assertEqual(out.get('messages', [])[0].get('role'), 'developer')
        self.assertNotIn('stop', out)
        self.assertNotIn('temperature', out)
        self.assertNotIn('top_p', out)
        self.assertNotIn('presence_penalty', out)
        self.assertNotIn('frequency_penalty', out)

    def test_openai_format_preset_strips_sampling_params_for_o4_mini(self):
        m = importlib.import_module('lanying_vendor_openai')
        preset = {
            'model': 'o4-mini',
            'stream': True,
            'max_tokens': 1024,
            'temperature': 0.9,
            'top_p': 1,
            'presence_penalty': 0.6,
            'frequency_penalty': 0,
            'stop': [' Human:', ' AI:'],
            'messages': [{'role': 'system', 'content': 'Follow policy'}, {'role': 'user', 'content': 'hello'}],
        }
        out = m.format_preset(preset, {'reasoning': True, 'function_call': True})
        self.assertEqual(out.get('max_completion_tokens'), 1024)
        self.assertNotIn('max_tokens', out)
        self.assertEqual(out.get('messages', [])[0].get('role'), 'developer')
        self.assertNotIn('stop', out)
        self.assertNotIn('temperature', out)
        self.assertNotIn('top_p', out)
        self.assertNotIn('presence_penalty', out)
        self.assertNotIn('frequency_penalty', out)

    def test_openai_format_preset_strips_tools_for_o1_mini(self):
        m = importlib.import_module('lanying_vendor_openai')
        preset = {
            'model': 'o1-mini',
            'max_tokens': 2048,
            'messages': [{'role': 'system', 'content': 'Follow policy'}, {'role': 'user', 'content': 'hello'}],
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
        out = m.format_preset(preset, {'reasoning': True, 'function_call': False})
        self.assertEqual(out.get('messages', [])[0].get('role'), 'developer')
        self.assertEqual(out.get('max_completion_tokens'), 2048)
        self.assertNotIn('tools', out)
        self.assertNotIn('tool_choice', out)

    def test_openai_model_configs_include_current_official_chat_models(self):
        m = importlib.import_module('lanying_vendor_openai')
        models = {item.get('model'): item for item in m.model_configs()}
        self.assertIn('gpt-5.4', models)
        self.assertIn('gpt-5.4-mini', models)
        self.assertIn('gpt-5.4-nano', models)
        self.assertIn('gpt-5.2', models)
        self.assertIn('gpt-5.1', models)
        self.assertIn('gpt-4.1', models)
        self.assertIn('o3', models)
        self.assertIn('o4-mini', models)
        self.assertIn('o1', models)
        self.assertNotIn('gpt-5-pro', models)
        self.assertNotIn('gpt-5.2-pro', models)
        self.assertNotIn('o3-pro', models)
        self.assertNotIn('o1-pro', models)
        self.assertNotIn('o1-preview', models)
        self.assertNotIn('o1-preview-2024-09-12', models)
        self.assertNotIn('o1-mini', models)
        self.assertNotIn('o1-mini-2024-09-12', models)
        self.assertNotIn('gpt-4-1106-preview', models)
        self.assertNotIn('gpt-4-0125-preview', models)
        self.assertNotIn('gpt-4o-mini-audio-preview', models)
        self.assertEqual(models['gpt-5.4-mini'].get('reasoning'), True)
        self.assertEqual(models['gpt-5.4'].get('support_vision'), True)
        self.assertEqual(models['gpt-5.1'].get('reasoning'), True)
        self.assertEqual(models['gpt-4.1'].get('support_vision'), True)
        self.assertEqual(models['o4-mini'].get('function_call'), True)
        self.assertEqual(models['o1'].get('function_call'), True)

    def test_openai_chat_models_put_default_first_then_put_gpt_before_o_models(self):
        m = importlib.import_module('lanying_vendor_openai')
        chat_models = [item for item in m.model_configs() if item.get('type') == 'chat']
        self.assertTrue(len(chat_models) > 1)
        self.assertEqual(chat_models[0].get('model'), 'gpt-5-mini')
        self.assertEqual(chat_models[0].get('is_default'), True)
        remaining_models = [item.get('model') for item in chat_models[1:]]
        gpt_models = [item for item in remaining_models if item.startswith('gpt')]
        o_models = [item for item in remaining_models if item.startswith('o')]
        other_models = [item for item in remaining_models if not item.startswith('gpt') and not item.startswith('o')]
        self.assertEqual(remaining_models, gpt_models + o_models + other_models)
        self.assertEqual(gpt_models, sorted(gpt_models, reverse=True))
        self.assertEqual(o_models, sorted(o_models, reverse=True))

    def test_openai_new_gpt_model_quotas_follow_pricing_formula(self):
        m = importlib.import_module('lanying_vendor_openai')
        pricing = importlib.import_module('lanying_vendor_pricing')
        models = {item.get('model'): item for item in m.model_configs()}

        self.assertEqual(models['gpt-5.4-mini'].get('quota'), pricing.calc_adjusted_points(0.75, 4.5))
        self.assertEqual(models['gpt-5.4-mini'].get('input_price'), 0.75)
        self.assertEqual(models['gpt-5.4-mini'].get('output_price'), 4.5)
        self.assertEqual(models['gpt-5.4-mini'].get('currency'), 'USD')
        self.assertEqual(models['gpt-5.4'].get('quota'), pricing.calc_adjusted_points(2.5, 15.0))
        self.assertEqual(models['gpt-5'].get('quota'), pricing.calc_adjusted_points(1.25, 10.0))
        self.assertEqual(models['gpt-5.2'].get('quota'), pricing.calc_adjusted_points(1.75, 14.0))
        self.assertEqual(models['gpt-5.1'].get('quota'), pricing.calc_adjusted_points(1.25, 10.0))
        self.assertEqual(models['gpt-5.4-nano'].get('quota'), pricing.calc_adjusted_points(0.2, 1.25))
        self.assertEqual(models['gpt-5-nano'].get('quota'), pricing.calc_adjusted_points(0.05, 0.4))
        self.assertEqual(models['gpt-4.1'].get('quota'), pricing.calc_adjusted_points(2.0, 8.0))
        self.assertEqual(models['gpt-4.1-mini'].get('quota'), pricing.calc_adjusted_points(0.4, 1.6))
        self.assertEqual(models['gpt-4.1-nano'].get('quota'), pricing.calc_adjusted_points(0.1, 0.4))
        self.assertEqual(models['o4-mini'].get('quota'), pricing.calc_adjusted_points(1.1, 4.4))
        self.assertEqual(models['o3'].get('quota'), pricing.calc_adjusted_points(2.0, 8.0))

    def test_openai_legacy_chat_models_also_use_priced_config_shape(self):
        m = importlib.import_module('lanying_vendor_openai')
        models = {item.get('model'): item for item in m.model_configs()}

        for model in ['gpt-3.5-turbo', 'gpt-4o', 'gpt-4-turbo', 'gpt-4', 'gpt-4o-mini-2024-07-18', 'o4-mini', 'o3', 'o1']:
            self.assertIn('input_price', models[model])
            self.assertIn('output_price', models[model])
            self.assertEqual(models[model].get('currency'), 'USD')

    def test_wanjie_model_configs_include_first_party_chat_models(self):
        m = importlib.import_module('lanying_vendor_wanjie')
        pricing = importlib.import_module('lanying_vendor_pricing')
        models = {item.get('model'): item for item in m.model_configs()}

        self.assertIn('claude-sonnet-4-5-20250929', models)
        self.assertIn('GPT-4o', models)
        self.assertIn('kimi-k2', models)
        self.assertIn('mimo-v2-pro', models)
        self.assertEqual(models['mimo-v2-pro'].get('service'), 'xiaomi')
        self.assertEqual(models['kimi-k2'].get('service'), 'kimi')
        self.assertEqual(models['GPT-4o'].get('service'), 'chatgpt')
        self.assertEqual(models['claude-sonnet-4-5-20250929'].get('service'), 'claude')
        self.assertEqual(models['claude-sonnet-4-5-20250929'].get('currency'), 'CNY')
        self.assertEqual(models['claude-sonnet-4-5-20250929'].get('input_price'), 0.02139)
        self.assertEqual(models['claude-sonnet-4-5-20250929'].get('output_price'), 0.10695)
        self.assertEqual(models['claude-sonnet-4-5-20250929'].get('quota'), pricing.calc_adjusted_points(0.02139, 0.10695, currency='CNY'))
        self.assertEqual(models['claude-sonnet-4-20250514'].get('token_limit'), 200000)
        self.assertEqual(models['claude-sonnet-4-20250514'].get('max_output_tokens'), 64000)
        self.assertEqual(models['claude-opus-4-20250514'].get('max_output_tokens'), 32000)
        self.assertEqual(models['GPT-4.1'].get('token_limit'), 1047576)
        self.assertEqual(models['GPT-4.1'].get('max_output_tokens'), 32768)
        self.assertEqual(models['gpt-5.4'].get('token_limit'), 400000)
        self.assertEqual(models['gpt-5.4'].get('max_output_tokens'), 128000)
        self.assertEqual(models['GPT-4o'].get('token_limit'), 128000)
        self.assertEqual(models['GPT-4o'].get('max_output_tokens'), 16384)
        self.assertEqual(models['kimi-k2'].get('token_limit'), 256000)
        self.assertEqual(models['mimo-v2-pro'].get('token_limit'), 256000)
        self.assertEqual(models['mimo-v2-pro'].get('max_output_tokens'), 8192)
        self.assertTrue(all(item.get('type') == 'chat' for item in models.values()))

    def test_wanjie_prepare_chat_uses_domestic_direct_endpoint(self):
        m = importlib.import_module('lanying_vendor_wanjie')
        prepare_info = m.prepare_chat({'api_key': 'k'}, {'messages': [{'role': 'user', 'content': 'hi'}]})

        self.assertEqual(prepare_info.get('api_endpoint'), 'https://maas-openapi.wanjiedata.com/api/v1')
        self.assertEqual(prepare_info.get('api_endpoint_server_location'), 'domestic')
        self.assertEqual(prepare_info.get('auth_info', {}).get('vendor_type'), 'wanjie')

    def test_wanjie_list_remote_models_uses_direct_bearer_auth(self):
        m = importlib.import_module('lanying_vendor_wanjie')
        response = _FakeResponse(200, {'data': [{'id': 'wanjie-model'}]})

        with mock.patch.object(m.requests, 'request', return_value=response) as mocked_request:
            out = m.list_remote_models({'api_key': 'secret', 'validation_timeout_seconds': 8})

        self.assertEqual(out['result'], 'ok')
        self.assertEqual(out['status_code'], 200)
        self.assertEqual(out['response']['data'][0]['id'], 'wanjie-model')
        args, kwargs = mocked_request.call_args
        self.assertEqual(args[0], 'GET')
        self.assertEqual(args[1], 'https://maas-openapi.wanjiedata.com/api/v1/models')
        self.assertEqual(kwargs['headers'].get('Authorization'), 'Bearer secret')
        self.assertNotIn('X-Lanying-Proxy-Api-Endpoint', kwargs['headers'])

    def test_pricing_compare_model_quota_reports_difference(self):
        pricing = importlib.import_module('lanying_vendor_pricing')
        out = pricing.compare_model_quota({
            'model': 'gpt-5-mini',
            'service': 'chatgpt',
            'quota': 1.00,
            'input_price': 0.25,
            'output_price': 2.0,
            'currency': 'USD'
        })

        self.assertEqual(out.get('model'), 'gpt-5-mini')
        self.assertEqual(out.get('calculated_quota'), 0.94)
        self.assertEqual(out.get('quota_diff'), 0.06)
        self.assertEqual(out.get('is_matched'), False)

    def test_pricing_compare_model_quotas_skips_models_without_pricing(self):
        pricing = importlib.import_module('lanying_vendor_pricing')
        out = pricing.compare_model_quotas([
            {
                'model': 'priced-model',
                'quota': 0.94,
                'input_price': 0.25,
                'output_price': 2.0,
                'currency': 'USD'
            },
            {
                'model': 'no-price-model',
                'quota': 1.0
            }
        ])

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].get('model'), 'priced-model')
        self.assertEqual(out[0].get('is_matched'), True)

    def test_pricing_format_quota_diff_report_summarizes_mismatch(self):
        pricing = importlib.import_module('lanying_vendor_pricing')
        out = pricing.format_quota_diff_report([
            {
                'model': 'matched-model',
                'quota': 0.94,
                'input_price': 0.25,
                'output_price': 2.0,
                'currency': 'USD'
            },
            {
                'model': 'mismatched-model',
                'quota': 1.00,
                'input_price': 0.25,
                'output_price': 2.0,
                'currency': 'USD'
            }
        ])

        self.assertEqual(out.get('total'), 2)
        self.assertEqual(out.get('matched'), 1)
        self.assertEqual(out.get('mismatched'), 1)
        self.assertEqual(len(out.get('lines', [])), 2)
        self.assertIn('matched-model | quota=0.94 | calculated_quota=0.94 | quota_diff=0.0 | matched=True', out.get('lines', []))
        self.assertEqual(len(out.get('mismatched_items', [])), 1)
        self.assertEqual(out.get('mismatched_items', [])[0].get('model'), 'mismatched-model')

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


class OpenAIEndpointPriorityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_fake_tiktoken_if_needed()
        _install_fake_requests_if_needed()
        _install_fake_redis_if_needed()
        _install_fake_anthropic_if_needed()
        _install_fake_zhipuai_if_needed()
        _install_fake_etcd3_if_needed()

    def test_user_api_endpoint_bypasses_proxy_headers(self):
        m = importlib.import_module('lanying_vendor_openai')
        with mock.patch.dict(os.environ, {
            'LANYING_CONNECTOR_OPENAI_PROXY_API_BASE': 'https://proxy.example.com/v1',
            'LANYING_CONNECTOR_OPENAI_PROXY_API_KEY': 'proxy-secret'
        }, clear=False), \
             mock.patch.object(m.lanying_utils, 'is_valid_public_url', return_value=True):
            api_base, headers = m.get_api_base_and_headers({
                'api_key': 'user-secret',
                'api_endpoint': 'https://user-endpoint.example.com/v1/'
            })

        self.assertEqual(api_base, 'https://proxy.example.com/v1')
        self.assertEqual(headers.get('Authorization'), 'Basic proxy-secret')
        self.assertEqual(headers.get('Authorization-Next'), 'Bearer user-secret')
        self.assertEqual(headers.get('X-Lanying-Proxy-Api-Endpoint'), 'https://user-endpoint.example.com/v1')

    def test_official_openai_endpoint_uses_proxy_headers(self):
        m = importlib.import_module('lanying_vendor_openai')
        with mock.patch.dict(os.environ, {
            'LANYING_CONNECTOR_OPENAI_PROXY_API_BASE': 'https://proxy.example.com/v1',
            'LANYING_CONNECTOR_OPENAI_PROXY_API_KEY': 'proxy-secret'
        }, clear=False):
            api_base, headers = m.get_api_base_and_headers({
                'api_key': 'user-secret',
                'api_endpoint': 'https://api.openai.com/v1/'
            })

        self.assertEqual(api_base, 'https://proxy.example.com/v1')
        self.assertEqual(headers.get('Authorization'), 'Basic proxy-secret')
        self.assertEqual(headers.get('Authorization-Next'), 'Bearer user-secret')
        self.assertEqual(headers.get('X-Lanying-Proxy-Api-Endpoint'), 'https://api.openai.com/v1')

    def test_service_catalog_contains_xiaomi_from_wanjie_models(self):
        vendor_module = importlib.import_module('lanying_vendor')
        catalog = vendor_module.service_catalog()
        xiaomi = next((item for item in catalog if item.get('service') == 'xiaomi'), None)

        self.assertIsNotNone(xiaomi)
        self.assertTrue(any(model.get('model') == 'mimo-v2-pro' for model in xiaomi.get('models', [])))

    def test_api_type_configs_include_wanjie(self):
        vendor_module = importlib.import_module('lanying_vendor')
        config = vendor_module.get_api_type_config('wanjie')

        self.assertIsNotNone(config)
        self.assertEqual(config.get('handler_vendor'), 'wanjie')
        self.assertEqual(config.get('fields'), ['api_key'])
        self.assertEqual(config.get('services'), ['xiaomi', 'kimi', 'chatgpt', 'claude'])

    def test_wanjie_custom_model_uses_official_model_template_by_model_id(self):
        vendor_module = importlib.import_module('lanying_vendor')
        vendor_info = {
            'vendor_id': 'custom_vendor_wanjie',
            'vendor_type': 'wanjie',
            'handler_vendor': 'wanjie',
            'name': 'Custom Wanjie',
            'config_version': 2,
            'model_config': [
                {
                    'service': 'claude',
                    'model': 'claude-sonnet-4-20250514',
                    'enabled': True,
                    'source': 'custom',
                    'extra_params': []
                }
            ]
        }

        out = vendor_module._get_v2_chat_model_config(vendor_info, 'custom_vendor_wanjie', 'claude-sonnet-4-20250514')

        self.assertIsNotNone(out)
        self.assertEqual(out.get('model'), 'claude-sonnet-4-20250514')
        self.assertEqual(out.get('token_limit'), 200000)
        self.assertEqual(out.get('max_output_tokens'), 64000)
        self.assertEqual(out.get('vendor'), 'custom_vendor_wanjie')


if __name__ == '__main__':
    unittest.main()
