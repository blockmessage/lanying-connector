import importlib
import json
import sys
import types
import unittest
from unittest import mock
from pathlib import Path


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
    try:
        import requests  # noqa: F401
        return
    except Exception:
        pass
    mod = types.ModuleType('requests')
    mod.__path__ = []

    def _not_implemented(*args, **kwargs):
        raise RuntimeError('fake requests module: please mock requests.request/post in tests')

    class Response:
        pass

    class HTTPError(Exception):
        pass

    auth_mod = types.ModuleType('requests.auth')

    class HTTPDigestAuth:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class HTTPBasicAuth:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    mod.request = _not_implemented
    mod.post = _not_implemented
    mod.Response = Response
    mod.HTTPError = HTTPError
    auth_mod.HTTPDigestAuth = HTTPDigestAuth
    auth_mod.HTTPBasicAuth = HTTPBasicAuth
    sys.modules['requests'] = mod
    sys.modules['requests.auth'] = auth_mod


def _install_fake_openai_if_needed():
    if 'openai' in sys.modules:
        return
    mod = types.ModuleType('openai')
    sys.modules['openai'] = mod


def _install_fake_redis_if_needed():
    if 'redis' in sys.modules:
        return
    try:
        import redis  # noqa: F401
        return
    except Exception:
        pass
    mod = types.ModuleType('redis')
    mod.__path__ = []

    commands_mod = types.ModuleType('redis.commands')
    commands_mod.__path__ = []
    search_mod = types.ModuleType('redis.commands.search')
    search_mod.__path__ = []
    query_mod = types.ModuleType('redis.commands.search.query')

    class StrictRedis:
        pass

    class ConnectionPool:
        @classmethod
        def from_url(cls, _url):
            return cls()

    class Query:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    mod.StrictRedis = StrictRedis
    mod.ConnectionPool = ConnectionPool
    query_mod.Query = Query

    sys.modules['redis'] = mod
    sys.modules['redis.commands'] = commands_mod
    sys.modules['redis.commands.search'] = search_mod
    sys.modules['redis.commands.search.query'] = query_mod


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


def _install_fake_openai_service_local_modules_if_needed():
    module_names = [
        'lanying_connector',
        'lanying_config',
        'lanying_redis',
        'lanying_command',
        'lanying_url_loader',
        'lanying_vendor',
        'lanying_utils',
        'lanying_ai_plugin',
        'lanying_file_storage',
        'lanying_chatbot',
        'lanying_ai_capsule',
        'lanying_im_api',
        'lanying_message',
        'lanying_image',
        'lanying_message_quota_usage',
        'lanying_grow_ai',
        'lanying_slack',
        'lanying_openclaw',
        'lanying_openai_compat',
    ]
    for name in module_names:
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)

    def safe_json_loads(value, default=None):
        if value is None or value == '':
            return default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default

    sys.modules['lanying_utils'].safe_json_loads = safe_json_loads
    sys.modules['lanying_utils'].is_valid_public_url = lambda *args, **kwargs: False

    sys.modules['lanying_openai_compat'].get_tools_as_functions = lambda preset: []

    if 'lanying_tasks' not in sys.modules:
        tasks_mod = types.ModuleType('lanying_tasks')
        for func_name in [
            'add_embedding_file',
            'delete_doc_data',
            're_run_doc_to_embedding_by_doc_ids',
            'prepare_site',
            'continue_site_task',
        ]:
            setattr(tasks_mod, func_name, lambda *args, **kwargs: None)
        sys.modules['lanying_tasks'] = tasks_mod

    if 'lanying_embedding' not in sys.modules:
        embedding_mod = types.ModuleType('lanying_embedding')
        embedding_mod.calc_functions_tokens = lambda *args, **kwargs: 0
        sys.modules['lanying_embedding'] = embedding_mod

    if 'lanying_async' not in sys.modules:
        async_mod = types.ModuleType('lanying_async')
        async_mod.executor = None
        sys.modules['lanying_async'] = async_mod

    if 'flask' not in sys.modules:
        flask_mod = types.ModuleType('flask')

        class Blueprint:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

            def route(self, *args, **kwargs):
                def _decorator(func):
                    return func
                return _decorator

        class Response:
            pass

        flask_mod.Blueprint = Blueprint
        flask_mod.request = object()
        flask_mod.make_response = lambda *args, **kwargs: None
        flask_mod.Response = Response
        sys.modules['flask'] = flask_mod

    if 'pydub' not in sys.modules:
        pydub_mod = types.ModuleType('pydub')

        class AudioSegment:
            pass

        pydub_mod.AudioSegment = AudioSegment
        sys.modules['pydub'] = pydub_mod


class OpenAIServiceBudgetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_fake_tiktoken_if_needed()
        _install_fake_requests_if_needed()
        _install_fake_openai_if_needed()
        _install_fake_redis_if_needed()
        _install_fake_anthropic_if_needed()
        _install_fake_openai_service_local_modules_if_needed()
        service_dir = str(Path(__file__).resolve().parents[1] / 'services')
        if service_dir not in sys.path:
            sys.path.insert(0, service_dir)

    def test_ai_generate_disabled_msg_is_detected_from_ai_ext(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        self.assertTrue(m.is_ai_generate_disabled_msg({
            'ext': '{"ai":{"ai_generate":false}}'
        }))
        self.assertTrue(m.is_ai_generate_disabled_msg({
            'ext': '{"lanying_connector":{"ai_generate":false}}'
        }))
        self.assertFalse(m.is_ai_generate_disabled_msg({
            'ext': '{"openclaw":{"type":"session_sync_delivery","role":"user"}}'
        }))

    def test_openclaw_delivery_no_reentry_msg_is_detected_from_openclaw_ext(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        self.assertTrue(m.is_openclaw_delivery_no_reentry_msg({
            'ext': '{"openclaw":{"type":"session_sync_delivery","session":"agent:main"}}'
        }))
        self.assertTrue(m.is_openclaw_delivery_no_reentry_msg({
            'ext': '{"openclaw":{"type":"im_reply_delivery","source":"im_reply"}}'
        }))
        self.assertFalse(m.is_openclaw_delivery_no_reentry_msg({
            'ext': '{"openclaw":{"type":"session_message_sync","session":"agent:main"}}'
        }))

    def test_maybe_sync_to_openclaw_skips_delivery_no_reentry_messages(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        base_msg = {
            'msgId': 'mid-no-reentry',
            'appId': 1,
            'from': {'uid': 'openclaw-user'},
            'to': {'uid': 'openclaw-user'},
            'ext': '{"openclaw":{"type":"session_sync_delivery","session":"agent:main"}}',
        }
        executor = mock.Mock()
        with mock.patch.object(m, 'executor', executor):
            m.maybe_sync_to_openclaw(base_msg)
        executor.submit.assert_not_called()

        command_msg = dict(base_msg)
        command_msg['ext'] = '{"openclaw":{"type":"session_message_sync","session":"agent:main"}}'
        executor = mock.Mock()
        with (
            mock.patch.object(m, 'executor', executor),
            mock.patch.object(m.lanying_openclaw, 'handle_chat_message', create=True),
        ):
            m.maybe_sync_to_openclaw(command_msg)
        executor.submit.assert_called_once()

    def test_handle_chat_message_skips_router_context_for_delivery_no_reentry_command_text(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'openclaw_node_info': {'node_id': 'node-1'},
        }
        msg = {
            'msgId': 'mid-command-looking-delivery',
            'appId': 1,
            'type': 'GROUPCHAT',
            'content': '/subagents spawn main hi',
            'from': {'uid': 'user-1'},
            'to': {'uid': 'group-1'},
            'ext': '{"openclaw":{"type":"session_sync_delivery","session":"agent:main"},"ai":{"ai_generate":false}}',
        }
        with (
            mock.patch.object(m, 'maybe_sync_to_openclaw') as maybe_sync,
            mock.patch.object(m, 'init_chatbot_config'),
            mock.patch.object(m, 'maybe_reply_message_read_ack'),
            mock.patch.object(m, 'maybe_transcription_audio_msg'),
            mock.patch.object(m, 'maybe_save_image_msg'),
            mock.patch.object(m, 'maybe_add_history'),
            mock.patch.object(m, 'list_group_openclaw_router_context_targets') as list_targets,
            mock.patch.object(m.lanying_openclaw, 'redirect_to_openclaw', create=True) as redirect,
            mock.patch.object(m, 'handle_chat_message_try') as handle_try,
        ):
            result = m.handle_chat_message(config, msg)

        self.assertEqual(result, '')
        maybe_sync.assert_called_once_with(msg)
        list_targets.assert_not_called()
        redirect.assert_not_called()
        handle_try.assert_not_called()

    def test_build_openclaw_reply_ext_contains_sync_context(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        msg = {
            'msgId': 'mid-1',
            'ext': '{"openclaw":{"type":"session_message_sync","session":"agent:main:clawchat:direct:u1","source":"control_ui_user","role":"user","message_id":"evt-1"}}'
        }
        out = m.build_openclaw_reply_ext(msg)
        self.assertEqual(out['openclaw']['type'], 'session_sync_delivery')
        self.assertEqual(out['openclaw']['session'], 'agent:main:clawchat:direct:u1')
        self.assertEqual(out['openclaw']['source'], 'control_ui_reply')
        self.assertEqual(out['openclaw']['role'], 'assistant')
        self.assertEqual(out['openclaw']['request_source'], 'control_ui_user')
        self.assertEqual(out['openclaw']['request_role'], 'user')
        self.assertEqual(out['openclaw']['request_message_id'], 'evt-1')
        self.assertEqual(out['openclaw']['request_msg_id'], 'mid-1')

    def test_append_message_can_drop_developer_prompt_when_context_is_full(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")
        preset = {
            'model': 'gpt-5-mini',
            'max_completion_tokens': 20,
            'messages': [
                {'role': 'developer', 'content': 'policy'},
                {'role': 'user', 'content': 'u1'},
                {'role': 'assistant', 'content': 'a1'},
                {'role': 'user', 'content': 'u2'},
            ]
        }
        model_config = {'model': 'gpt-5-mini', 'vendor': 'openai', 'token_limit': 100}
        message = {'role': 'tool', 'content': 'tool reply'}

        with mock.patch.object(m, 'model_token_limit', return_value=100), \
             mock.patch.object(m, 'calcMessageTokens', side_effect=lambda *args, **kwargs: 20), \
             mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=0):
            out = m.append_message('app', preset, model_config, message)

        self.assertEqual(out['messages'][0]['role'], 'user')
        self.assertNotIn('developer', [item['role'] for item in out['messages']])

    def test_list_models_openai_api_returns_vendor_prefixed_ids(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        fake_request = types.SimpleNamespace(headers={'Authorization': 'Bearer app-test-key'})

        with mock.patch.object(m, 'check_authorization', return_value={'result': 'ok', 'app_id': 'app', 'config': {}}), \
             mock.patch.object(m.lanying_vendor, 'list_models', create=True, return_value=[
                 {'vendor': 'openai', 'model': 'gpt-4o-mini', 'quota': 0.49, 'quota_without_content_security': 0.38, 'token_limit': 128000, 'max_output_tokens': 16384, 'reasoning': False, 'function_call': True},
                 {'vendor': 'openai', 'model': 'hidden-model', 'hidden': True},
             ]):
            out = m.list_models_openai_api(fake_request)

        self.assertEqual(out['result'], 'ok')
        self.assertEqual(out['response']['object'], 'list')
        self.assertEqual(
            [item['id'] for item in out['response']['data']],
            ['openai/gpt-4o-mini']
        )
        self.assertEqual(out['response']['data'][0]['object'], 'model')
        self.assertEqual(out['response']['data'][0]['owned_by'], 'openai')
        self.assertEqual(out['response']['data'][0]['quota'], 0.49)
        self.assertEqual(out['response']['data'][0]['quota_without_content_security'], 0.38)
        self.assertEqual(out['response']['data'][0]['token_limit'], 128000)
        self.assertEqual(out['response']['data'][0]['max_output_tokens'], 16384)
        self.assertEqual(out['response']['data'][0]['reasoning'], False)
        self.assertEqual(out['response']['data'][0]['function_call'], True)
        
    def test_list_models_openai_api_returns_auth_error(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        fake_request = types.SimpleNamespace(headers={})

        with mock.patch.object(m, 'check_authorization', return_value={'result': 'error', 'msg': 'bad_authorization', 'code': 'bad_authorization'}):
            out = m.list_models_openai_api(fake_request)

        self.assertEqual(out['result'], 'error')
        self.assertEqual(out['code'], 'bad_authorization')


if __name__ == '__main__':
    unittest.main()
