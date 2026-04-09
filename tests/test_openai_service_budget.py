import importlib
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
                 {'vendor': 'azure2', 'model': 'gpt-4o', 'quota': 8.0, 'token_limit': 128000, 'max_output_tokens': 16384, 'reasoning': True, 'function_call': False},
                 {'vendor': 'openai', 'model': 'hidden-model', 'hidden': True},
             ]):
            out = m.list_models_openai_api(fake_request)

        self.assertEqual(out['result'], 'ok')
        self.assertEqual(out['response']['object'], 'list')
        self.assertEqual(
            [item['id'] for item in out['response']['data']],
            ['openai/gpt-4o-mini', 'azure2/gpt-4o']
        )
        self.assertEqual(out['response']['data'][0]['object'], 'model')
        self.assertEqual(out['response']['data'][0]['owned_by'], 'openai')
        self.assertEqual(out['response']['data'][0]['quota'], 0.49)
        self.assertEqual(out['response']['data'][0]['quota_without_content_security'], 0.38)
        self.assertEqual(out['response']['data'][0]['token_limit'], 128000)
        self.assertEqual(out['response']['data'][0]['max_output_tokens'], 16384)
        self.assertEqual(out['response']['data'][0]['reasoning'], False)
        self.assertEqual(out['response']['data'][0]['function_call'], True)
        self.assertEqual(out['response']['data'][1]['reasoning'], True)
        self.assertEqual(out['response']['data'][1]['function_call'], False)

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
