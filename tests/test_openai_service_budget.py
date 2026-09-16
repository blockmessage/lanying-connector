import importlib
import json
import sys
import threading
import time
import types
import unittest
from unittest import mock
from pathlib import Path


def _load_real_openai_compat():
    module_path = Path(__file__).resolve().parents[1] / 'lanying_openai_compat.py'
    spec = importlib.util.spec_from_file_location(
        'lanying_openai_compat_for_history_test', module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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

def _install_fake_dateutil_if_needed():
    if 'dateutil.relativedelta' in sys.modules:
        return
    try:
        import dateutil.relativedelta  # noqa: F401
        return
    except Exception:
        pass

    dateutil_mod = types.ModuleType('dateutil')
    relativedelta_mod = types.ModuleType('dateutil.relativedelta')

    class relativedelta:
        def __init__(self, months=0):
            self.months = months

        def __radd__(self, value):
            month = value.month + self.months
            year = value.year + (month - 1) // 12
            month = (month - 1) % 12 + 1
            return value.replace(year=year, month=month)

    relativedelta_mod.relativedelta = relativedelta
    dateutil_mod.relativedelta = relativedelta_mod
    sys.modules['dateutil'] = dateutil_mod
    sys.modules['dateutil.relativedelta'] = relativedelta_mod


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
        'lanying_im_sender',
        'lanying_message',
        'lanying_image',
        'lanying_message_quota_usage',
        'lanying_grow_ai',
        'lanying_slack',
        'lanying_openclaw',
        'lanying_openai_compat',
        'lanying_pgvector',
        'lanying_operational_storage',
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
    sys.modules['lanying_slack'].async_send_grafana_message_with_filter = lambda *args, **kwargs: None
    sys.modules['lanying_config'].get_message_daily_quota_limit_reached = lambda _app_id: 'daily quota limit reached'
    sys.modules['lanying_config'].get_lanying_connector = lambda _app_id: None

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
        _install_fake_dateutil_if_needed()
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

    def test_chat_history_keeps_one_hundred_records_for_thirty_days(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def __init__(self):
                self.expiry = None
                self.popped = False

            def rpush(self, key, value):
                return 101

            def expire(self, key, seconds):
                self.expiry = seconds

            def lpop(self, key):
                self.popped = True

        redis = FakeRedis()
        m.addHistory(redis, 'history-key', {'time': 1, 'user': 'hello'})

        self.assertEqual(100, m.maxUserHistoryLen)
        self.assertEqual(30 * 86400, m.expireSeconds)
        self.assertEqual(m.expireSeconds, m.imMessageExpireSeconds)
        self.assertEqual(m.expireSeconds, redis.expiry)
        self.assertTrue(redis.popped)

    def test_image_message_client_id_mapping_uses_history_retention(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        redis = mock.Mock()
        with mock.patch.object(
                m.lanying_redis, 'get_redis_connection',
                return_value=redis, create=True):
            m.save_im_message_client_id('client-message', 'server-message')

        redis.setex.assert_called_once_with(
            m.im_message_client_id_key('client-message'),
            m.imMessageExpireSeconds + 300,
            'server-message')

    def test_agent_tool_resume_stops_when_message_deduction_failed(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(
                f"optional dependency missing for openai_service import: {exc}")

        request_info = {
            'request_id': 'request-1',
            'app_id': 'app-1',
            'continuation': {'config': {'reply_msg_type': 'CHAT'}},
        }
        with mock.patch.object(
                m.lanying_agent_tools, 'record_resume_status'), mock.patch.object(
                m.lanying_config, 'get_lanying_connector',
                return_value={'product_id': 7002}), mock.patch.object(
                m, 'check_message_deduct_failed',
                return_value={'result': 'error', 'msg': 'deduct failed'}) as check, mock.patch.object(
                m, 'invoke_chat_model') as invoke, mock.patch.object(
                m, 'replyMessageAsync'):
            m.resume_client_tool_request(request_info, {'result': 'ok'})

        check.assert_called_once()
        invoke.assert_not_called()

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
        self.assertFalse(m.is_openclaw_delivery_no_reentry_msg({
            'ext': '{"openclaw":{"type":"session_transcript_observed","session":"agent:main"}}'
        }))

    def test_openclaw_internal_control_msg_covers_sync_events(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        self.assertTrue(m.is_openclaw_internal_control_msg({
            'ext': '{"openclaw":{"type":"session_sync_delivery","session":"agent:main"}}'
        }))
        self.assertTrue(m.is_openclaw_internal_control_msg({
            'ext': '{"openclaw":{"type":"session_message_sync","session":"agent:main"}}'
        }))
        self.assertTrue(m.is_openclaw_internal_control_msg({
            'ext': '{"openclaw":{"type":"session_transcript_observed","session":"agent:main"}}'
        }))

    def test_daily_quota_fuse_defaults_to_ten_percent_with_minimum_one(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 10
        daily_limit, percent = m.calc_daily_quota_fuse_limit('app-daily', {'message_per_month': 30, 'product_id': 7002})

        self.assertEqual(10, percent)
        self.assertEqual(3.0, daily_limit)

    def test_daily_quota_fuse_allows_high_percent(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 10000
        daily_limit, percent = m.calc_daily_quota_fuse_limit('app-daily', {'message_per_month': 10000, 'product_id': 7002})

        self.assertEqual(10000, percent)
        self.assertEqual(1000000, daily_limit)

    def test_check_message_limit_rejects_when_daily_quota_fuse_is_reached(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def hincrby(self, key, field, amount):
                if ':everyday:' in key and field == 'message_count_quota':
                    return 30
                return 0

            def incr(self, key):
                return 1

            def expire(self, key, seconds):
                pass

        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 1
        m.lanying_config.get_message_daily_quota_limit_reached = lambda _app_id: 'daily quota limit reached'
        res = m.check_daily_quota_fuse_limit('app-daily', {'message_per_month': 30, 'product_id': 7002}, FakeRedis(), 0)

        self.assertEqual('error', res['result'])
        self.assertEqual('daily_quota_fuse_limit_reached', res['code'])
        self.assertEqual(1, res['daily_quota_limit'])

    def test_chat_and_api_quota_checks_share_daily_quota_fuse(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def hincrby(self, key, field, amount):
                if ':everyday:' in key and field == 'message_count_quota':
                    return 30
                return 0

            def incr(self, key):
                return 1

            def expire(self, key, seconds):
                pass

        fake_redis = FakeRedis()
        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 1
        m.lanying_config.get_message_daily_quota_limit_reached = lambda _app_id: 'daily quota limit reached'
        config = {'message_per_month': 30, 'product_id': 7002}
        model_config = {'api_key_type': 'share', 'quota': 1}

        with mock.patch.object(m.lanying_redis, 'get_redis_connection', return_value=fake_redis, create=True):
            chat_res = m.check_message_limit('app-daily', config, model_config, True)
            api_res = m.check_deduct_message_quota('app-daily', config, 1)

        self.assertEqual('daily_quota_fuse_limit_reached', chat_res['code'])
        self.assertEqual('daily_quota_fuse_limit_reached', api_res['code'])

    def test_daily_quota_fuse_grafana_notice_is_silenced_for_one_hour_per_app(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def __init__(self):
                self.values = {}
                self.expired_keys = []

            def hincrby(self, key, field, amount):
                if ':everyday:' in key and field == 'message_count_quota':
                    return 30
                return 0

            def incr(self, key):
                self.values[key] = self.values.get(key, 0) + 1
                return self.values[key]

            def expire(self, key, seconds):
                self.expired_keys.append((key, seconds))

        fake_redis = FakeRedis()
        notices = []
        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 1
        m.lanying_config.get_message_daily_quota_limit_reached = lambda _app_id: 'daily quota limit reached'
        m.lanying_slack.async_send_grafana_message_with_filter = lambda text, filter_name: notices.append((text, filter_name))

        first_res = m.check_daily_quota_fuse_limit('app-daily', {'message_per_month': 30, 'product_id': 7002}, fake_redis, 0)
        second_res = m.check_daily_quota_fuse_limit('app-daily', {'message_per_month': 30, 'product_id': 7002}, fake_redis, 0)

        self.assertEqual('daily_quota_fuse_limit_reached', first_res['code'])
        self.assertEqual('daily_quota_fuse_limit_reached', second_res['code'])
        self.assertEqual(1, len(notices))
        self.assertIn('app_id:app-daily', notices[0][0])
        self.assertEqual('daily_quota_fuse_limit_app-daily', notices[0][1])
        self.assertEqual(1, len(fake_redis.expired_keys))
        self.assertEqual(3600, fake_redis.expired_keys[0][1])

    def test_daily_quota_fuse_skips_free_package(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def hincrby(self, key, field, amount):
                return 999999

            def incr(self, key):
                raise AssertionError("free package should not notify")

            def expire(self, key, seconds):
                raise AssertionError("free package should not notify")

        m.lanying_config.get_lanying_connector_daily_quota_fuse_percent = lambda _app_id: 1
        res = m.check_daily_quota_fuse_limit('app-free', {'message_per_month': 30, 'product_id': 7001}, FakeRedis(), 1000)
        self.assertEqual('ok', res['result'])

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

        transcript_msg = dict(base_msg)
        transcript_msg['ext'] = '{"openclaw":{"type":"session_transcript_observed","session":"agent:main"}}'
        executor = mock.Mock()
        with (
            mock.patch.object(m, 'executor', executor),
            mock.patch.object(m.lanying_openclaw, 'handle_chat_message', create=True),
        ):
            m.maybe_sync_to_openclaw(transcript_msg)
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

    def test_handle_chat_message_skips_ai_processing_for_visible_session_message_sync(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'openclaw_node_info': {'node_id': 'node-1'},
        }
        msg = {
            'msgId': 'mid-session-sync-visible',
            'appId': 1,
            'type': 'GROUPCHAT',
            'content': 'visible sync payload',
            'from': {'uid': 'chatbot-user'},
            'to': {'uid': 'group-1'},
            'ext': '{"openclaw":{"type":"session_message_sync","session":"agent:main:clawchat-router:group:group-1","source":"control_ui_reply","role":"assistant","message_id":"evt-1"},"ai":{"role":"ai"}}',
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

    def test_handle_chat_message_skips_ai_processing_for_visible_session_transcript_observed(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'openclaw_node_info': {'node_id': 'node-1'},
        }
        msg = {
            'msgId': 'mid-session-transcript-visible',
            'appId': 1,
            'type': 'GROUPCHAT',
            'content': 'visible transcript payload',
            'from': {'uid': 'chatbot-user'},
            'to': {'uid': 'group-1'},
            'ext': '{"openclaw":{"type":"session_transcript_observed","session":"agent:main:subagent:test-child","source":"control_ui_reply","message_id":"evt-2","message":{"role":"assistant","content":"hello"}},"ai":{"role":"ai"}}',
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

    def test_handle_chat_message_skips_outer_debug_finish_when_openclaw_defers(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'reply_msg_type': 'CHAT',
            'reply_from': 'chatbot-user',
            'reply_to': 'sender-user',
        }
        msg = {
            'msgId': 'mid-openclaw-defer-finish',
            'appId': 1,
            'type': 'CHAT',
            'content': 'hello',
            'from': {'uid': 'sender-user'},
            'to': {'uid': 'chatbot-user'},
            'ext': '',
        }

        def defer_to_openclaw(config_in, msg_in, retry_times):
            config_in['defer_debug_finish_to_openclaw'] = True
            return ''

        with (
            mock.patch.object(m, 'maybe_sync_to_openclaw'),
            mock.patch.object(m, 'init_chatbot_config'),
            mock.patch.object(m, 'maybe_reply_message_read_ack'),
            mock.patch.object(m, 'maybe_transcription_audio_msg'),
            mock.patch.object(m, 'maybe_save_image_msg'),
            mock.patch.object(m, 'maybe_add_history'),
            mock.patch.object(m, 'handle_chat_message_try', side_effect=defer_to_openclaw),
            mock.patch.object(m, 'add_debug_message') as mocked_debug,
            mock.patch.object(m, 'replyMessageAsync'),
        ):
            result = m.handle_chat_message(config, msg)

        self.assertIsNone(result)
        mocked_debug.assert_not_called()

    def test_add_debug_message_remembers_request_stream_with_target(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'enable_debug_message': True,
            'status_bar': True,
            'app_id': 'app-id',
            'reply_msg_type': 'GROUPCHAT',
            'reply_from': 'openclaw-user',
            'reply_to': 'group-42',
            'request_msg_id': 'request-1',
        }

        with (
            mock.patch.object(m.lanying_im_api, 'send_message_sync', return_value=777, create=True),
            mock.patch.object(m.lanying_openclaw, 'remember_request_debug_stream_state', create=True) as remember,
        ):
            m.add_debug_message(config, "处理开始")

        remember.assert_called_once()
        self.assertEqual(remember.call_args.args[:4], ('request-1', 777, 1, '[蓝莺AI] 处理开始'))
        self.assertEqual(remember.call_args.kwargs['app_id'], 'app-id')
        self.assertEqual(remember.call_args.kwargs['target_kind'], 'group')
        self.assertEqual(remember.call_args.kwargs['target_id'], 'group-42')
        self.assertEqual(remember.call_args.kwargs['target_sender_id'], 'openclaw-user')

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
        self.assertNotIn('trigger_msg_id', out['openclaw'])
        self.assertNotIn('request_msg_id', out['openclaw'])

    def test_build_openclaw_reply_ext_reads_nested_role_from_session_transcript_observed(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        msg = {
            'msgId': 'mid-2',
            'ext': json.dumps({
                'openclaw': {
                    'type': 'session_transcript_observed',
                    'session': 'agent:main:clawchat:direct:u1',
                    'source': 'control_ui_user',
                    'message_id': 'evt-2',
                    'trigger_msg_id': 'trigger-im-2',
                    'message': {
                        'role': 'user',
                        'content': 'hello',
                    },
                }
            })
        }
        out = m.build_openclaw_reply_ext(msg)
        self.assertEqual(out['openclaw']['type'], 'session_sync_delivery')
        self.assertEqual(out['openclaw']['request_source'], 'control_ui_user')
        self.assertEqual(out['openclaw']['request_role'], 'user')
        self.assertEqual(out['openclaw']['request_message_id'], 'evt-2')
        self.assertEqual(out['openclaw']['trigger_msg_id'], 'trigger-im-2')
        self.assertEqual(out['openclaw']['request_msg_id'], 'trigger-im-2')

    def test_build_openclaw_reply_ext_does_not_treat_reply_as_request_context(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        msg = {
            'msgId': 'mid-reply',
            'ext': json.dumps({
                'openclaw': {
                    'type': 'session_sync_delivery',
                    'session': 'agent:main:clawchat:direct:u1',
                    'source': 'control_ui_reply',
                    'role': 'assistant',
                    'message_id': 'router_reply_123_1',
                    'visible_delivery_owner': 'plugin',
                },
                'ai': {
                    'ai_generate': False,
                }
            })
        }
        out = m.build_openclaw_reply_ext(msg)
        self.assertEqual(out['openclaw']['source'], 'control_ui_reply')
        self.assertEqual(out['openclaw']['role'], 'assistant')
        self.assertEqual(out['openclaw']['visible_delivery_owner'], 'plugin')
        self.assertNotIn('request_source', out['openclaw'])
        self.assertNotIn('request_role', out['openclaw'])
        self.assertNotIn('request_message_id', out['openclaw'])
        self.assertNotIn('request_msg_id', out['openclaw'])
        self.assertEqual(out['ai']['ai_generate'], False)

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

    def test_add_message_statistic_writes_quota_usage_to_mysql(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        class FakeRedis:
            def hincrby(self, key, field, amount):
                return 0

        captured = []
        config = {'product_id': 7001}
        preset = {'model': 'gpt-4o-mini'}
        response = {'usage': {'completion_tokens': 10, 'prompt_tokens': 30, 'total_tokens': 40}}
        model_config = {'api_key_type': 'share', 'type': 'chat', 'vendor': 'openai', 'quota': 1}

        with (
            mock.patch.object(m.lanying_redis, 'get_redis_connection', return_value=FakeRedis(), create=True),
            mock.patch.object(m, 'get_message_statistic_keys', return_value=['stat-key']),
            mock.patch.object(m, 'add_quota', return_value=0),
            mock.patch.object(m, 'calc_used_text_size', return_value=12),
            mock.patch.object(m, 'calc_message_quota', return_value=1.5),
            mock.patch.object(m, 'add_quota_used_statistic'),
            mock.patch.object(m, 'maybe_trace_message_quota_usage'),
            mock.patch.object(m, 'maybe_statistic_ai_capsule'),
            mock.patch.object(m.lanying_operational_storage, 'append_message_quota_usage_log', side_effect=lambda entry: captured.append(entry) or {'result': 'ok'}, create=True),
        ):
            m.add_message_statistic('app-quota', config, preset, response, model_config)

        self.assertEqual(1, len(captured))
        self.assertEqual('app-quota', captured[0]['app_id'])
        self.assertEqual(1.5, captured[0]['quota'])
        self.assertEqual('chat', captured[0]['model_type'])
        self.assertEqual('openai', captured[0]['vendor'])

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

    def test_group_mention_returns_all_explicit_chatbots(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        msg_config = {'mentionAll': False, 'mentionList': [101, '102', 101, 999]}
        with (
            mock.patch.object(m, 'is_chatbot_user_id', side_effect=lambda _app, uid, _config: uid in {'101', '102'}),
            mock.patch.object(
                m.lanying_im_api, 'filter_group_member_ids', return_value=['101', '102'], create=True) as filter_ids,
        ):
            result = m.find_chatbot_user_ids_in_group_mention({}, 'app-1', 'group-1', '100', msg_config)

        self.assertEqual(result, ['101', '102'])
        filter_ids.assert_called_once_with('app-1', 'group-1', ['101', '102'])

    def test_group_explicit_mentions_exclude_chatbots_outside_group(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with (
            mock.patch.object(m, 'is_chatbot_user_id', return_value=True),
            mock.patch.object(m.lanying_im_api, 'filter_group_member_ids', return_value=['101'], create=True),
        ):
            result = m.find_chatbot_user_ids_in_group_mention(
                {}, 'app-1', 'group-1', '100', {'mentionAll': False, 'mentionList': [101, 102]})

        self.assertEqual(result, ['101'])

    def test_group_mention_resolution_caches_empty_result_for_same_message(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {}
        msg = {
            'msgId': 'mid-empty-target',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'from': {'uid': '100'},
            'to': {'uid': 'group-1'},
            'config': '{"mentionAll":false,"mentionList":[101]}',
        }
        with mock.patch.object(m, 'find_chatbot_user_ids_in_group_mention', return_value=[]) as find_targets:
            self.assertEqual(m.resolve_group_chatbot_user_ids(config, msg), [])
            self.assertEqual(m.resolve_group_chatbot_user_ids(config, msg), [])

        find_targets.assert_called_once()

    def test_group_mention_all_filters_chatbots_by_group_membership(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with (
            mock.patch.object(m.lanying_chatbot, 'is_chatbot_mode', return_value=True, create=True),
            mock.patch.object(m.lanying_chatbot, 'get_chatbot_user_ids', return_value=['101', '102', '103'], create=True),
            mock.patch.object(m.lanying_im_api, 'filter_group_member_ids', return_value=['102', '103'], create=True) as filter_ids,
        ):
            result = m.find_chatbot_user_ids_in_group_mention(
                {}, 'app-1', 'group-1', '101', {'mentionAll': True, 'mentionList': []})

        self.assertEqual(result, ['102', '103'])
        filter_ids.assert_called_once_with('app-1', 'group-1', ['102', '103'])

    def test_group_multiple_mentions_fan_out_and_share_message_side_effects(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'from_user_id': '100',
            'to_user_id': 'group-1',
            'ext': '',
            'is_sync_mode': True,
            'sync_mode_messages': [],
        }
        msg = {
            'msgId': 'mid-multi-mention',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'ctype': 'TEXT',
            'content': '@a @b hello',
            'from': {'uid': '100'},
            'to': {'uid': 'group-1'},
            'config': '{"mentionAll":false,"mentionList":[101,102]}',
            'ext': '',
        }
        target_barrier = threading.Barrier(2)
        reset_tracker_ids = []

        def init_target(child_config, _msg, target):
            child_config['initialized_target'] = target

        def handle_target(child_config, _msg, _retry, target):
            self.assertEqual(child_config['initialized_target'], target)
            self.assertIsInstance(child_config['_group_history_reset_tracker'], dict)
            reset_tracker_ids.append(id(child_config['_group_history_reset_tracker']))
            target_barrier.wait(timeout=1)
            if target == '101':
                time.sleep(0.1)
            m.add_group_reply_history(child_config, None, 'history-key', {'target': target})
            child_config['sync_mode_messages'].append(target)
            return ''

        with (
            mock.patch.object(m, 'maybe_sync_to_openclaw') as maybe_sync,
            mock.patch.object(m, 'find_chatbot_user_ids_in_group_mention', return_value=['101', '102']),
            mock.patch.object(m, 'init_chatbot_config', side_effect=init_target),
            mock.patch.object(m, 'maybe_reply_message_read_ack') as read_ack,
            mock.patch.object(m, 'maybe_transcription_audio_msg') as transcribe,
            mock.patch.object(m, 'maybe_save_image_msg') as save_image,
            mock.patch.object(m, 'maybe_add_history') as add_history,
            mock.patch.object(
                m,
                'handle_chat_message_try',
                side_effect=handle_target,
            ) as handle_try,
            mock.patch.object(m, 'add_debug_message'),
        ):
            result = m.handle_chat_message(config, msg)

        self.assertEqual(result, '')
        maybe_sync.assert_called_once_with(msg)
        self.assertCountEqual([call.args[3] for call in handle_try.call_args_list], ['101', '102'])
        self.assertEqual(config['sync_mode_messages'], ['102', '101'])
        self.assertEqual(len(set(reset_tracker_ids)), 1)
        read_ack.assert_called_once()
        transcribe.assert_called_once()
        save_image.assert_called_once()
        add_history.assert_called_once()

    def test_group_history_reset_keeps_current_message_and_runs_once(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            '_group_history_reset_tracker': {
                'lock': threading.Lock(),
                'done': False,
            },
        }
        msg = {
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'from': {'uid': '100'},
            'to': {'uid': 'group-1'},
            'ext': '{"ai":{"reset_prompt":true}}',
        }
        redis = mock.Mock()
        history_events = []
        with (
            mock.patch.object(
                m, 'removeAllHistory',
                side_effect=lambda *_args: history_events.append('reset')) as remove_history,
            mock.patch.object(
                m, 'maybe_add_history',
                side_effect=lambda *_args: history_events.append('add')) as add_history,
        ):
            m.reset_group_history_once(config, msg, redis, 'history-key')
            m.reset_group_history_once(config, msg, redis, 'history-key')

        remove_history.assert_called_once_with(redis, 'history-key')
        add_history.assert_called_once_with(config, msg)
        self.assertEqual(history_events, ['reset', 'add'])

    def test_chat_history_normalizes_completed_tool_trace_for_new_model(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        real_compat = _load_real_openai_compat()
        old_history = {
            'user': 'old question',
            'assistant': 'old answer',
            'function_messages': [
                {
                    'role': 'assistant',
                    'content': '',
                    'function_call': {
                        'id': 'call-old',
                        'name': 'lookup',
                        'arguments': '{}',
                    },
                },
                {'role': 'function', 'name': 'lookup', 'content': '{"ok":true}'},
            ],
        }
        with (
            mock.patch.object(m, 'reversed_history_generator', return_value=iter([old_history])),
            mock.patch.object(m, 'calcMessagesTokens', return_value=0),
            mock.patch.object(m, 'calcMessageTokens', return_value=1),
            mock.patch.object(
                m.lanying_openai_compat, 'normalize_chat_message',
                side_effect=real_compat.normalize_chat_message, create=True),
        ):
            result = m.loadHistory(
                {}, 'app-1', None, 'history-key', 'current question', [], 1,
                {'model': 'new-model', 'max_tokens': 64}, {},
                {'token_limit': 4096}, 'openai')

        messages = list(result['data'])
        self.assertEqual([message['role'] for message in messages], [
            'user', 'assistant', 'tool', 'assistant'])
        self.assertEqual(messages[1]['tool_calls'][0]['id'], 'call-old')
        self.assertEqual(messages[2]['tool_call_id'], 'call-old')

        messages.append({'role': 'user', 'content': 'current question'})
        prompt = m.build_chat_prompt(
            {}, 'CHAT', {'model': 'new-model'}, messages, [], [], {})
        sent_presets = []
        with (
            mock.patch.object(
                m, 'maybe_transform_preset_to_vision_preset',
                side_effect=lambda _config, _app_id, _model_config, preset: preset),
            mock.patch.object(
                m, 'chat_or_force_function_call',
                side_effect=lambda _app_id, _config, _vendor, _prepare, preset:
                sent_presets.append(preset) or {'reply': 'ok'}),
            mock.patch.object(
                m.lanying_openai_compat, 'normalize_vendor_response',
                side_effect=lambda response: response, create=True),
        ):
            model_result = m.invoke_chat_model(
                'app-1', {}, 'new-vendor', {}, {'token_limit': 4096}, prompt)

        self.assertIsNone(model_result.error)
        self.assertEqual(len(sent_presets), 1)
        self.assertEqual(
            [message['role'] for message in sent_presets[0]['messages']],
            ['user', 'assistant', 'tool', 'assistant', 'user'])

    def test_group_history_normalizes_completed_tool_trace_for_new_model(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        real_compat = _load_real_openai_compat()
        old_history = {
            'from': '101',
            'content': 'old answer',
            'function_messages_owner': '100',
            'function_messages': [
                {
                    'role': 'assistant',
                    'content': '',
                    'tool_calls': [{
                        'id': 'call-old',
                        'type': 'function',
                        'function': {'name': 'lookup', 'arguments': '{}'},
                    }],
                },
                {'role': 'tool', 'tool_call_id': 'call-old', 'content': '{"ok":true}'},
            ],
        }
        config = {'send_from': '100', 'reply_from': '101'}
        with (
            mock.patch.object(m, 'reversed_group_history_generator', return_value=iter([old_history])),
            mock.patch.object(m, 'calcMessagesTokens', return_value=0),
            mock.patch.object(m, 'calcMessageTokens', return_value=1),
            mock.patch.object(
                m.lanying_openai_compat, 'normalize_chat_message',
                side_effect=real_compat.normalize_chat_message, create=True),
        ):
            result = m.loadGroupHistory(
                config, 'app-1', None, 'history-key', 'current question', [], 1,
                {'model': 'new-model', 'max_tokens': 64}, {},
                {'token_limit': 4096}, 'openai')

        messages = list(result['data'])
        self.assertEqual([message['role'] for message in messages], [
            'assistant', 'tool', 'assistant'])
        self.assertEqual(messages[0]['tool_calls'][0]['id'], 'call-old')
        self.assertEqual(messages[1]['tool_call_id'], 'call-old')

    def test_group_history_does_not_load_other_chatbot_tool_trace(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        other_chatbot_history = {
            'from': '102',
            'content': 'other chatbot answer',
            'function_messages_owner': '100',
            'function_messages': [
                {
                    'role': 'assistant',
                    'content': '',
                    'tool_calls': [{
                        'id': 'call-other',
                        'type': 'function',
                        'function': {'name': 'lookup', 'arguments': '{}'},
                    }],
                },
                {
                    'role': 'tool',
                    'tool_call_id': 'call-other',
                    'content': '{"ok":true}',
                },
            ],
            'subsequent_messages_owner': '100',
            'subsequent_messages': [
                {'role': 'assistant', 'content': 'other private follow-up'},
            ],
        }
        config = {'send_from': '100', 'reply_from': '101'}
        with (
            mock.patch.object(
                m, 'reversed_group_history_generator',
                return_value=iter([other_chatbot_history])),
            mock.patch.object(m, 'calcMessagesTokens', return_value=0),
            mock.patch.object(m, 'calcMessageTokens', return_value=1),
        ):
            result = m.loadGroupHistory(
                config, 'app-1', None, 'history-key', 'current question', [], 1,
                {'model': 'new-model', 'max_tokens': 64}, {},
                {'token_limit': 4096}, 'openai')

        self.assertEqual(list(result['data']), [{
            'role': 'user',
            'content': 'other chatbot answer',
            'name': '102',
        }])

    def test_build_chat_prompt_preserves_group_tools_and_stream_contract(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        preset = {'model': 'model-a'}
        messages = [{'role': 'user', 'content': 'hello'}]
        system_functions = [{'name': 'system-tool'}]
        user_functions = [{'name': 'user-tool'}]
        with mock.patch.object(
            m.lanying_openai_compat,
            'functions_to_tools',
            return_value=['normalized-tools'],
            create=True,
        ):
            prompt = m.build_chat_prompt(
                {'send_from': '100'}, 'GROUPCHAT', preset, messages,
                system_functions, user_functions, {'force_stream': True})

        self.assertIs(prompt.preset, preset)
        self.assertEqual(prompt.preset['messages'], messages)
        self.assertEqual(prompt.preset['user'], '100')
        self.assertEqual(prompt.preset['functions'], system_functions)
        self.assertEqual(prompt.preset['tools'], ['normalized-tools'])
        self.assertTrue(prompt.preset['stream'])
        self.assertTrue(prompt.is_force_stream)
        self.assertEqual(prompt.oper_msg_config, {'force_callback': True})

        numeric_prompt = m.build_chat_prompt(
            {'send_from': '100'}, 'GROUPCHAT', {'model': 'model-a'}, [],
            [], [], {'force_stream': 1})
        self.assertTrue(numeric_prompt.is_force_stream)

    def test_invoke_chat_model_preserves_rate_limit_error_contract(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        prompt = m.Prompt({'model': 'model-a'}, False, {'force_callback': True})
        with (
            mock.patch.object(
                m, 'maybe_transform_preset_to_vision_preset',
                return_value={'model': 'vision-model'}) as transform,
            mock.patch.object(m, 'chat_or_force_function_call', return_value={'raw': True}),
            mock.patch.object(
                m.lanying_openai_compat,
                'normalize_vendor_response',
                return_value={
                    'result': 'error',
                    'code': 'rate_limit_reached_error',
                    'message': 'vendor detail',
                },
                create=True,
            ),
        ):
            result = m.invoke_chat_model(
                'app-1', {}, 'openai', {}, {'token_limit': 1024}, prompt)

        transform.assert_called_once()
        self.assertEqual(result.preset, {'model': 'vision-model'})
        self.assertEqual(result.error, {
            'result': 'error',
            'code': 'rate_limit_reached_error',
            'msg': '请求过快，请稍后再试。',
        })

    def test_group_ai_callback_records_history_without_triggering_reply(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {'ext': '{"ai":{"role":"ai"}}'}
        msg = {
            'msgId': 'bot-reply-1',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'ctype': 'TEXT',
            'content': 'bot reply',
            'from': {'uid': '101'},
            'to': {'uid': 'group-1'},
            'config': '{}',
            'ext': '{"ai":{"role":"ai","stream":false,"request_msg_id":"user-1"}}',
        }
        with (
            mock.patch.object(m, 'maybe_sync_to_openclaw'),
            mock.patch.object(
                m.lanying_redis, 'get_redis_connection', return_value=None, create=True),
            mock.patch.object(m.group_history_repository, 'record_received_message') as record,
            mock.patch.object(m, 'maybe_save_image_msg') as save_image,
            mock.patch.object(m, 'route_group_chatbot_message') as route,
            mock.patch.object(m, 'handle_chat_message_try') as handle_try,
        ):
            result = m.handle_chat_message(config, msg)

        self.assertEqual(result, '')
        record.assert_called_once_with(config, msg)
        save_image.assert_called_once_with(config, msg)
        route.assert_not_called()
        handle_try.assert_not_called()

    def test_group_ai_history_records_only_final_stream_message(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        base_msg = {
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'content': 'reply',
            'from': {'uid': '101'},
            'to': {'uid': 'group-1'},
        }
        intermediate = dict(
            base_msg,
            ctype='TEXT',
            ext='{"ai":{"role":"ai","stream":true,"finish":false}}',
        )
        final = dict(
            base_msg,
            ctype='REPLACE',
            ext='{"ai":{"role":"ai","stream":true,"finish":true}}',
        )

        self.assertFalse(m.need_add_history({}, intermediate))
        self.assertTrue(m.need_add_history({}, final))

        image = dict(
            base_msg,
            ctype='IMAGE',
            content='',
            ext='{"ai":{"role":"ai","stream":false,"finish":true}}',
        )
        self.assertTrue(m.need_add_history({}, image))

    def test_async_group_image_uses_callback_as_only_history_source(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        self.assertFalse(m.should_add_image_subsequent_message({
            'reply_msg_type': 'GROUPCHAT',
        }))
        self.assertTrue(m.should_add_image_subsequent_message({
            'reply_msg_type': 'GROUPCHAT',
            'is_sync_mode': True,
        }))
        self.assertTrue(m.should_add_image_subsequent_message({
            'reply_msg_type': 'CHAT',
        }))

    def test_group_history_pending_reply_is_saved_as_expiring_list(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        redis = mock.Mock()
        pipeline = redis.pipeline.return_value
        config = {
            'app_id': 'app-1',
            'reply_to': 'group-1',
            'reply_from': '101',
            'request_msg_id': 'user-1',
        }
        history = {
            'function_messages': [{'role': 'tool', 'content': 'result'}],
            'function_messages_owner': '100',
        }
        with mock.patch.object(
            m.lanying_redis, 'get_redis_connection', return_value=redis, create=True):
            m.group_history_repository.save_pending_reply(config, history)

        key = 'lanying:connector:history:pending:group:app-1:group-1:101:user-1'
        redis.pipeline.assert_called_once_with(transaction=True)
        pipeline.delete.assert_called_once_with(key)
        pipeline.rpush.assert_called_once()
        self.assertEqual(pipeline.rpush.call_args.args[0], key)
        self.assertEqual(
            json.loads(pipeline.rpush.call_args.args[1]), history)
        pipeline.expire.assert_called_once_with(key, 3600)
        pipeline.execute.assert_called_once_with()

    def test_group_history_callback_consumes_pending_private_context_once(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        redis = mock.Mock()
        redis.lpop.side_effect = [json.dumps({
            'function_messages': [{'role': 'tool', 'content': 'result'}],
            'function_messages_owner': '100',
        }).encode('utf-8'), None]
        redis.rpush.return_value = 1
        msg = {
            'msgId': 'bot-reply-1',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'ctype': 'TEXT',
            'content': 'bot reply',
            'from': {'uid': '101'},
            'to': {'uid': 'group-1'},
            'config': '{}',
            'ext': '{"ai":{"role":"ai","request_msg_id":"user-1"}}',
        }
        with mock.patch.object(
            m.lanying_redis, 'get_redis_connection', return_value=redis, create=True):
            m.group_history_repository.record_received_message({}, msg)
            second_msg = dict(msg, msgId='bot-reply-2', content='second reply')
            m.group_history_repository.record_received_message({}, second_msg)

        first_history = json.loads(redis.rpush.call_args_list[0].args[1])
        second_history = json.loads(redis.rpush.call_args_list[1].args[1])
        self.assertEqual(first_history['content'], 'bot reply')
        self.assertEqual(
            first_history['function_messages'],
            [{'role': 'tool', 'content': 'result'}])
        self.assertEqual(first_history['function_messages_owner'], '100')
        self.assertEqual(second_history['content'], 'second reply')
        self.assertNotIn('function_messages', second_history)
        self.assertEqual(redis.lpop.call_count, 2)
        redis.lpop.assert_called_with(
            'lanying:connector:history:pending:group:app-1:group-1:101:user-1')
        redis.delete.assert_not_called()

    def test_group_history_callback_normalizes_numeric_group_id(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        redis = mock.Mock()
        redis.rpush.return_value = 1
        msg = {
            'msgId': 'bot-reply-2',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'ctype': 'TEXT',
            'content': 'bot reply',
            'from': {'uid': 101},
            'to': {'uid': 200},
            'config': '{}',
            'ext': '{"ai":{"role":"ai"}}',
        }
        with mock.patch.object(
            m.lanying_redis, 'get_redis_connection', return_value=redis, create=True):
            m.group_history_repository.record_received_message({}, msg)

        self.assertEqual(
            redis.rpush.call_args.args[0],
            'lanying:connector:history:list:group:app-1:200')

    def test_async_group_error_reply_waits_for_callback_before_shared_history(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {
            'reply_msg_type': 'GROUPCHAT',
            'reply_from': '101',
            'reply_to': 'group-1',
            'send_from': '100',
            'request_msg_id': 'user-1',
            'ext': '',
        }
        msg = {'appId': 'app-1', 'type': 'GROUPCHAT'}
        result = m.ChatHandlerResult(['error reply'], 'internal_error', 'error reply')
        with (
            mock.patch.object(m.group_history_repository, 'save_pending_reply') as save_pending,
            mock.patch.object(m, 'replyMessageAsync') as send_reply,
            mock.patch.object(m, 'add_group_reply_history') as add_history,
            mock.patch.object(m, 'add_debug_message'),
            mock.patch.object(m.time, 'sleep'),
        ):
            m.emit_chat_handler_result(config, msg, result)

        save_pending.assert_called_once()
        send_reply.assert_called_once()
        add_history.assert_not_called()
        self.assertEqual(config['app_id'], 'app-1')

    def test_group_mention_all_without_member_does_not_resolve_twice(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        config = {'from_user_id': '100', 'to_user_id': 'group-1', 'ext': ''}
        msg = {
            'msgId': 'mid-mention-all-empty',
            'appId': 'app-1',
            'type': 'GROUPCHAT',
            'ctype': 'TEXT',
            'content': '@all hello',
            'from': {'uid': '100'},
            'to': {'uid': 'group-1'},
            'config': '{"mentionAll":true,"mentionList":[]}',
            'ext': '',
        }
        with (
            mock.patch.object(m, 'maybe_sync_to_openclaw'),
            mock.patch.object(m, 'find_chatbot_user_ids_in_group_mention', return_value=[]) as find_targets,
            mock.patch.object(m, 'init_chatbot_config'),
            mock.patch.object(m, 'maybe_reply_message_read_ack'),
            mock.patch.object(m, 'maybe_transcription_audio_msg'),
            mock.patch.object(m, 'maybe_save_image_msg'),
            mock.patch.object(m, 'maybe_add_history'),
            mock.patch.object(m, 'handle_chat_message_try', return_value='') as handle_try,
            mock.patch.object(m, 'add_debug_message'),
        ):
            result = m.handle_chat_message(config, msg)

        self.assertIsNone(result)
        find_targets.assert_called_once()
        self.assertEqual(handle_try.call_args.args[3], '')


if __name__ == '__main__':
    unittest.main()
