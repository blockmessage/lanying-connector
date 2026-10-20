import importlib
import json
import sys
import threading
import time
import types
import unittest
from contextlib import ExitStack
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
        'lanying_masked_config',
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
        # Never overwrite attributes on a real module already held by another suite.
        # Keep available helpers, but give this suite its own module namespace.
        module = types.ModuleType(name)
        if name in sys.modules:
            module.__dict__.update(vars(sys.modules[name]))
        sys.modules[name] = module

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

    sys.modules['lanying_openai_compat'] = _load_real_openai_compat()
    sys.modules['lanying_image'].calculate_tokens = lambda *_args: 85
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
        modules = mock.patch.dict(sys.modules)
        modules.start()
        cls.addClassCleanup(modules.stop)
        paths = mock.patch.object(sys, 'path', list(sys.path))
        paths.start()
        cls.addClassCleanup(paths.stop)
        # Import against this suite's dependencies, not a previous suite's service cache.
        sys.modules.pop('openai_service', None)
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

    def test_download_embedding_doc_reads_only_resolved_app_embedding(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with mock.patch.object(m.lanying_embedding, 'get_embedding_name_info', return_value={'embedding_uuid': 'uuid-1'}, create=True), \
                mock.patch.object(m.lanying_embedding, 'get_doc', return_value={
                    'filename': 'guide.pdf',
                    'object_name': 'embedding/app-a/uuid-1/doc-1.pdf',
                    'type': 'file'
                }, create=True), \
                mock.patch.object(m.lanying_file_storage, 'open_object', return_value={
                    'result': 'ok', 'stream': 'file-stream', 'content_length': 11
                }, create=True) as open_object:
            result = m.download_embedding_doc('app-a', 'knowledge', 'doc-1')

        self.assertEqual('guide.pdf', result['filename'])
        self.assertEqual('application/pdf', result['content_type'])
        self.assertEqual('file-stream', result['stream'])
        self.assertEqual(11, result['content_length'])
        open_object.assert_called_once_with('embedding/app-a/uuid-1/doc-1.pdf')

    def test_download_embedding_doc_does_not_read_storage_for_unknown_embedding(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with mock.patch.object(m.lanying_embedding, 'get_embedding_name_info', return_value=None, create=True), \
                mock.patch.object(m.lanying_file_storage, 'open_object', create=True) as open_object:
            result = m.download_embedding_doc('app-a', 'missing', 'doc-1')

        self.assertIsNone(result)
        open_object.assert_not_called()

    def test_download_embedding_doc_accepts_stored_web_content(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with mock.patch.object(m.lanying_embedding, 'get_embedding_name_info', return_value={'embedding_uuid': 'uuid-1'}, create=True), \
                mock.patch.object(m.lanying_embedding, 'get_doc', return_value={
                    'filename': 'https://example.com/docs',
                    'object_name': 'embedding/app-a/uuid-1/doc-2.html',
                    'type': 'url'
                }, create=True), \
                mock.patch.object(m.lanying_file_storage, 'open_object', return_value={
                    'result': 'ok', 'stream': 'web-stream', 'content_length': 13
                }, create=True) as open_object:
            result = m.download_embedding_doc('app-a', 'knowledge', 'doc-2')

        self.assertEqual('docs.html', result['filename'])
        self.assertEqual('text/html', result['content_type'])
        self.assertEqual('web-stream', result['stream'])
        open_object.assert_called_once_with('embedding/app-a/uuid-1/doc-2.html')

    def test_embedding_doc_download_metadata_sanitizes_web_urls(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        root = m.embedding_doc_download_metadata({
            'filename': 'https://www.example.com/',
            'object_name': 'embedding/app-a/uuid-1/root.html',
            'type': 'site'
        }, 'root', 1)
        query = m.embedding_doc_download_metadata({
            'filename': 'https://example.com/docs?lang=zh#intro',
            'object_name': 'embedding/app-a/uuid-1/docs.html',
            'type': 'url'
        }, 'query', 1)
        existing_extension = m.embedding_doc_download_metadata({
            'filename': 'https://example.com/report.pdf?download=1',
            'object_name': 'embedding/app-a/uuid-1/report.pdf',
            'type': 'url'
        }, 'pdf', 1)
        upload = m.embedding_doc_download_metadata({
            'filename': 'report.final.pdf',
            'object_name': 'embedding/app-a/uuid-1/upload.pdf',
            'type': 'file'
        }, 'upload', 1)

        self.assertEqual('www_example_com.html', root['filename'])
        self.assertEqual('text/html', root['content_type'])
        self.assertEqual('docs.html', query['filename'])
        self.assertEqual('text/html', query['content_type'])
        self.assertEqual('report.pdf', existing_extension['filename'])
        self.assertEqual('application/pdf', existing_extension['content_type'])
        self.assertEqual('report.final.pdf', upload['filename'])

    def test_download_embedding_doc_accepts_legacy_file_without_type(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with mock.patch.object(m.lanying_embedding, 'get_embedding_name_info', return_value={'embedding_uuid': 'uuid-1'}, create=True), \
                mock.patch.object(m.lanying_embedding, 'get_doc', return_value={
                    'filename': 'legacy.csv',
                    'object_name': 'embedding/app-a/uuid-1/doc-legacy.csv'
                }, create=True), \
                mock.patch.object(m.lanying_file_storage, 'open_object', return_value={
                    'result': 'ok', 'stream': 'legacy-stream', 'content_length': 7
                }, create=True) as open_object:
            result = m.download_embedding_doc('app-a', 'knowledge', 'doc-legacy')

        self.assertEqual('legacy.csv', result['filename'])
        self.assertEqual('legacy-stream', result['stream'])
        open_object.assert_called_once_with('embedding/app-a/uuid-1/doc-legacy.csv')

    def test_embedding_doc_list_marks_downloadable_files(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        docs = [
            {'doc_id': 'new', 'type': 'file', 'object_name': 'new.pdf'},
            {'doc_id': 'legacy', 'object_name': 'legacy.csv'},
            {'doc_id': 'url', 'type': 'url', 'object_name': 'cached.html'},
            {'doc_id': 'site', 'type': 'site', 'object_name': 'site.html'},
            {'doc_id': 'plugin', 'type': 'plugin', 'object_name': 'dummy'},
            {'doc_id': 'failed', 'type': 'file', 'object_name': ''}
        ]
        with mock.patch.object(m.lanying_embedding, 'get_embedding_doc_info_list',
                               return_value=(len(docs), docs), create=True):
            total, result = m.get_embedding_doc_info_list('app-a', 'knowledge', 0, 20)

        self.assertEqual(6, total)
        self.assertEqual([True, True, True, True, False, False],
                         [doc['downloadable'] for doc in result])

    def test_prepare_embedding_doc_download_validates_without_opening_file(self):
        try:
            m = importlib.import_module('openai_service')
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest(f"optional dependency missing for openai_service import: {exc}")

        with mock.patch.object(m.lanying_embedding, 'get_embedding_name_info', return_value={'embedding_uuid': 'uuid-1'}, create=True), \
                mock.patch.object(m.lanying_embedding, 'get_doc', return_value={
                    'filename': 'guide.pdf',
                    'object_name': 'embedding/app-a/uuid-1/doc-1.pdf',
                    'type': 'file'
                }, create=True), \
                mock.patch.object(m.lanying_file_storage, 'stat_object', return_value={
                    'result': 'ok', 'content_length': 11
                }, create=True) as stat_object, \
                mock.patch.object(m.lanying_file_storage, 'open_object', create=True) as open_object:
            result = m.prepare_embedding_doc_download('app-a', 'knowledge', 'doc-1')

        self.assertEqual('guide.pdf', result['filename'])
        self.assertEqual(11, result['content_length'])
        stat_object.assert_called_once_with('embedding/app-a/uuid-1/doc-1.pdf')
        open_object.assert_not_called()

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

    def test_invalid_quota_rejected_before_legacy_or_durable_statistics(self):
        m = importlib.import_module('openai_service')
        for enabled in (False, True):
            for quota in (-1, -0.000001, float('nan'), float('inf'), -float('inf'), True):
                with self.subTest(enabled=enabled, quota=quota), \
                     mock.patch.object(m.lanying_gift_quota, 'enabled', return_value=enabled), \
                     mock.patch.object(m.lanying_gift_quota, 'apply_statistics') as apply, \
                     mock.patch.object(m, 'get_message_statistic_keys') as keys, \
                     mock.patch.object(m, 'notify_butler') as notify:
                    redis = mock.Mock()
                    with self.assertRaises(ValueError):
                        m._record_quota_statistics(redis, 'app', {}, quota, 'share', {'message_count': 1})
                    keys.assert_not_called()
                    apply.assert_not_called()
                    notify.assert_not_called()
                    self.assertEqual([], redis.mock_calls)

    def test_invalid_quota_cannot_trigger_admission_or_purchase(self):
        m = importlib.import_module('openai_service')
        with mock.patch.object(m.lanying_redis, 'get_redis_connection', create=True) as connection, \
             mock.patch.object(m, 'notify_butler') as notify:
            for quota in (-1, float('nan'), float('inf'), True):
                with self.assertRaises(ValueError):
                    m.check_deduct_message_quota('app', {'product_id': 7001}, quota)
            connection.assert_not_called()
            notify.assert_not_called()

    def test_zero_and_positive_statistics_keep_original_values(self):
        from test_gift_quota import FakeRedis
        m = importlib.import_module('openai_service')
        for enabled in (False, True):
            for quota in (0, 3, 0.12345):
                with self.subTest(enabled=enabled, quota=quota), \
                     mock.patch.object(m.lanying_gift_quota, 'enabled', return_value=enabled), \
                     mock.patch.object(m.lanying_gift_quota, 'purchases_enabled', return_value=False), \
                     mock.patch.object(m.lanying_gift_quota, 'billable_used', return_value=0), \
                     mock.patch.object(m.lanying_gift_quota, 'apply_statistics', return_value=True) as apply, \
                     mock.patch.object(m, 'get_message_statistic_keys', return_value=['key']):
                    redis = FakeRedis()
                    self.assertTrue(m._record_quota_statistics(redis, 'app', {}, quota, 'share', {'message_count': 1}))
                    if enabled:
                        self.assertEqual(quota, apply.call_args.args[1]['quota'])
                    else:
                        self.assertEqual(1, redis.hget('key', 'message_count'))
                        self.assertEqual(round(quota * 10000),
                                         int(redis.hget('key', 'message_count_quota') or 0) * 10000
                                         + int(redis.hget('key', 'message_count_quota_float') or 0))

    def test_quota_admission_uses_positive_balance_not_estimated_cost(self):
        m = importlib.import_module('openai_service')
        class FakeRedis:
            def hincrby(self, key, field, amount):
                return 0
        redis = FakeRedis()
        cases = [
            # used, gift, purchased, product, extra price, allowed
            (29, 0, 0, 7001, 0, True),
            (30, 0, 0, 7001, 0, False),
            (59, 0, 0, 7001, 0, False),
            (31, .5, 0, 7001, 0, True),
            (31, 0, 2, 7001, 0, True),
            (32, 0, 2, 7001, 0, False),
            (30, 0, 0, 7002, 0, False),
            (30, 0, 0, 7002, 1, True),
            (330, 0, 0, 7002, 1, False),
        ]
        for used, gift, purchased, product, payg, allowed in cases:
            with self.subTest(used=used, gift=gift, purchased=purchased, payg=payg):
                redis.hincrby = lambda key, field, amount: purchased if field == 'message_count_quota_buy' else used
                config = {'message_per_month': 30, 'product_id': product,
                          'enable_extra_price': payg, 'quota_pre_check': 30}
                with mock.patch.object(m.lanying_redis, 'get_redis_connection', return_value=redis, create=True), \
                     mock.patch.object(m, '_gift_allowance', return_value=(used, gift)), \
                     mock.patch.object(m.lanying_message_credits, 'auto_recharge', return_value=bool(payg) and product > 7001), \
                     mock.patch.object(m.lanying_gift_quota, 'purchases_enabled', return_value=False), \
                     mock.patch.object(m, 'check_daily_quota_fuse_limit', return_value={'result':'ok'}), \
                     mock.patch.object(m.lanying_config, 'get_message_quota_not_enough', return_value='no quota', create=True):
                    model_result = m.check_message_limit('app', config, {'api_key_type':'share', 'quota':30}, False)
                    search_result = m.check_deduct_message_quota('app', config, 30)
                self.assertEqual(allowed, model_result['result'] == 'ok')
                self.assertEqual(allowed, search_result['result'] == 'ok')

    def test_purchased_credit_mode_requires_successful_refill_before_new_call(self):
        m = importlib.import_module('openai_service')
        redis = mock.Mock()
        redis.hincrby.return_value = 0
        config = {'message_per_month': 30, 'product_id': 7001, '_quota_app_id': 'app'}
        with mock.patch.object(m.lanying_gift_quota, 'purchases_enabled', return_value=True), \
             mock.patch.object(m.lanying_message_credits, 'has_preference', return_value=True), \
             mock.patch.object(m.lanying_message_credits, 'auto_recharge', return_value=True), \
             mock.patch.object(m, 'notify_butler', return_value=False) as notify, \
             mock.patch.object(m.lanying_message_credits, 'remaining', return_value=1000):
            self.assertFalse(m._has_message_quota_remaining(redis, 'key', config, 30, 0))
            notify.assert_called_once()
        with mock.patch.object(m.lanying_gift_quota, 'purchases_enabled', return_value=True), \
             mock.patch.object(m.lanying_message_credits, 'has_preference', return_value=True), \
             mock.patch.object(m.lanying_message_credits, 'auto_recharge', return_value=True), \
             mock.patch.object(m, 'notify_butler', return_value=True), \
             mock.patch.object(m.lanying_message_credits, 'remaining', return_value=1000):
            self.assertTrue(m._has_message_quota_remaining(redis, 'key', config, 30, 0))
        with mock.patch.object(m.lanying_message_credits, 'auto_recharge', return_value=False), \
             mock.patch.object(m, 'notify_butler') as notify:
            self.assertFalse(m._has_message_quota_remaining(redis, 'key', config, 30, 0))
            notify.assert_not_called()

    def test_unconfigured_paid_apps_keep_the_original_buffer_with_feature_enabled(self):
        m = importlib.import_module('openai_service')
        redis=mock.Mock(); redis.hincrby.return_value=0
        config={'product_id':7002,'message_per_month':30,'_quota_app_id':'app'}
        with mock.patch.object(m.lanying_gift_quota,'purchases_enabled',return_value=True), \
             mock.patch.object(m.lanying_message_credits,'auto_recharge',return_value=True), \
             mock.patch.object(m.lanying_message_credits,'has_preference',return_value=False), \
             mock.patch.object(m,'notify_butler') as notify:
            self.assertTrue(m._has_message_quota_remaining(redis,'key',config,30,0))
            self.assertFalse(m._has_message_quota_remaining(redis,'key',config,330,0))
            notify.assert_not_called()

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

        with mock.patch.object(m.lanying_redis, 'get_redis_connection', return_value=fake_redis, create=True), \
                mock.patch.object(m, '_gift_allowance', return_value=(0, 0)):
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

    def test_append_message_keeps_instructions_and_drops_complete_old_turn(self):
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
        model_config = {'model': 'gpt-5-mini', 'vendor': 'openai', 'token_limit': 170}
        message = {'role': 'tool', 'content': 'tool reply'}

        with mock.patch.object(m, 'calcMessagesTokens', side_effect=lambda _a, msgs, *_args: len(msgs) * 20 + 2), \
             mock.patch.object(m, 'calcMessageTokens', side_effect=lambda *args, **kwargs: 20), \
             mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=0):
            out = m.append_message('app', preset, model_config, message)

        self.assertEqual([item['role'] for item in out['messages']],
                         ['developer', 'user', 'tool'])
        self.assertEqual(out['messages'][1]['content'], 'u2')

    def test_large_tool_result_history_uses_same_truncated_content_as_request(self):
        m = importlib.import_module('openai_service')
        model_config = {'model': 'model', 'vendor': 'openai', 'token_limit': 1000}
        preset = {'model': 'model', 'max_tokens': 100, 'messages': [
            {'role': 'system', 'content': 'policy'},
            {'role': 'user', 'content': 'query'},
        ]}
        tool_call = {'id': 'call-1', 'type': 'function',
                     'function': {'name': 'query', 'arguments': '{}'}}
        function_response = {'result': 'ok', 'data': 'x' * 10000}
        function_messages = []
        encoding = mock.Mock()
        encoding.encode.side_effect = lambda text, **_kwargs: list(text)
        with (
            mock.patch.object(m.lanying_vendor, 'encoding_for_model',
                              return_value=encoding, create=True),
            mock.patch.object(m, 'maybe_transform_preset_to_vision_preset',
                              side_effect=lambda _c, _a, _m, p: p),
            mock.patch.object(m, 'chat_or_force_function_call',
                              return_value={'result': 'ok', 'reply': 'answer'}) as chat,
        ):
            m._append_function_result_and_continue(
                'app', {}, tool_call, 'call-1', preset, model_config,
                'openai', {}, function_messages, function_response, True)
            sent = chat.call_args.args[-1]['messages'][-1]
            self.assertLess(len(sent['content']), 1000)
            self.assertIs(sent, function_messages[-1])
            self.assertEqual(sent['content'], function_messages[-1]['content'])
            # Persist/reload as JSON, just like chat history, not by shared identity.
            history = json.loads(json.dumps({
                'user': 'query', 'assistant': 'answer',
                'function_messages': function_messages,
            }))
            with mock.patch.object(m, 'reversed_history_generator',
                                   return_value=iter([history])):
                result = m.loadHistory(
                    {'history_msg_size_max': 1024}, 'app', None, 'key',
                    'follow up', [{'role': 'system', 'content': 'policy'}],
                    1, {'model': 'model', 'max_tokens': 100}, {}, model_config, 'openai')
            self.assertEqual('ok', result['result'])
            loaded = list(result['data'])
            self.assertEqual(['user', 'assistant', 'tool', 'assistant'],
                             [message['role'] for message in loaded])
            self.assertEqual(sent['content'], loaded[2]['content'])
        # Only the model/history message is shortened, not the business result.
        self.assertEqual('x' * 10000, function_response['data'])
        self.assertEqual('{}', tool_call['function']['arguments'])

    def test_batch_and_single_counters_include_tool_call_arguments(self):
        m = importlib.import_module('openai_service')
        encoding = mock.Mock()
        encoding.encode.side_effect = lambda text, **_kwargs: list(text)
        with mock.patch.object(m.lanying_vendor, 'encoding_for_model',
                               return_value=encoding, create=True):
            counts = []
            for size in (10, 10000):
                message = {'role': 'assistant', 'content': None, 'tool_calls': [
                    {'id': 'call-1', 'type': 'function',
                     'function': {'name': 'search', 'arguments': 'x' * size}}]}
                single = m.calcMessageTokens('app', message, 'model', 'vendor')
                batch = m.calcMessagesTokens('app', [message], 'model', 'vendor')
                self.assertEqual(single + 2, batch)
                counts.append(batch)
            self.assertEqual(9990, counts[1] - counts[0])

    def test_final_tools_reduce_chat_and_group_history_budget(self):
        m = importlib.import_module('openai_service')
        functions = [{'name': 'query', 'parameters': {'type': 'object'}}]
        with (
            mock.patch.object(m, 'calcMessagesTokens', return_value=20),
            mock.patch.object(m, 'calcMessageTokens', return_value=100),
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens',
                              side_effect=lambda fs, *_args: 600 if fs else 0) as counter,
            mock.patch.object(m, 'reversed_history_generator',
                              side_effect=lambda _: iter([{'user': 'old', 'assistant': 'answer'}])),
            mock.patch.object(m, 'reversed_group_history_generator',
                              side_effect=lambda _: iter([{'from': '100', 'content': 'old'}])),
        ):
            for loader in (m.loadHistory, m.loadGroupHistory):
                for fs, expected_history in (([], True), (functions, False)):
                    result = loader(
                        {'send_from': '100', 'reply_from': '101'}, 'app', None,
                        'key', 'hello', [], 1, {'model': 'model', 'max_tokens': 100,
                                              'functions': fs},
                        {}, {'token_limit': 1000}, 'vendor')
                    self.assertEqual('ok', result['result'])
                    self.assertEqual(expected_history, bool(list(result['data'])))
            self.assertEqual(counter.call_args.args, (functions, 'model', 'vendor', 'app'))

    def test_skills_are_loaded_before_retrieval_and_final_tools_before_history(self):
        m = importlib.import_module('openai_service')
        system = {'name': 'system_tool', 'priority': 1}
        skill = {'name': 'skill_tool', 'priority': 5}
        plugin = types.SimpleNamespace(
            function='{"name":"plugin_tool","parameters":{}}',
            block_id='block', doc_id='doc', owner_app_id='app', vector_score=0)
        events = []

        def load_skills(_app, _config, messages, functions):
            events.append('skills')
            messages.append({'role': 'system', 'content': 'skill instructions'})
            functions.append(skill)
            return messages, functions

        def retrieve(*args):
            events.append('retrieval')
            # (4000 - 100 - 64) / 1.05 - two tools - three messages - history reserve
            self.assertEqual(2881 if supports_tools else 3081, args[-1])
            return [plugin]

        def history(*args):
            events.append('history')
            self.assertEqual('question with metadata', args[4])
            self.assertIn('skill instructions', [msg['content'] for msg in args[5]])
            self.assertEqual({'system_tool', 'skill_tool', 'plugin_tool'},
                             {f['name'] for f in args[7]['functions']})
            return {'result': 'error', 'code': 'test-stop'}

        materials = types.ModuleType('lanying_seenical_materials')
        materials.retrieval_scope = lambda *_args: None
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}))
            for name, replacement in {
                'check_message_limit': lambda *_args: {'result': 'ok', 'api_key_type': 'share'},
                'get_preset_auth_info': lambda *_args: {},
                'get_system_functions': lambda *_args: [system],
                'make_metadata_from_msg': lambda *_args: {},
                'format_content_and_metadata': lambda *_args: ('question with metadata', {}),
                'get_embedding_info': lambda *_args: {},
                'set_embedding_info': lambda *_args: None,
                'calc_embedding_query_text': lambda *_args: 'query',
                'calcMessagesTokens': lambda *_args: 20,
                'calcMessageTokens': lambda *_args: 20,
                'multi_embedding_search': retrieve,
                'add_debug_message': lambda *_args, **_kwargs: None,
                'loadHistory': history,
                'loadGroupHistory': history,
            }.items():
                stack.enter_context(mock.patch.object(m, name, side_effect=replacement))
            for module, name, replacement in [
                (m.lanying_vendor, 'prepare_chat', lambda *_args: {}),
                (m.lanying_redis, 'get_redis_connection', lambda: mock.Mock()),
                (m.lanying_embedding, 'get_preset_embedding_infos', lambda *_args: [{'embedding_name': 'kb'}]),
                (m.lanying_embedding, 'parse_segment_id_int_value', lambda *_args: 0),
                (m.lanying_embedding, 'calc_functions_tokens', lambda fs, *_args: len(fs) * 100),
                (m.lanying_ai_plugin, 'get_preset_function_embeddings', lambda *_args: []),
                (m.lanying_ai_plugin, 'remove_function_parameters_without_function_call_reference',
                 lambda _app, function, _doc: function),
                (m.lanying_agent_tools, 'apply_active_skills', load_skills),
                (m.lanying_agent_tools, 'filter_supported_client_functions', lambda _app, _config, fs: fs),
            ]:
                stack.enter_context(mock.patch.object(module, name, side_effect=replacement, create=True))
            for msg_type, supports_tools in (('CHAT', True), ('GROUPCHAT', True),
                                             ('CHAT', False), ('GROUPCHAT', False)):
                events.clear()
                result = m.handle_chat_message_with_config(
                    {'from_user_id': '1', 'to_user_id': '2'},
                    {'token_limit': 4000, 'function_call': supports_tools}, 'openai',
                    {'appId': 'app', 'content': 'question', 'type': msg_type},
                    {'model': 'model', 'max_tokens': 100}, {}, {}, 'default', {}, 0)
                self.assertEqual('test-stop', result['code'])
                self.assertEqual(['skills', 'retrieval', 'history'], events)

    def test_final_guard_checks_tools_but_does_not_send_margin_to_vendor(self):
        m = importlib.import_module('openai_service')
        with (
            mock.patch.object(m.lanying_vendor, 'get_chat_model_config',
                              return_value={'token_limit': 1000}, create=True),
            mock.patch.object(m.lanying_vendor, 'chat',
                              return_value={'result': 'ok', 'reply': 'ok'}, create=True) as chat,
            mock.patch.object(m, 'calcMessagesTokens', return_value=100),
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=800) as tools,
            mock.patch.object(m.lanying_config, 'get_message_too_long',
                              return_value='too long', create=True),
        ):
            preset = {'model': 'model', 'max_tokens': 100, 'messages': [],
                      'functions': [{'name': 'query'}]}
            result = m.chat_or_force_function_call('app', {}, 'vendor', {}, preset)
            self.assertEqual('message_too_long', result['code'])
            chat.assert_not_called()
            tools.return_value = 300
            result = m.chat_or_force_function_call('app', {}, 'vendor', {}, preset)
            self.assertEqual('ok', result['result'])
            self.assertEqual(preset, chat.call_args.args[3])

    def test_chat_and_group_return_tool_continuation_budget_error(self):
        m = importlib.import_module('openai_service')
        model_config = {'token_limit': 1000, 'vendor': 'openai'}
        initial = {'result': 'ok', 'reply': '', 'tool_calls': [{
            'id': 'call-1', 'type': 'function',
            'function': {'name': 'query', 'arguments': '{}'}}]}
        materials = types.ModuleType('lanying_seenical_materials')
        materials.retrieval_scope = lambda *_args: None

        def continue_tool(app_id, config, _call, preset, *_args, **_kwargs):
            return m.chat_or_force_function_call(app_id, config, 'openai', {}, preset)

        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}))
            for name, replacement in {
                'check_message_limit': lambda *_args: {'result': 'ok', 'api_key_type': 'share'},
                'get_preset_auth_info': lambda *_args: {},
                'get_system_functions': lambda *_args: [{'name': 'query'}],
                'make_metadata_from_msg': lambda *_args: {},
                'format_content_and_metadata': lambda *_args: ('question', {}),
                'calcMessagesTokens': lambda *_args: 900,
                'add_debug_message': lambda *_args, **_kwargs: None,
                'add_message_statistic': lambda *_args: None,
                'build_openclaw_reply_ext': lambda *_args: {},
                'loadHistory': lambda *_args: {'result': 'ok', 'data': []},
                'loadGroupHistory': lambda *_args: {'result': 'ok', 'data': []},
                'invoke_chat_model': lambda *args: m.ModelResponse(dict(initial), args[-1].preset, None),
                'handle_function_call': continue_tool,
            }.items():
                stack.enter_context(mock.patch.object(m, name, side_effect=replacement))
            for module, name, replacement in [
                (m.lanying_vendor, 'prepare_chat', lambda *_args: {}),
                (m.lanying_vendor, 'get_chat_model_config', lambda *_args: model_config),
                (m.lanying_config, 'get_message_too_long', lambda *_args: 'too long'),
                (m.lanying_redis, 'get_redis_connection', lambda: mock.Mock()),
                (m.lanying_embedding, 'get_preset_embedding_infos', lambda *_args: []),
                (m.lanying_embedding, 'calc_functions_tokens', lambda *_args: 10),
                (m.lanying_ai_plugin, 'get_preset_function_embeddings', lambda *_args: []),
                (m.lanying_agent_tools, 'apply_active_skills', lambda _a, _c, ms, fs: (ms, fs)),
                (m.lanying_agent_tools, 'filter_supported_client_functions', lambda _a, _c, fs: fs),
            ]:
                stack.enter_context(mock.patch.object(module, name, side_effect=replacement, create=True))
            build_reply = stack.enter_context(mock.patch.object(m, 'build_reply_result'))
            save_history = stack.enter_context(mock.patch.object(m, 'addHistory'))
            save_group = stack.enter_context(mock.patch.object(m.group_history_repository, 'save_pending_reply'))
            vendor_call = stack.enter_context(mock.patch.object(m.lanying_vendor, 'chat', create=True))
            for msg_type in ('CHAT', 'GROUPCHAT'):
                with self.subTest(msg_type=msg_type):
                    result = m.handle_chat_message_with_config(
                        {'from_user_id': '1', 'to_user_id': '2', 'send_from': '1'},
                        model_config, 'openai',
                        {'appId': 'app', 'content': 'question', 'type': msg_type},
                        {'model': 'model', 'max_tokens': 100}, {}, {}, 'default', {}, 0)
                    self.assertEqual({'result': 'error', 'code': 'message_too_long',
                                      'msg': 'too long'}, result)
            build_reply.assert_not_called()
            save_history.assert_not_called()
            save_group.assert_not_called()
            vendor_call.assert_not_called()

    def test_client_tool_resume_records_continuation_error_not_success(self):
        m = importlib.import_module('openai_service')
        error = {'result': 'error', 'code': 'message_too_long', 'msg': 'too long'}
        request = {
            'app_id': 'app', 'request_id': 'request-1', 'chatbot_id': 'bot',
            'tool_call': {'id': 'call-1'},
            'continuation': {
                'config': {'reply_msg_type': 'CHAT'},
                'preset': {'model': 'model', 'max_tokens': 100},
            },
        }
        with ExitStack() as stack:
            for name, value in {
                'check_message_deduct_failed': {'result': 'ok'},
                'check_message_limit': {'result': 'ok', 'api_key_type': 'share'},
                'get_preset_auth_info': {},
                'append_message': None,
                'invoke_chat_model': m.ModelResponse({'result': 'ok', 'reply': ''}, {}, None),
                'run_chat_tools': m.ToolRunResult(error, {'ai': {}}, 0, False, 0, [], []),
            }.items():
                stack.enter_context(mock.patch.object(m, name, return_value=value))
            for module, name, value in [
                (m.lanying_config, 'get_lanying_connector', {}),
                (m.lanying_chatbot, 'get_chatbot', {}),
                (m.lanying_vendor, 'get_chat_model_config', {'token_limit': 1000}),
                (m.lanying_vendor, 'prepare_chat', {}),
            ]:
                stack.enter_context(mock.patch.object(module, name, return_value=value, create=True))
            status = stack.enter_context(mock.patch.object(m.lanying_agent_tools, 'record_resume_status'))
            build_reply = stack.enter_context(mock.patch.object(m, 'build_reply_result'))
            send = stack.enter_context(mock.patch.object(m, 'replyMessageAsync'))
            save_history = stack.enter_context(mock.patch.object(m, 'addHistory'))
            m.resume_client_tool_request(request, {'result': 'ok'})
            self.assertEqual([
                mock.call('app', 'request-1', 'running'),
                mock.call('app', 'request-1', 'failed', 'too long'),
            ], status.call_args_list)
            build_reply.assert_not_called()
            save_history.assert_not_called()
            send.assert_called_once()
            self.assertEqual('error', send.call_args.args[2]['ai']['result'])
            self.assertEqual('request-1', send.call_args.args[2]['seenical_tool_request']['request_id'])

    def test_fallback_usage_does_not_include_context_headroom(self):
        m = importlib.import_module('openai_service')
        with (
            mock.patch.object(m, 'calcMessagesTokens', return_value=100),
            mock.patch.object(m, 'calcMessageTokens', return_value=20),
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=300),
            mock.patch.object(m.lanying_token_budget, 'input_budget',
                              side_effect=AssertionError('budget margin is not usage')),
        ):
            usage = m.build_fallback_usage_for_stat(
                'app', {'model': 'model', 'functions': [{'name': 'query'}]}, {'reply': 'answer'},
                {'type': 'chat', 'vendor': 'openai'})
            self.assertEqual(400, usage['prompt_tokens'])
            self.assertEqual(420, usage['total_tokens'])

    def test_fallback_completion_includes_calls_but_real_usage_wins(self):
        m = importlib.import_module('openai_service')
        calls = [{'id': 'c1', 'type': 'function',
                  'function': {'name': 'query', 'arguments': '{"text":"hello"}'}}]
        with (
            mock.patch.object(m, 'calcMessagesTokens', return_value=100),
            mock.patch.object(m, 'calcMessageTokens',
                              side_effect=lambda _app, msg, *_args: 200 if msg.get('tool_calls') else 10) as count,
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=300),
            mock.patch.object(m.lanying_vendor, 'get_chat_model_config',
                              return_value={'function_call': True}, create=True),
        ):
            preset = {'model': 'model', 'functions': [{'name': 'query'}]}
            response = {'reply': '', 'tool_calls': calls}
            usage = m.build_fallback_usage_for_stat(
                'app', preset, response, {'type': 'chat', 'vendor': 'openai'})
            self.assertEqual(600, usage['total_tokens'])
            streamed = m.stream_lines_to_response('app', preset, '', 'openai', {}, calls)
            self.assertEqual(usage, streamed['usage'])
            count.reset_mock()
            actual = {'prompt_tokens': 18, 'completion_tokens': 24, 'total_tokens': 42}
            streamed = m.stream_lines_to_response('app', preset, '', 'openai', actual, calls)
            self.assertEqual(actual, streamed['usage'])
            count.assert_not_called()

    def test_unsupported_tools_are_not_budgeted_or_estimated_as_usage(self):
        m = importlib.import_module('openai_service')
        cfg = {'token_limit': 100000, 'function_call': False,
               'model': 'claude-2.0', 'type': 'chat', 'vendor': 'claude'}
        preset = {'model': cfg['model'], 'max_tokens': 1024,
                  'functions': [{'name': 'query'}],
                  'messages': [{'role': 'user', 'content': 'current'}]}
        with (
            mock.patch.object(m.lanying_vendor, 'get_chat_model_config', return_value=cfg, create=True),
            mock.patch.object(m.lanying_vendor, 'chat',
                              return_value={'result': 'ok', 'reply': 'ok'}, create=True) as chat,
            mock.patch.object(m, 'calcMessagesTokens', return_value=88000),
            mock.patch.object(m, 'calcMessageTokens', return_value=20),
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens',
                              side_effect=AssertionError('unsupported schemas must not be counted')),
        ):
            result = m.chat_or_force_function_call('app', {}, 'claude', {}, preset)
            self.assertEqual('ok', result['result'])
            chat.assert_called_once()
            m.append_message('app', preset, cfg, {'role': 'tool', 'content': 'ok'})
            self.assertEqual([{'name': 'query'}], preset['functions'])
            usage = m.build_fallback_usage_for_stat('app', preset, {'reply': 'ok'}, cfg)
            self.assertEqual(88000, usage['prompt_tokens'])
            streamed = m.stream_lines_to_response('app', preset, 'ok', 'claude', {})
            self.assertEqual(usage, streamed['usage'])

    def test_separate_input_limit_applies_to_history_append_and_final_guard(self):
        m = importlib.import_module('openai_service')
        cfg = {'token_limit': 128000, 'input_token_limit': 96000,
               'model': 'model', 'vendor': 'vendor'}
        preset = {'model': 'model', 'max_tokens': 8192,
                  'messages': [{'role': 'user', 'content': 'current'}]}
        with (
            mock.patch.object(m, 'calcMessagesTokens', return_value=100000),
            mock.patch.object(m, 'calcMessageTokens', return_value=20),
            mock.patch.object(m.lanying_vendor, 'get_chat_model_config', return_value=cfg, create=True),
            mock.patch.object(m.lanying_vendor, 'chat', create=True) as chat,
            mock.patch.object(m.lanying_config, 'get_message_too_long',
                              return_value='too long', create=True),
        ):
            for loader in (m.loadHistory, m.loadGroupHistory):
                result = loader({'send_from': '1', 'reply_from': '2'}, 'app', None,
                                'key', 'question', [], 0, preset, {}, cfg, 'vendor')
                self.assertEqual('message_too_long', result['code'])
            result = m.chat_or_force_function_call('app', {}, 'vendor', {}, preset)
            self.assertEqual('message_too_long', result['code'])
            chat.assert_not_called()
            with self.assertRaisesRegex(ValueError, 'message_too_long'):
                m.append_message('app', preset, cfg, {'role': 'tool', 'content': 'ok'})

    def test_append_rejects_oversized_current_call_without_dropping_policy(self):
        m = importlib.import_module('openai_service')
        preset = {'model': 'model', 'max_tokens': 100, 'messages': [
            {'role': 'system', 'content': 'policy'},
            {'role': 'user', 'content': 'current'}]}
        message = {'role': 'assistant', 'content': None, 'tool_calls': [
            {'id': 'c', 'function': {'name': 'query', 'arguments': 'x' * 10000}}]}
        with (
            mock.patch.object(m, 'calcMessageTokens', return_value=10000),
            mock.patch.object(m, 'calcMessagesTokens', return_value=10100),
            mock.patch.object(m.lanying_embedding, 'calc_functions_tokens', return_value=0),
        ):
            with self.assertRaisesRegex(ValueError, 'message_too_long'):
                m.append_message('app', preset, {'model': 'model', 'vendor': 'v', 'token_limit': 1000}, message)
            self.assertEqual('policy', preset['messages'][0]['content'])
            self.assertEqual(message, preset['messages'][-1])

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
            mock.patch.object(m, '_record_quota_statistics', return_value=True),
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

    def test_small_context_model_switch_keeps_chat_and_group_history_usable(self):
        m = importlib.import_module('openai_service')
        compat = _load_real_openai_compat()
        openai_models = importlib.import_module('lanying_vendor_openai').model_configs()
        moonshot_models = importlib.import_module('lanying_vendor_moonshot').model_configs()
        for model in ('gpt-4', 'moonshot-v1-8k'):
            model_config = next(c for c in openai_models + moonshot_models
                                if c['model'] == model)
            saved = {'model': model, 'max_tokens': 8192}
            effective = compat.limit_completion_tokens(saved, model_config)
            with (
                self.subTest(model=model),
                mock.patch.object(m, 'calcMessagesTokens', return_value=20),
                mock.patch.object(m, 'calcMessageTokens', return_value=5),
                mock.patch.object(m, 'reversed_history_generator', return_value=iter([
                    {'user': 'old question', 'assistant': 'old answer'}])),
                mock.patch.object(m, 'reversed_group_history_generator', return_value=iter([
                    {'from': '100', 'content': 'old question'}])),
            ):
                chat = m.loadHistory(
                    {}, 'app', None, 'history', 'hello', [], 1,
                    effective, {}, model_config, 'openai')
                group = m.loadGroupHistory(
                    {'send_from': '100', 'reply_from': '101'},
                    'app', None, 'history', 'hello', [], 1,
                    effective, {}, model_config, 'openai')
            self.assertEqual('ok', chat['result'])
            self.assertEqual('ok', group['result'])
            self.assertTrue(list(chat['data']))
            self.assertTrue(list(group['data']))
            self.assertEqual(8192, saved['max_tokens'])

    def test_small_context_model_still_rejects_oversized_input(self):
        m = importlib.import_module('openai_service')
        config = next(c for c in importlib.import_module('lanying_vendor_openai').model_configs()
                      if c['model'] == 'gpt-4')
        preset = _load_real_openai_compat().limit_completion_tokens(
            {'model': 'gpt-4', 'max_tokens': 8192}, config)
        with (
            mock.patch.object(m, 'calcMessagesTokens', return_value=20),
            mock.patch.object(m, 'calcMessageTokens', return_value=config['token_limit']),
            mock.patch.object(m.lanying_config, 'get_message_too_long',
                              return_value='too long', create=True),
        ):
            for loader in (m.loadHistory, m.loadGroupHistory):
                result = loader(
                    {'send_from': '100', 'reply_from': '101'},
                    'app', None, 'history', 'long input', [], 1,
                    preset, {}, config, 'openai')
                self.assertEqual('message_too_long', result['code'])

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
