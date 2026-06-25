import importlib.util
import json
import pathlib
import sys
import threading
import time
import types
import unittest
from unittest import mock


def _fake_requests_call(*args, **kwargs):
    raise RuntimeError("fake requests module: please mock requests.request/post in tests")

def _safe_json_loads(raw, default=None):
    if not isinstance(raw, str):
        return default if default is not None else {}
    try:
        return json.loads(raw)
    except Exception:
        return default if default is not None else {}


def _load_lanying_openclaw():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw.py"
    module_name = "lanying_openclaw_router_identity_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: object(),
            redis_hget=lambda *args, **kwargs: "",
            redis_get=lambda *args, **kwargs: None,
            redis_hgetall=lambda *args, **kwargs: {},
        ),
        "lanying_config": types.SimpleNamespace(
            get_lanying_admin_token=lambda app_id: "admin-token",
            get_lanying_api_endpoint=lambda app_id: "https://api.example.com",
        ),
        "lanying_chatbot": types.SimpleNamespace(
            get_user_chatbot_id=lambda app_id, user_id: "chatbot-id",
            get_chatbot=lambda app_id, chatbot_id: {"user_id": "chatbot-user"},
        ),
        "lanying_im_api": types.SimpleNamespace(
            send_message_sync=lambda *args, **kwargs: 1,
            get_group_info=lambda *args, **kwargs: {"code": 200, "data": {}},
            set_group_ext=lambda *args, **kwargs: {"code": 200},
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=_safe_json_loads,
        ),
        "lanying_vendor": types.SimpleNamespace(),
        "lanying_pgvector": types.SimpleNamespace(
            append_openclaw_session_map_log=lambda entry: {"result": "ignored", "message": "test stub"},
        ),
        "requests": types.SimpleNamespace(
            post=_fake_requests_call,
            get=_fake_requests_call,
            request=_fake_requests_call,
        ),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module

def _load_lanying_openclaw_migration(openclaw_module):
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw_migration.py"
    module_name = "lanying_openclaw_migration_router_identity_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_openclaw": openclaw_module,
        "lanying_im_api": types.SimpleNamespace(
            admin_join_group_direct=lambda *args, **kwargs: {"code": 200},
            admin_add_group_admin=lambda *args, **kwargs: {"code": 200},
            group_owner_transfer=lambda *args, **kwargs: {"code": 200},
            admin_kick_group_member=lambda *args, **kwargs: {"code": 200},
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_openclaw = _load_lanying_openclaw()
lanying_openclaw_migration = _load_lanying_openclaw_migration(lanying_openclaw)

class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.sets = {}
        self.ttls = {}

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)
        self.hashes.pop(key, None)
        self.sets.pop(key, None)
        self.ttls.pop(key, None)
        return 1

    def rpush(self, key, *values):
        bucket = self.values.setdefault(key, [])
        if not isinstance(bucket, list):
            bucket = []
            self.values[key] = bucket
        bucket.extend(values)
        return len(bucket)

    def lrange(self, key, start, end):
        bucket = self.values.get(key, [])
        if not isinstance(bucket, list):
            return []
        normalized_end = None if int(end) == -1 else int(end) + 1
        return list(bucket[int(start):normalized_end])

    def ltrim(self, key, start, end):
        bucket = self.values.get(key, [])
        if not isinstance(bucket, list):
            self.values[key] = []
            return True
        normalized_end = None if int(end) == -1 else int(end) + 1
        self.values[key] = list(bucket[int(start):normalized_end])
        return True

    def expire(self, key, ttl):
        self.ttls[key] = ttl
        return True

    def pipeline(self):
        return FakeRedisPipeline(self)

    def hset(self, key, field, value):
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = value

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def setex(self, key, ttl, value):
        self.values[key] = value
        self.ttls[key] = ttl

    def sadd(self, key, *values):
        bucket = self.sets.setdefault(key, set())
        for value in values:
            bucket.add(value)

    def srem(self, key, *values):
        bucket = self.sets.setdefault(key, set())
        for value in values:
            bucket.discard(value)

    def smembers(self, key):
        return set(self.sets.get(key, set()))


class FakeRedisPipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def watch(self, key):
        self.watched_key = key

    def get(self, key):
        return self.redis.get(key)

    def multi(self):
        self.commands = []

    def delete(self, key):
        self.commands.append(("delete", key))

    def execute(self):
        for command, key in self.commands:
            if command == "delete":
                self.redis.delete(key)
        self.commands = []
        return True

    def unwatch(self):
        self.commands = []

    def reset(self):
        self.commands = []


class RouterSessionIdentityTests(unittest.TestCase):
    def setUp(self):
        self.default_redis = FakeRedis()
        self.redis_patcher = mock.patch.object(
            lanying_openclaw.lanying_redis,
            "get_redis_connection",
            return_value=self.default_redis,
        )
        self.redis_patcher.start()

    def tearDown(self):
        if hasattr(self, "redis_patcher"):
            self.redis_patcher.stop()

    def test_create_openclaw_session_group_updates_group_ext_async_after_success(self):
        m = lanying_openclaw
        fake_response = mock.Mock()
        fake_response.content = json.dumps({"code": 200, "data": {"group_id": "group-9"}}).encode("utf-8")

        with mock.patch.object(m.requests, "post", return_value=fake_response) as mocked_post, \
             mock.patch.object(m.executor, "submit", side_effect=lambda fn, *args, **kwargs: fn(*args, **kwargs)) as mocked_submit, \
             mock.patch.object(m.lanying_im_api, "set_group_ext", return_value={"code": 200}) as mocked_set_group_ext:
            group_id = m.create_openclaw_session_group(
                "app-id",
                "owner-user",
                "OpenClaw-15",
                "15",
                "agent:main:subagent:test-child",
                metadata={
                    "scene": "openclaw_session_group",
                    "peer_user_id": "peer-user",
                    "created_by_user_id": "owner-user",
                    "created_at": 1710000000000,
                    "name_source": "openclaw_session_group",
                    "peer_name_snapshot": "peer-user",
                    "session_key": "agent:main:subagent:test-child",
                    "root_session_key": "agent:main:clawchat-router:group:group-1",
                    "parent_session_key": "agent:main:clawchat-router:group:group-1",
                },
                log_context={
                    "node_id": "15",
                    "session_key": "agent:main:subagent:test-child",
                    "owner_user_id": "owner-user",
                },
            )

        self.assertEqual(group_id, "group-9")
        mocked_post.assert_called_once()
        mocked_submit.assert_called_once()
        mocked_set_group_ext.assert_called_once()
        ext_value = mocked_set_group_ext.call_args.args[2]
        self.assertEqual(
            json.loads(ext_value),
            {
                m.OPENCLAW_SESSION_GROUP_METADATA_KEY: {
                    "scene": "openclaw_session_group",
                    "peer_user_id": "peer-user",
                    "created_by_user_id": "owner-user",
                    "created_at": 1710000000000,
                    "name_source": "openclaw_session_group",
                    "peer_name_snapshot": "peer-user",
                    "session_key": "agent:main:subagent:test-child",
                    "root_session_key": "agent:main:clawchat-router:group:group-1",
                }
            },
        )

    def test_create_openclaw_session_group_does_not_call_set_group_ext_when_create_fails(self):
        m = lanying_openclaw
        fake_response = mock.Mock()
        fake_response.content = json.dumps({"code": 500, "message": "failed"}).encode("utf-8")

        with mock.patch.object(m.requests, "post", return_value=fake_response), \
             mock.patch.object(m.executor, "submit") as mocked_submit, \
             mock.patch.object(m.lanying_im_api, "set_group_ext") as mocked_set_group_ext:
            group_id = m.create_openclaw_session_group(
                "app-id",
                "owner-user",
                "OpenClaw-15",
                "15",
                "agent:main:subagent:test-child",
                metadata={"scene": "openclaw_session_group"},
            )

        self.assertEqual(group_id, "")
        mocked_submit.assert_not_called()
        mocked_set_group_ext.assert_not_called()

    def test_create_openclaw_session_group_metadata_failure_does_not_block_group_creation(self):
        m = lanying_openclaw
        fake_response = mock.Mock()
        fake_response.content = json.dumps({"code": 200, "data": {"group_id": "group-9"}}).encode("utf-8")

        with mock.patch.object(m.requests, "post", return_value=fake_response), \
             mock.patch.object(m.executor, "submit", side_effect=lambda fn, *args, **kwargs: fn(*args, **kwargs)), \
             mock.patch.object(m.lanying_im_api, "set_group_ext", return_value={"code": 500, "message": "failed"}):
            group_id = m.create_openclaw_session_group(
                "app-id",
                "owner-user",
                "OpenClaw-15",
                "15",
                "agent:main:subagent:test-child",
                metadata={"scene": "openclaw_session_group"},
            )

        self.assertEqual(group_id, "group-9")

    def test_legacy_router_session_key_is_normalized_to_clawchat_router(self):
        m = lanying_openclaw

        self.assertEqual(
            m.normalize_session_key("agent:main:router:group:6726580510113"),
            "agent:main:clawchat-router:group:6726580510113",
        )
        self.assertEqual(
            m.parse_clawchat_session_identity("agent:main:router:direct:6632092019520"),
            {
                "channel": "clawchat-router",
                "chat_type": "direct",
                "target_id": "6632092019520",
            },
        )

    def test_update_session_last_message_time_overwrites_with_current_time(self):
        m = lanying_openclaw
        redis = FakeRedis()
        key = m.get_openclaw_session_last_message_time_key(
            "app-id",
            "15",
        )
        field = m.get_openclaw_session_last_message_time_field("agent:main:clawchat:direct:user-1")
        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis):
            first = m.update_session_last_message_time(
                "app-id",
                "15",
                "agent:main:clawchat:direct:user-1",
                now_ms=2000,
            )
            second = m.update_session_last_message_time(
                "app-id",
                "15",
                "agent:main:clawchat:direct:user-1",
                now_ms=1000,
            )

        self.assertEqual(first, 2000)
        self.assertEqual(second, 1000)
        self.assertEqual(redis.hget(key, field), 1000)

    def test_handle_session_message_sync_event_updates_session_last_message_time(self):
        m = lanying_openclaw
        redis = FakeRedis()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m, "is_session_map_sync_enabled", return_value=True), \
             mock.patch.object(m, "time") as mocked_time:
            mocked_time.time.return_value = 1234.567
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:direct:user-1",
                    "visible_delivery_owner": "plugin",
                    "message": {
                        "role": "assistant",
                        "content": "already delivered by plugin",
                    },
                },
            )

        self.assertEqual(
            redis.hget(
                m.get_openclaw_session_last_message_time_key("app-id", "15"),
                m.get_openclaw_session_last_message_time_field(
                    "agent:main:clawchat:direct:user-1",
                ),
            ),
            1234567,
        )

    def test_render_canonical_html_includes_session_last_message_time(self):
        m = lanying_openclaw_migration
        details = [{
            "session_key": "agent:main:clawchat:direct:user-1",
            "app_id": "app-id",
            "node_id": "15",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_kind": "direct_user",
            "origin_user_id": "user-1",
            "chatbot_user_id": "chatbot-user",
            "group_id": "",
            "parent_session_key": "",
            "root_session_key": "agent:main:clawchat:direct:user-1",
            "effective_target_session_key": "agent:main:clawchat:direct:user-1",
        }]
        inspect_result = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:clawchat:direct:user-1",
                    "status": "clean",
                    "root_mode": "direct",
                    "target_user_id": "user-1",
                    "expected_fields": {},
                    "issues": [],
                    "proposed_changes": [],
                }]
            },
        }

        with mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=details), \
             mock.patch.object(m.lanying_openclaw, "get_session_last_message_time", return_value=1234567), \
             mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", return_value=inspect_result):
            html_text = m.render_inspect_session_mapping_canonical_html_for_node("app-id", "15")

        self.assertIn("last_message_time", html_text)
        self.assertIn("1234567 (1970-01-01 08:20:34)", html_text)

    def test_render_inspect_session_mapping_canonical_html_for_node_highlights_origin_identity(self):
        m = lanying_openclaw_migration
        details = [{
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "app_id": "uioczdkuvci",
            "node_id": "8",
            "openclaw_user_id": "6760921908880",
            "management_user_id": "6632092019520",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "6674822238512",
            "group_id": "6632098115105",
            "parent_session_key": "",
            "root_session_key": "agent:main:clawchat-router:direct:6632092019520",
            "effective_target_session_key": "agent:main:clawchat-router:direct:6632092019520",
        }]
        inspect_result = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:clawchat-router:direct:6632092019520",
                    "status": "dirty",
                    "root_mode": "router_direct",
                    "target_user_id": "6632092019520",
                    "expected_fields": {
                        "origin_kind": "direct_user",
                        "origin_user_id": "6632092019520",
                    },
                    "issues": [
                        {
                            "severity": "error",
                            "code": "direct_root_origin_kind_mismatch",
                            "message": "direct root lineage 的 origin_kind 与当前规则不一致",
                        },
                        {
                            "severity": "error",
                            "code": "direct_root_origin_user_mismatch",
                            "message": "direct root lineage 的 origin_user_id 与当前规则不一致",
                        },
                    ],
                    "proposed_changes": [],
                }]
            },
        }

        with mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=details), \
             mock.patch.object(m.lanying_openclaw, "get_session_last_message_time", return_value=1234567), \
             mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", return_value=inspect_result):
            html_text = m.render_inspect_session_mapping_canonical_html_for_node("app-id", "8")

        self.assertIn("origin_identity", html_text)
        self.assertIn("current_origin_kind", html_text)
        self.assertIn("openclaw_control", html_text)
        self.assertIn("expected_origin_kind", html_text)
        self.assertIn("direct_user", html_text)
        self.assertIn("expected_origin_user_id", html_text)
        self.assertIn("6632092019520", html_text)
        self.assertIn("origin_repair_reason", html_text)
        self.assertIn("direct root lineage inferred from root_session_key", html_text)
        self.assertIn("last_message_time", html_text)
        self.assertIn("1234567 (1970-01-01 08:20:34)", html_text)

    def test_legacy_agent_main_clawchat_group_and_direct_session_keys_are_canonicalized(self):
        m = lanying_openclaw

        self.assertEqual(
            m.normalize_session_key("agent:main:group:6726580510113"),
            "agent:main:clawchat:group:6726580510113",
        )
        self.assertEqual(
            m.normalize_session_key("agent:main:6632092019520"),
            "agent:main:clawchat:direct:6632092019520",
        )
        self.assertEqual(
            m.normalize_session_key("agent:main:6597711675232"),
            "agent:main:clawchat:direct:6597711675232",
        )
        self.assertEqual(
            m.parse_clawchat_session_identity("agent:main:group:6726580510113"),
            {
                "channel": "clawchat",
                "chat_type": "group",
                "target_id": "6726580510113",
            },
        )
        self.assertEqual(
            m.parse_clawchat_session_identity("agent:main:6632092019520"),
            {
                "channel": "clawchat",
                "chat_type": "direct",
                "target_id": "6632092019520",
            },
        )
        self.assertEqual(
            m.parse_clawchat_session_identity("agent:main:6597711675232"),
            {
                "channel": "clawchat",
                "chat_type": "direct",
                "target_id": "6597711675232",
            },
        )

    def test_rewrite_session_mapping_for_migration_allows_rebinding_existing_session_group(self):
        m = lanying_openclaw
        redis = FakeRedis()
        session_key = "agent:main:clawchat-router:direct:6632092019520"
        old_group_id = "legacy-group"
        new_mapping = {
            "session_key": session_key,
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "manager-1",
            "origin_kind": "direct_user",
            "origin_user_id": "6632092019520",
            "chatbot_user_id": "chatbot-user",
            "group_id": "",
            "root_session_key": session_key,
            "effective_target_session_key": session_key,
        }
        old_mapping = dict(new_mapping, group_id=old_group_id, origin_kind="openclaw_control", origin_user_id="", created_at=123)
        old_body = json.dumps(old_mapping, ensure_ascii=False)
        redis.set(m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", session_key), old_body)
        redis.set(m.get_openclaw_session_mapping_by_group_key("app-id", "node-1", "openclaw-user", old_group_id), old_body)
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda _redis, key: redis.get(key)), \
             mock.patch.object(m, "record_session_mapping_change_async") as mocked_log:
            result = m.rewrite_session_mapping_for_migration("app-id", "node-1", new_mapping)
            saved = m.get_session_mapping_by_session("app-id", "node-1", session_key)
            old_group_mapping = m.get_session_mapping_by_group("app-id", "node-1", "openclaw-user", old_group_id)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(saved["group_id"], "")
        self.assertEqual(saved["origin_kind"], "direct_user")
        self.assertEqual(saved["origin_user_id"], "6632092019520")
        self.assertIsNone(old_group_mapping)
        mocked_log.assert_called_once()

    def test_rewrite_session_mapping_for_migration_still_rejects_group_bound_to_another_session(self):
        m = lanying_openclaw
        redis = FakeRedis()
        target_session_key = "agent:main:clawchat-router:group:group-2"
        conflict_session_key = "agent:main:clawchat-router:group:group-1"
        target_group_id = "group-2"
        target_mapping = {
            "session_key": target_session_key,
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "manager-1",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "group_id": "wrong-group",
            "root_session_key": target_session_key,
            "effective_target_session_key": target_session_key,
        }
        conflict_mapping = dict(target_mapping, session_key=conflict_session_key, group_id=target_group_id)
        redis.set(m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", target_session_key), json.dumps(target_mapping, ensure_ascii=False))
        redis.set(m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", conflict_session_key), json.dumps(conflict_mapping, ensure_ascii=False))
        redis.set(m.get_openclaw_session_mapping_by_group_key("app-id", "node-1", "openclaw-user", target_group_id), json.dumps(conflict_mapping, ensure_ascii=False))
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), target_session_key, conflict_session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda _redis, key: redis.get(key)):
            result = m.rewrite_session_mapping_for_migration(
                "app-id",
                "node-1",
                dict(target_mapping, group_id=target_group_id),
            )

        self.assertEqual(result["result"], "error")
        self.assertEqual(result["message"], "group already bind to another session")

    def test_rewrite_session_mapping_for_migration_deletes_old_group_lookup_when_openclaw_user_changes(self):
        m = lanying_openclaw
        redis = FakeRedis()
        session_key = "agent:main:clawchat-router:group:group-2"
        group_id = "group-2"
        new_mapping = {
            "session_key": session_key,
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "new-openclaw-user",
            "management_user_id": "manager-1",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "group_id": group_id,
            "root_session_key": session_key,
            "effective_target_session_key": session_key,
        }
        old_mapping = dict(new_mapping, openclaw_user_id="old-openclaw-user", created_at=123)
        old_body = json.dumps(old_mapping, ensure_ascii=False)
        redis.set(m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", session_key), old_body)
        redis.set(m.get_openclaw_session_mapping_by_group_key("app-id", "node-1", "old-openclaw-user", group_id), old_body)
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda _redis, key: redis.get(key)):
            result = m.rewrite_session_mapping_for_migration("app-id", "node-1", new_mapping)
            saved = m.get_session_mapping_by_session("app-id", "node-1", session_key)
            old_group_mapping = m.get_session_mapping_by_group("app-id", "node-1", "old-openclaw-user", group_id)
            new_group_mapping = m.get_session_mapping_by_group("app-id", "node-1", "new-openclaw-user", group_id)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(saved["openclaw_user_id"], "new-openclaw-user")
        self.assertIsNone(old_group_mapping)
        self.assertEqual(new_group_mapping["openclaw_user_id"], "new-openclaw-user")

    def test_non_clawchat_session_key_remains_unchanged(self):
        m = lanying_openclaw

        self.assertEqual(m.normalize_session_key("legacy-user"), "legacy-user")
        self.assertIsNone(m.parse_clawchat_session_identity("legacy-user"))

    def test_legacy_router_session_mapping_record_is_canonicalized(self):
        m = lanying_openclaw

        normalized = m.normalize_session_mapping_record({
            "session_key": "agent:main:router:group:6726580510113",
            "parent_session_key": "agent:main:router:group:6726580510113",
            "root_session_key": "agent:main:router:group:6726580510113",
            "effective_target_session_key": "agent:main:router:direct:6632092019520",
        })

        self.assertEqual(normalized["session_key"], "agent:main:clawchat-router:group:6726580510113")
        self.assertEqual(normalized["parent_session_key"], "agent:main:clawchat-router:group:6726580510113")
        self.assertEqual(normalized["root_session_key"], "agent:main:clawchat-router:group:6726580510113")
        self.assertEqual(
            normalized["effective_target_session_key"],
            "agent:main:clawchat-router:direct:6632092019520",
        )

    def test_legacy_agent_main_clawchat_session_mapping_record_is_canonicalized(self):
        m = lanying_openclaw

        normalized = m.normalize_session_mapping_record({
            "session_key": "agent:main:group:6726580510113",
            "parent_session_key": "agent:main:group:6726580510113",
            "root_session_key": "agent:main:group:6726580510113",
            "effective_target_session_key": "agent:main:6597711675232",
        })

        self.assertEqual(normalized["session_key"], "agent:main:clawchat:group:6726580510113")
        self.assertEqual(normalized["parent_session_key"], "agent:main:clawchat:group:6726580510113")
        self.assertEqual(normalized["root_session_key"], "agent:main:clawchat:group:6726580510113")
        self.assertEqual(
            normalized["effective_target_session_key"],
            "agent:main:clawchat:direct:6597711675232",
        )

    def test_session_key_facts_expose_canonical_and_legacy_flags(self):
        m = lanying_openclaw

        legacy_facts = m.get_session_key_facts("agent:main:router:group:group-1")
        canonical_facts = m.get_session_key_facts("agent:main:clawchat:direct:12345")
        subagent_facts = m.get_session_key_facts("agent:main:subagent:test-child")

        self.assertEqual(legacy_facts["canonical_session_key"], "agent:main:clawchat-router:group:group-1")
        self.assertTrue(legacy_facts["is_legacy_alias"])
        self.assertTrue(legacy_facts["is_router"])
        self.assertTrue(legacy_facts["is_group"])
        self.assertEqual(canonical_facts["channel"], "clawchat")
        self.assertTrue(canonical_facts["is_direct"])
        self.assertFalse(canonical_facts["is_legacy_alias"])
        self.assertTrue(subagent_facts["is_subagent"])
        self.assertFalse(subagent_facts["is_clawchat_session"])

    def test_get_session_mapping_by_session_reads_legacy_storage_and_converges_to_canonical(self):
        m = lanying_openclaw
        redis = FakeRedis()
        old_session_key = "agent:main:router:group:group-1"
        canonical_session_key = "agent:main:clawchat-router:group:group-1"
        raw_mapping = {
            "session_key": old_session_key,
            "group_id": "group-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }
        redis.set(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            json.dumps(raw_mapping),
        )
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), old_session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda r, key: r.values.get(key)):
            mapping = m.get_session_mapping_by_session("app-id", "node-1", canonical_session_key)

        self.assertEqual(mapping["session_key"], canonical_session_key)
        self.assertNotIn(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            redis.values,
        )
        self.assertIn(
            m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", canonical_session_key),
            redis.values,
        )
        self.assertIn(canonical_session_key, redis.smembers(m.get_openclaw_session_mapping_index_key("app-id", "node-1")))
        self.assertNotIn(old_session_key, redis.smembers(m.get_openclaw_session_mapping_index_key("app-id", "node-1")))

    def test_converge_session_mapping_record_logs_async_change(self):
        m = lanying_openclaw
        redis = FakeRedis()
        legacy_mapping = {
            "session_key": "agent:main:router:group:group-1",
            "group_id": "group-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "record_session_mapping_change_async") as mocked_log:
            result = m.converge_session_mapping_record(
                redis,
                "app-id",
                "node-1",
                legacy_mapping,
                legacy_session_keys=["agent:main:router:group:group-1"],
            )

        self.assertEqual(result["session_key"], "agent:main:clawchat-router:group:group-1")
        mocked_log.assert_called_once()
        self.assertEqual(mocked_log.call_args.args[4], "read_time_converge")

    def test_get_session_mapping_by_session_does_not_overwrite_conflicting_canonical_mapping(self):
        m = lanying_openclaw
        redis = FakeRedis()
        old_session_key = "agent:main:router:group:group-1"
        canonical_session_key = "agent:main:clawchat-router:group:group-1"
        legacy_mapping = {
            "session_key": old_session_key,
            "group_id": "group-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }
        canonical_mapping = {
            "session_key": canonical_session_key,
            "group_id": "group-2",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }
        redis.set(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            json.dumps(legacy_mapping),
        )
        redis.set(
            m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", canonical_session_key),
            json.dumps(canonical_mapping),
        )
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), old_session_key, canonical_session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda r, key: r.values.get(key)):
            mapping = m.get_session_mapping_by_session("app-id", "node-1", canonical_session_key)

        self.assertEqual(mapping["group_id"], "group-2")
        self.assertIn(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            redis.values,
        )
        self.assertEqual(
            json.loads(redis.values[m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", canonical_session_key)])["group_id"],
            "group-2",
        )

    def test_list_session_mappings_for_node_converges_legacy_index_entries_without_duplicates(self):
        m = lanying_openclaw
        redis = FakeRedis()
        old_session_key = "agent:main:group:group-9"
        canonical_session_key = "agent:main:clawchat:group:group-9"
        raw_mapping = {
            "session_key": old_session_key,
            "group_id": "group-9",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }
        redis.set(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            json.dumps(raw_mapping),
        )
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), old_session_key, canonical_session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda r, key: r.values.get(key)):
            mappings = m.list_session_mappings_for_node("app-id", "node-1")

        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]["session_key"], canonical_session_key)
        self.assertIn(
            m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", canonical_session_key),
            redis.values,
        )

    def test_set_session_mapping_logs_async_only_for_material_change(self):
        m = lanying_openclaw
        redis = FakeRedis()
        mapping = {
            "session_key": "agent:main:router:group:group-1",
            "group_id": "group-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "get_session_mapping_by_group", return_value=None), \
             mock.patch.object(m, "record_session_mapping_change_async") as mocked_log:
            result = m.set_session_mapping("app-id", "node-1", mapping)

        self.assertEqual(result["result"], "ok")
        mocked_log.assert_called_once()
        self.assertEqual(mocked_log.call_args.args[4], "set_session_mapping")

        existing_body = dict(result["data"])
        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=existing_body), \
             mock.patch.object(m, "get_session_mapping_by_group", return_value=existing_body), \
             mock.patch.object(m, "record_session_mapping_change_async") as mocked_same_log:
            same_result = m.set_session_mapping("app-id", "node-1", existing_body)

        self.assertEqual(same_result["result"], "ok")
        mocked_same_log.assert_called_once()

    def test_record_session_mapping_change_async_skips_executor_for_same_signature(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-1",
            "group_id": "group-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "updated_at": 1,
            "created_at": 1,
        }

        with mock.patch.object(m.executor, "submit") as mocked_submit:
            m.record_session_mapping_change_async(
                "app-id",
                "node-1",
                dict(mapping),
                dict(mapping),
                "set_session_mapping",
            )

        mocked_submit.assert_not_called()

    def test_list_session_mapping_details_for_node_skips_group_lookups_for_non_group_mapping(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat:direct:user-1",
            "group_id": "",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "origin-user",
            "chatbot_user_id": "chatbot-user",
        }

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.lanying_im_api, "get_group_info") as mocked_group_info, \
             mock.patch.object(m, "get_group_member_list_for_group_admin") as mocked_member_list, \
             mock.patch.object(m, "get_group_admin_list_for_group_admin") as mocked_admin_list:
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["session_key"], "agent:main:clawchat:direct:user-1")
        self.assertEqual(details[0]["group_info"], {"group_id": "", "owner_id": ""})
        self.assertEqual(details[0]["member_summary"]["members"], [])
        self.assertEqual(details[0]["member_summary"]["member_count_loaded"], 0)
        self.assertEqual(details[0]["key_user_status"]["openclaw_user_id"]["present_in_group"], False)
        mocked_group_info.assert_not_called()
        mocked_member_list.assert_not_called()
        mocked_admin_list.assert_not_called()

    def test_list_session_mapping_details_for_node_aggregates_group_summary_and_key_users(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "origin-user",
            "chatbot_user_id": "chatbot-user",
        }
        member_pages = [
            {
                "code": 200,
                "data": [
                    {"user_id": "openclaw-user", "display_name": "OpenClaw", "join_time": 11, "expired_time": 0},
                    {"user_id": "management-user", "display_name": "Manager", "join_time": 12, "expired_time": 0},
                ],
                "cursor": "page-2",
            },
            {
                "code": 200,
                "data": [
                    {"user_id": "origin-user", "display_name": "Origin", "join_time": 13, "expired_time": 0},
                ],
                "cursor": "",
            },
        ]
        admin_list = {
            "code": 200,
            "data": [
                {"user_id": "management-user", "display_name": "Manager", "join_time": 12, "expired_time": 0},
            ],
        }
        group_info = {
            "code": 200,
            "data": {
                "group_id": "group-9",
                "name": "Group Nine",
                "owner_id": "management-user",
                "count": 3,
            },
        }

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.lanying_im_api, "get_group_info", return_value=group_info) as mocked_group_info, \
             mock.patch.object(m, "get_group_member_list_for_group_admin", side_effect=member_pages) as mocked_member_list, \
             mock.patch.object(m, "get_group_admin_list_for_group_admin", return_value=admin_list) as mocked_admin_list:
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertEqual(len(details), 1)
        detail = details[0]
        self.assertEqual(detail["group_info"]["group_id"], "group-9")
        self.assertEqual(detail["group_info"]["name"], "Group Nine")
        self.assertEqual(detail["member_summary"]["member_count_reported"], 3)
        self.assertEqual(detail["member_summary"]["member_count_loaded"], 3)
        self.assertTrue(detail["member_summary"]["members_loaded_complete"])
        self.assertEqual(detail["member_summary"]["members"][0]["user_id"], "openclaw-user")
        self.assertTrue(detail["key_user_status"]["openclaw_user_id"]["present_in_group"])
        self.assertTrue(detail["key_user_status"]["management_user_id"]["is_group_owner"])
        self.assertFalse(detail["key_user_status"]["chatbot_user_id"]["present_in_group"])
        self.assertEqual(detail["key_user_status"]["management_user_id"]["admin_status"], "admin")
        self.assertEqual(detail["key_user_status"]["origin_user_id"]["admin_status"], "not_admin")
        self.assertEqual(detail["group_info_error"], "")
        self.assertEqual(detail["member_list_error"], "")
        self.assertEqual(detail["admin_list_error"], "")
        self.assertEqual(detail["member_list_viewer_user_id"], "")
        self.assertEqual(detail["admin_list_viewer_user_id"], "")
        mocked_group_info.assert_called_once_with("app-id", "group-9")
        self.assertEqual(mocked_member_list.call_count, 2)
        self.assertEqual(mocked_member_list.call_args_list[0].args, ("app-id", "group-9", "", 500))
        mocked_admin_list.assert_called_once_with("app-id", "group-9")

    def test_list_session_mapping_details_for_node_reuses_group_lookup_for_same_group(self):
        m = lanying_openclaw
        mappings = [
            {
                "session_key": "agent:main:clawchat:group:group-9",
                "group_id": "group-9",
                "openclaw_user_id": "openclaw-user",
                "management_user_id": "management-user",
            },
            {
                "session_key": "agent:main:subagent:group-child",
                "group_id": "group-9",
                "openclaw_user_id": "openclaw-user",
                "management_user_id": "management-user",
            },
        ]

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=mappings), \
             mock.patch.object(m.lanying_im_api, "get_group_info", return_value={"code": 200, "data": {"group_id": "group-9", "owner_id": "management-user"}}) as mocked_group_info, \
             mock.patch.object(m, "get_group_member_list_for_group_admin", return_value={"code": 200, "data": [], "cursor": ""}) as mocked_member_list, \
             mock.patch.object(m, "get_group_admin_list_for_group_admin", return_value={"code": 200, "data": []}) as mocked_admin_list:
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertEqual(len(details), 2)
        mocked_group_info.assert_called_once_with("app-id", "group-9")
        mocked_member_list.assert_called_once_with("app-id", "group-9", "", 500)
        mocked_admin_list.assert_called_once_with("app-id", "group-9")

    def test_list_session_mapping_details_for_node_marks_member_list_incomplete_on_failure(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.lanying_im_api, "get_group_info", return_value={"code": 200, "data": {"group_id": "group-9", "owner_id": ""}}), \
             mock.patch.object(m, "get_group_member_list_for_group_admin", side_effect=[
                 {"code": 200, "data": [{"user_id": "openclaw-user", "display_name": "OpenClaw", "join_time": 11, "expired_time": 0}], "cursor": "next"},
                 {"code": 500, "message": "boom"},
             ]), \
             mock.patch.object(m, "get_group_admin_list_for_group_admin", return_value={"code": 200, "data": []}):
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertFalse(details[0]["member_summary"]["members_loaded_complete"])
        self.assertEqual(details[0]["member_summary"]["member_count_loaded"], 1)
        self.assertEqual(details[0]["member_list_error"], "boom")

    def test_list_session_mapping_details_for_node_marks_admin_status_unknown_when_admin_list_fails(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.lanying_im_api, "get_group_info", return_value={"code": 200, "data": {"group_id": "group-9", "owner_id": "someone-else"}}), \
             mock.patch.object(m, "get_group_member_list_for_group_admin", return_value={
                 "code": 200,
                 "data": [{"user_id": "openclaw-user", "display_name": "OpenClaw", "join_time": 11, "expired_time": 0}],
                 "cursor": "",
             }), \
             mock.patch.object(m, "get_group_admin_list_for_group_admin", return_value={"code": 402, "message": "Operation rejected"}):
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertEqual(details[0]["admin_list_error"], "Operation rejected")
        self.assertEqual(details[0]["key_user_status"]["openclaw_user_id"]["admin_status"], "unknown")

    def test_list_session_mapping_details_for_node_marks_group_info_error_when_group_info_fails(self):
        m = lanying_openclaw
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.lanying_im_api, "get_group_info", return_value={"code": 404, "message": "group not found"}), \
             mock.patch.object(m, "get_group_member_list_for_group_admin", return_value={
                 "code": 200,
                 "data": [{"user_id": "openclaw-user", "display_name": "OpenClaw", "join_time": 11, "expired_time": 0}],
                 "cursor": "",
             }), \
             mock.patch.object(m, "get_group_admin_list_for_group_admin", return_value={"code": 200, "data": []}):
            details = m.list_session_mapping_details_for_node("app-id", "node-1")

        self.assertEqual(details[0]["group_info"], {"group_id": "group-9", "owner_id": ""})
        self.assertEqual(details[0]["group_info_error"], "group not found")

    def test_render_inspect_session_mapping_group_states_html_for_node_renders_nested_tables(self):
        m = lanying_openclaw_migration
        details = [{
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "origin-<user>",
            "chatbot_user_id": "chatbot-user",
            "parent_session_key": "parent-session",
            "root_session_key": "root-session",
            "effective_target_session_key": "target-session",
            "created_at": 122,
            "updated_at": 123,
            "group_info": {
                "group_id": "group-9",
                "name": "Group <Nine>",
                "owner_id": "management-user",
                "count": 3,
                "created_at": 456000,
                "updated_at": 789000,
            },
            "member_summary": {
                "member_count_reported": 3,
                "member_count_loaded": 2,
                "members_loaded_complete": False,
                "members": [
                    {
                        "user_id": "openclaw-user",
                        "display_name": "OpenClaw <Admin>",
                        "join_time": 11,
                        "expired_time": 0,
                    }
                ],
            },
            "key_user_status": {
                "openclaw_user_id": {
                    "user_id": "openclaw-user",
                    "present_in_group": True,
                    "is_group_owner": False,
                    "admin_status": "admin",
                }
            },
            "group_info_error": "",
            "member_list_error": "partial <error>",
            "admin_list_error": "",
            "member_list_viewer_user_id": "",
            "admin_list_viewer_user_id": "",
        }]
        inspect_result = {"result": "ok", "data": {"mapping_reports": [{
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "root_mode": "clawchat_group",
            "status": "dirty",
            "current_owner_user_id": "management-user",
            "expected_owner_user_id": "openclaw-user",
            "issues": [{
                "severity": "warning",
                "code": "unexpected_group_owner",
                "summary": "Group <Nine> owner mismatch",
                "current": "management-user",
                "expected": "openclaw-user",
            }],
            "proposed_changes": [{
                "action": "group_owner_transfer_review",
                "target_type": "group_relation",
                "group_id": "group-9",
                "from": "management-user",
                "to": "openclaw-user",
                "reason": "OpenClaw <Admin> should be reviewed",
                "risk": "high",
            }],
        }]}}

        with mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details), \
             mock.patch.object(m, "inspect_session_mapping_group_states_for_node", return_value=inspect_result):
            html_text = m.render_inspect_session_mapping_group_states_html_for_node("app-id", "node-1")

        self.assertIn("<table", html_text)
        self.assertIn("session_key", html_text)
        self.assertIn("session_facts", html_text)
        self.assertIn("canonical_session_key", html_text)
        self.assertIn("member_summary", html_text)
        self.assertIn("key_user_status", html_text)
        self.assertIn("proposed_changes", html_text)
        self.assertIn("created_at", html_text)
        self.assertIn("updated_at", html_text)
        self.assertIn("122 (1970-01-01 08:02:02)", html_text)
        self.assertIn("456000 (1970-01-01 08:07:36)", html_text)
        self.assertIn("unexpected_group_owner", html_text)
        self.assertIn("Group &lt;Nine&gt; owner mismatch", html_text)
        self.assertIn("OpenClaw &lt;Admin&gt;", html_text)
        self.assertIn("group_owner_transfer_review", html_text)
        self.assertGreaterEqual(html_text.count("<table"), 5)

    def test_inspect_session_mapping_group_states_for_node_reports_repairs(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:main",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "origin-user",
            "chatbot_user_id": "",
            "root_session_key": "agent:main:main",
            "group_info": {"group_id": "group-9", "owner_id": "someone-else", "type": 3},
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
                "origin_user_id": {"user_id": "origin-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
                "chatbot_user_id": {"user_id": "", "present_in_group": False, "is_group_owner": False, "admin_status": "unknown"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["dirty_mapping_count"], 1)
        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["status"], "dirty")
        self.assertEqual(report["expected_owner_user_id"], "openclaw-user")
        self.assertTrue(any(change["action"] == "group_member_add" and change["user_id"] == "openclaw-user" for change in report["proposed_changes"]))
        self.assertTrue(any(change["action"] == "group_member_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))
        self.assertTrue(any(change["action"] == "group_admin_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))
        self.assertTrue(any(change["action"] == "group_owner_transfer" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_requires_management_admin_fix_for_clawchat_regular_group(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "root_session_key": "agent:main:clawchat:group:source-group",
            "group_info": {"group_id": "group-9", "owner_id": "openclaw-user", "type": 0},
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": True, "is_group_owner": True, "admin_status": "admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": True, "is_group_owner": False, "admin_status": "not_admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value=""), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["status"], "dirty")
        self.assertTrue(any(change["action"] == "group_admin_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_does_not_prejoin_management_user_for_clawchat_regular_group(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "root_session_key": "agent:main:clawchat:group:source-group",
            "group_info": {"group_id": "group-9", "owner_id": "openclaw-user", "type": 0},
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": True, "is_group_owner": True, "admin_status": "admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value=""), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["status"], "clean")
        self.assertFalse(any(change["action"] == "group_member_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))
        self.assertFalse(any(change["action"] == "group_admin_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_requires_management_admin_fix_for_clawchat_temporary_group(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat-router:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "group_info": {"group_id": "group-9", "owner_id": "chatbot-user", "type": 3},
            "key_user_status": {
                "management_user_id": {"user_id": "management-user", "present_in_group": True, "is_group_owner": False, "admin_status": "not_admin"},
                "chatbot_user_id": {"user_id": "chatbot-user", "present_in_group": True, "is_group_owner": True, "admin_status": "admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["status"], "dirty")
        self.assertTrue(any(issue["code"] == "missing_management_group_admin" for issue in report["issues"]))
        self.assertTrue(any(change["action"] == "group_admin_add" and change["user_id"] == "management-user" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_requires_owner_transfer_and_old_owner_leave_for_router_temporary_group(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat-router:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "origin-user",
            "chatbot_user_id": "chatbot-user",
            "root_session_key": "agent:main:clawchat-router:group:source-group",
            "group_info": {"group_id": "group-9", "owner_id": "openclaw-user", "type": 3},
            "key_user_status": {
                "chatbot_user_id": {"user_id": "chatbot-user", "present_in_group": True, "is_group_owner": False, "admin_status": "admin"},
                "origin_user_id": {"user_id": "origin-user", "present_in_group": True, "is_group_owner": False, "admin_status": "not_admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["current_owner_user_id"], "openclaw-user")
        self.assertEqual(report["expected_owner_user_id"], "chatbot-user")
        self.assertTrue(any(change["action"] == "group_owner_transfer" and change["to"] == "chatbot-user" for change in report["proposed_changes"]))
        self.assertTrue(any(change["action"] == "group_member_remove" and change["user_id"] == "openclaw-user" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_router_group_root_falls_back_to_session_key_when_root_missing(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat-router:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "root_session_key": "",
            "group_info": {"group_id": "group-9", "owner_id": "chatbot-user", "type": 0},
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": True, "is_group_owner": False, "admin_status": "admin"},
                "chatbot_user_id": {"user_id": "chatbot-user", "present_in_group": True, "is_group_owner": True, "admin_status": "admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertEqual(report["root_mode"], "router_group")
        self.assertEqual(report["current_owner_user_id"], "chatbot-user")
        self.assertEqual(report["expected_owner_user_id"], "")
        self.assertFalse(any(issue["code"] == "missing_required_member" and issue["current"].get("user_id") == "openclaw-user" for issue in report["issues"]))
        self.assertFalse(any(change["action"] == "group_member_add" and change["user_id"] == "openclaw-user" for change in report["proposed_changes"]))

    def test_inspect_session_mapping_group_states_for_node_router_group_bound_chatbot_in_group_does_not_require_member_add(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        details = [{
            "session_key": "agent:main:clawchat-router:group:group-9",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "group_info": {"group_id": "group-9", "owner_id": "owner-user", "type": 0},
            "member_summary": {
                "member_count_reported": 3,
                "member_count_loaded": 3,
                "members_loaded_complete": True,
                "members": [
                    {"user_id": "owner-user"},
                    {"user_id": "chatbot-user"},
                    {"user_id": "management-user"},
                ],
            },
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": False, "is_group_owner": False, "admin_status": "not_admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": True, "is_group_owner": False, "admin_status": "not_admin"},
                "chatbot_user_id": {"user_id": "", "present_in_group": False, "is_group_owner": False, "admin_status": "unknown"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }]

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node", return_value=details):
            result = m.inspect_session_mapping_group_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        self.assertTrue(any(change["action"] == "mapping_field_update" and change["field"] == "chatbot_user_id" and change["to"] == "chatbot-user" for change in report["proposed_changes"]))
        self.assertFalse(any(issue["code"] == "missing_required_member" and issue["current"].get("user_id") == "chatbot-user" for issue in report["issues"]))
        self.assertFalse(any(change["action"] == "group_member_add" and change["user_id"] == "chatbot-user" for change in report["proposed_changes"]))

    def test_migrate_inspected_session_mapping_group_state_applies_changes_and_returns_before_after(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        before_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:test-child",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "mapping_field_update", "field": "chatbot_user_id", "to": "chatbot-user"},
                        {"action": "group_owner_transfer", "group_id": "group-9", "from": "old-owner", "to": "openclaw-user"},
                    ],
                }]
            }
        }
        after_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:test-child",
                    "status": "clean",
                    "proposed_changes": [],
                }]
            }
        }

        with mock.patch.object(m, "inspect_session_mapping_group_state_for_session", side_effect=[before_inspect, after_inspect]), \
             mock.patch.object(m, "render_inspect_session_mapping_group_state_html_for_session", side_effect=["before-html", "after-html"]), \
             mock.patch.object(m, "_apply_group_state_change", side_effect=[{"result": "ok"}, {"result": "ok"}]) as mocked_apply:
            result = m.migrate_inspected_session_mapping_group_state("app-id", node_info, "agent:main:subagent:test-child")

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["before_report"]["status"], "dirty")
        self.assertEqual(result["data"]["after_report"]["status"], "clean")
        self.assertEqual(result["data"]["stop_reason"], "clean")
        self.assertEqual(result["data"]["before_html"], "before-html")
        self.assertEqual(result["data"]["after_html"], "after-html")
        self.assertEqual(mocked_apply.call_count, 2)

    def test_migrate_inspected_session_mapping_group_state_converges_multiple_rounds(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        before_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:main",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "group_owner_transfer", "group_id": "group-9", "from": "old-owner", "to": "new-owner"},
                    ],
                }]
            }
        }
        second_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:main",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "group_admin_add", "group_id": "group-9", "user_id": "management-user", "from": "not_admin", "to": "admin"},
                    ],
                }]
            }
        }
        clean_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:main",
                    "status": "clean",
                    "proposed_changes": [],
                }]
            }
        }

        with mock.patch.object(m, "inspect_session_mapping_group_state_for_session", side_effect=[before_inspect, second_inspect, clean_inspect]), \
             mock.patch.object(m, "render_inspect_session_mapping_group_state_html_for_session", side_effect=["before-html", "after-html"]), \
             mock.patch.object(m, "_apply_group_state_change", side_effect=[{"result": "ok"}, {"result": "ok"}]) as mocked_apply:
            result = m.migrate_inspected_session_mapping_group_state("app-id", node_info, "agent:main:main")

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["after_report"]["status"], "clean")
        self.assertEqual(result["data"]["stop_reason"], "clean")
        self.assertEqual(mocked_apply.call_count, 2)
        self.assertEqual(
            [entry["round"] for entry in result["data"]["applied_changes"]],
            [1, 2],
        )

    def test_migrate_inspected_session_mapping_group_state_stops_on_repeated_proposed_changes(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        before_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:loop",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "group_admin_add", "group_id": "group-9", "user_id": "management-user", "from": "not_admin", "to": "admin"},
                    ],
                }]
            }
        }
        repeated_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:loop",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "group_admin_add", "group_id": "group-9", "user_id": "management-user", "from": "not_admin", "to": "admin"},
                    ],
                }]
            }
        }

        with mock.patch.object(m, "inspect_session_mapping_group_state_for_session", side_effect=[before_inspect, repeated_inspect]), \
             mock.patch.object(m, "render_inspect_session_mapping_group_state_html_for_session", side_effect=["before-html", "after-html"]), \
             mock.patch.object(m, "_apply_group_state_change", return_value={"result": "ok"}) as mocked_apply:
            result = m.migrate_inspected_session_mapping_group_state("app-id", node_info, "agent:main:loop")

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["stop_reason"], "repeated_proposed_changes")
        self.assertEqual(result["data"]["after_report"]["status"], "dirty")
        self.assertEqual(mocked_apply.call_count, 1)

    def test_migrate_inspected_session_mapping_group_state_supports_node_id_and_dry_run(self):
        m = lanying_openclaw_migration
        before_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:test-child",
                    "status": "dirty",
                    "current_owner_user_id": "old-owner",
                    "expected_owner_user_id": "new-owner",
                    "proposed_changes": [
                        {"action": "group_owner_transfer", "group_id": "group-9", "from": "old-owner", "to": "new-owner"},
                    ],
                }]
            }
        }

        with mock.patch.object(m.lanying_openclaw, "get_node", return_value={"node_id": "15", "user_id": "openclaw-user"}), \
             mock.patch.object(m, "inspect_session_mapping_group_state_for_session", return_value=before_inspect), \
             mock.patch.object(m, "render_inspect_session_mapping_group_state_html_for_session", return_value="before-html"), \
             mock.patch.object(m, "_apply_group_state_change") as mocked_apply:
            result = m.migrate_inspected_session_mapping_group_state("app-id", "15", "agent:main:subagent:test-child", dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["dry_run"])
        self.assertEqual(result["data"]["after_report"]["status"], "clean")
        self.assertEqual(result["data"]["after_report"]["current_owner_user_id"], "new-owner")
        mocked_apply.assert_not_called()

    def test_apply_group_state_change_uses_migration_rewrite_for_mapping_field_update(self):
        m = lanying_openclaw_migration
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "group_id": "legacy-group",
            "origin_kind": "openclaw_control",
        }

        with mock.patch.object(m.lanying_openclaw, "get_session_mapping_by_session", return_value=dict(mapping)), \
             mock.patch.object(m.lanying_openclaw, "rewrite_session_mapping_for_migration", return_value={"result": "ok", "data": {}}) as mocked_rewrite, \
             mock.patch.object(m.lanying_openclaw, "set_session_mapping") as mocked_set:
            result = m._apply_group_state_change(
                "app-id",
                "15",
                "agent:main:clawchat-router:direct:6632092019520",
                {
                    "action": "mapping_field_update",
                    "field": "group_id",
                    "to": "",
                },
            )

        self.assertEqual(result["result"], "ok")
        mocked_set.assert_not_called()
        mocked_rewrite.assert_called_once()
        rewritten_mapping = mocked_rewrite.call_args.args[2]
        self.assertEqual(rewritten_mapping["group_id"], "")
        self.assertEqual(mocked_rewrite.call_args.kwargs["change_source"], "group_state_session_mapping_migration")

    def test_inspect_session_mapping_group_state_for_session_only_checks_target_session(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        detail = {
            "session_key": "agent:main:main",
            "group_id": "group-9",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "root_session_key": "agent:main:main",
            "group_info": {"group_id": "group-9", "owner_id": "openclaw-user", "type": 0},
            "key_user_status": {
                "openclaw_user_id": {"user_id": "openclaw-user", "present_in_group": True, "is_group_owner": True, "admin_status": "admin"},
                "management_user_id": {"user_id": "management-user", "present_in_group": True, "is_group_owner": False, "admin_status": "not_admin"},
            },
            "group_info_error": "",
            "member_list_error": "",
            "admin_list_error": "",
        }

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value=""), \
             mock.patch.object(m.lanying_openclaw, "get_session_mapping_detail_by_session", return_value=detail) as mocked_get_detail, \
             mock.patch.object(m.lanying_openclaw, "list_session_mapping_details_for_node") as mocked_list_details:
            result = m.inspect_session_mapping_group_state_for_session("app-id", node_info, "agent:main:main")

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["checked_mapping_count"], 1)
        self.assertEqual(result["data"]["mapping_reports"][0]["session_key"], "agent:main:main")
        mocked_get_detail.assert_called_once_with("app-id", "15", "agent:main:main")
        mocked_list_details.assert_not_called()

    def test_migrate_inspected_session_mapping_group_states_for_node_supports_node_id(self):
        m = lanying_openclaw_migration
        inspect_result = {
            "result": "ok",
            "data": {
                "mapping_reports": [
                    {"session_key": "session-a", "status": "dirty"},
                    {"session_key": "session-b", "status": "clean"},
                ]
            }
        }

        with mock.patch.object(m.lanying_openclaw, "get_node", return_value={"node_id": "15", "user_id": "openclaw-user"}), \
             mock.patch.object(m, "inspect_session_mapping_group_states_for_node", side_effect=[inspect_result, inspect_result]), \
             mock.patch.object(m, "migrate_inspected_session_mapping_group_state", return_value={"result": "ok"}) as mocked_migrate:
            result = m.migrate_inspected_session_mapping_group_states_for_node("app-id", "15", dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["dry_run"])
        mocked_migrate.assert_called_once_with("app-id", {"node_id": "15", "user_id": "openclaw-user"}, "session-a", dry_run=True)

    def test_migrate_inspected_session_mapping_group_states_for_node_sorts_by_session_key(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        inspect_result = {
            "result": "ok",
            "data": {
                "mapping_reports": [
                    {"session_key": "session-c", "status": "dirty"},
                    {"session_key": "session-a", "status": "dirty"},
                    {"session_key": "session-b", "status": "dirty"},
                ]
            }
        }

        with mock.patch.object(m, "inspect_session_mapping_group_states_for_node", side_effect=[inspect_result, inspect_result]), \
             mock.patch.object(m, "migrate_inspected_session_mapping_group_state", return_value={"result": "ok"}) as mocked_migrate:
            m.migrate_inspected_session_mapping_group_states_for_node("app-id", node_info, dry_run=True)

        self.assertEqual(
            [call.args[2] for call in mocked_migrate.call_args_list],
            ["session-a", "session-b", "session-c"],
        )

    def test_migrate_inspected_session_mapping_group_states_for_app_sorts_nodes(self):
        m = lanying_openclaw_migration
        node_list_result = {
            "result": "ok",
            "data": {
                "list": [
                    {"node_id": "3"},
                    {"node_id": "1"},
                    {"node_id": "2"},
                ]
            }
        }

        with mock.patch.object(m.lanying_openclaw, "get_node_list", return_value=node_list_result), \
             mock.patch.object(m, "migrate_inspected_session_mapping_group_states_for_node", side_effect=[
                 {"result": "ok", "data": {"dirty_before_count": 1}},
                 {"result": "ok", "data": {"dirty_before_count": 2}},
                 {"result": "ok", "data": {"dirty_before_count": 3}},
             ]) as mocked_migrate:
            result = m.migrate_inspected_session_mapping_group_states_for_app("app-id", dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["node_count"], 3)
        self.assertEqual(result["data"]["dirty_before_count"], 6)
        self.assertEqual(
            [call.args[1]["node_id"] for call in mocked_migrate.call_args_list],
            ["1", "2", "3"],
        )

    def test_migrate_inspected_session_mapping_group_states_for_all_apps_scans_all_apps(self):
        m = lanying_openclaw_migration

        with mock.patch.object(m.lanying_openclaw, "list_openclaw_node_list_app_ids", return_value=["app-b", "app-a"]), \
             mock.patch.object(m, "migrate_inspected_session_mapping_group_states_for_app", side_effect=[
                 {"result": "ok", "data": {"node_count": 2, "dirty_before_count": 3}},
                 {"result": "ok", "data": {"node_count": 1, "dirty_before_count": 4}},
             ]) as mocked_migrate:
            result = m.migrate_inspected_session_mapping_group_states_for_all_apps(dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["app_count"], 2)
        self.assertEqual(result["data"]["node_count"], 3)
        self.assertEqual(result["data"]["dirty_before_count"], 7)
        self.assertEqual(
            [call.args[0] for call in mocked_migrate.call_args_list],
            ["app-a", "app-b"],
        )

    def test_set_session_mapping_accepts_group_bound_through_legacy_mapping(self):
        m = lanying_openclaw
        redis = FakeRedis()
        old_session_key = "agent:main:group:group-11"
        canonical_session_key = "agent:main:clawchat:group:group-11"
        legacy_body = {
            "session_key": old_session_key,
            "group_id": "group-11",
            "app_id": "app-id",
            "node_id": "node-1",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "management-user",
        }
        redis.set(
            m.get_openclaw_session_mapping_by_group_key("app-id", "node-1", "openclaw-user", "group-11"),
            json.dumps(legacy_body),
        )
        redis.set(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
            json.dumps(legacy_body),
        )
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), old_session_key)

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(m.lanying_redis, "redis_get", side_effect=lambda r, key: r.values.get(key)):
            result = m.set_session_mapping(
                "app-id",
                "node-1",
                {
                    "session_key": canonical_session_key,
                    "group_id": "group-11",
                    "app_id": "app-id",
                    "node_id": "node-1",
                    "openclaw_user_id": "openclaw-user",
                    "management_user_id": "management-user",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertIn(
            m.get_openclaw_session_mapping_by_session_key("app-id", "node-1", canonical_session_key),
            redis.values,
        )

    def test_router_child_mapping_uses_sender_and_bound_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_node_chatbot_id", return_value="chatbot-id"), \
             mock.patch.object(m.lanying_chatbot, "get_chatbot", return_value={"user_id": "chatbot-user"}):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                "sender-user",
            )
            decision = m.resolve_session_mapping_decision(
                "agent:main:subagent:test-child",
                lineage,
                False,
                inherited,
                "management-user",
                "openclaw-user",
            )
            payload = m.build_session_mapping_payload(
                "app-id",
                "15",
                "openclaw-user",
                "management-user",
                "agent:main:subagent:test-child",
                "group-1",
                lineage,
                "agent:main:subagent:test-child",
                inherited,
            )

        self.assertEqual(inherited["origin_kind"], "im_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["chatbot_user_id"], "chatbot-user")
        self.assertEqual(decision["owner_user_id"], "chatbot-user")
        self.assertEqual(decision["origin_kind"], "im_user")
        self.assertEqual(decision["origin_user_id"], "sender-user")
        self.assertEqual(decision["chatbot_user_id"], "chatbot-user")
        self.assertEqual(payload["origin_kind"], "im_user")
        self.assertEqual(payload["origin_user_id"], "sender-user")
        self.assertEqual(payload["chatbot_user_id"], "chatbot-user")

    def test_direct_root_child_mapping_owner_prefers_openclaw_user(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }
        inherited = {
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_group_root_child_mapping_owner_uses_openclaw_user(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        inherited = {
            "origin_kind": "openclaw_control",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_non_router_owner_uses_management_when_openclaw_missing(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        inherited = {
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "management-user")

    def test_router_group_missing_sender_does_not_inherit_management_user(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat-router:group:group-1",
            "root_session_key": "agent:main:clawchat-router:group:group-1",
        }

        with mock.patch.object(m, "get_node_chatbot_id", return_value="chatbot-id"), \
             mock.patch.object(m.lanying_chatbot, "get_chatbot", return_value={"user_id": "chatbot-user"}), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                "",
            )

        self.assertEqual(inherited["origin_kind"], "")
        self.assertEqual(inherited["origin_user_id"], "")
        self.assertEqual(inherited["source"], "missing")
        self.assertEqual(inherited["management_user_id"], "management-user")
        self.assertEqual(inherited["chatbot_user_id"], "chatbot-user")

    def test_clawchat_group_observed_sender_resolves_as_im_user(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=None):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_sender_user_id": "sender-user",
                    "observed_from_user_id": "sender-user",
                    "observed_to_id": "group-1",
                    "observed_chat_type": "group",
                    "observed_channel": "clawchat",
                    "observed_message_type": "im_inbound_user",
                },
            )

        self.assertEqual(inherited["origin_kind"], "im_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["source"], "explicit")

    def test_legacy_sender_for_clawchat_group_resolves_as_im_user(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=None):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                "sender-user",
            )

        self.assertEqual(inherited["origin_kind"], "im_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")

    def test_gateway_subsession_control_ui_fallback_inherits_parent_im_origin(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        parent_mapping = {
            "session_key": "agent:main:clawchat:group:group-1",
            "group_id": "group-1",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=parent_mapping):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "observed_message_text": "[Subagent Context]\n\n[Subagent Task]: question",
                },
            )

        self.assertEqual(inherited["origin_kind"], "im_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["source"], "parent")

    def test_im_subagent_bootstrap_inherits_parent_im_origin(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        parent_mapping = {
            "session_key": "agent:main:clawchat:group:group-1",
            "group_id": "group-1",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=parent_mapping):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "sync_variant": "im_subagent_bootstrap",
                    "observed_message_text": "[Subagent Context]\n\n[Subagent Task]: question",
                },
            )

        self.assertEqual(inherited["origin_kind"], "im_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["source"], "parent")

    def test_control_ui_provenance_does_not_inherit_parent_im_origin(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        parent_mapping = {
            "session_key": "agent:main:clawchat:group:group-1",
            "group_id": "group-1",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=parent_mapping):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "provenance",
                    "observed_message_text": "[Subagent Context]\n\n[Subagent Task]: question",
                },
            )

        self.assertEqual(inherited["origin_kind"], "openclaw_control")
        self.assertEqual(inherited["origin_user_id"], "")
        self.assertEqual(inherited["source"], "control_ui")

    def test_control_ui_user_without_parent_mapping_uses_direct_root_identity(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=None):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "observed_message_text": "[Subagent Context]\n\n[Subagent Task]: question",
                },
            )

        self.assertEqual(inherited["origin_kind"], "direct_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["source"], "direct")

    def test_router_direct_control_ui_user_without_sender_uses_direct_root_identity(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"):
            inherited = m.resolve_inherited_origin_identity(
                "app-id",
                node_info,
                lineage,
                "management-user",
                {
                    "observed_message_type": "control_ui_user",
                    "observed_message_text": "你是谁",
                },
            )

        self.assertEqual(inherited["origin_kind"], "direct_user")
        self.assertEqual(inherited["origin_user_id"], "sender-user")
        self.assertEqual(inherited["chatbot_user_id"], "chatbot-user")
        self.assertEqual(inherited["source"], "direct")

    def test_merge_sub_sessions_keeps_creating_child_group_for_clawchat_root(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        inherited = {
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            True,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_merge_sub_sessions_non_clawchat_subagent_child_creates_group(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:session-parent",
            "root_session_key": "agent:main:session-root",
        }
        inherited = {
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            True,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_merge_sub_sessions_non_clawchat_non_subagent_stays_metadata_only(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:session-parent",
            "root_session_key": "agent:main:session-root",
        }
        inherited = {
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:session-child",
            lineage,
            True,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "metadata_only")

    def test_merge_sub_sessions_requires_session_map_sync_enabled(self):
        m = lanying_openclaw
        self.assertFalse(m.is_session_map_sync_enabled({
            "session_map_sync": "off",
        }))
        self.assertTrue(m.is_session_map_sync_enabled({
            "session_map_sync": "on",
        }))
        self.assertFalse(m.is_merge_sub_sessions_enabled({
            "session_map_sync": "off",
            "merge_sub_sessions": "on",
        }))
        self.assertTrue(m.is_merge_sub_sessions_enabled({
            "session_map_sync": "on",
            "merge_sub_sessions": "on",
        }))

    def test_clawchat_direct_session_stays_metadata_only(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }
        inherited = {
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:clawchat:direct:sender-user",
            lineage,
            False,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "metadata_only")
        self.assertEqual(decision["group_id"], "")
        self.assertEqual(decision["effective_target_session_key"], "agent:main:clawchat:direct:sender-user")

    def test_router_group_members_include_sender_and_chatbot_only(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-1",
                "openclaw-user",
                "management-user",
                {
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                },
                {
                    "channel": "clawchat-router",
                    "chat_type": "direct",
                    "target_id": "sender-user",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "sender-user", "group-1"),
                ("app-id", "chatbot-user", "group-1"),
            ],
        )

    def test_direct_root_child_group_members_include_sender_and_openclaw_only(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-2",
                "openclaw-user",
                "management-user",
                {
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                },
                {
                    "channel": "clawchat",
                    "chat_type": "direct",
                    "target_id": "sender-user",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "sender-user", "group-2"),
                ("app-id", "openclaw-user", "group-2"),
            ],
        )

    def test_group_root_child_group_members_include_sender_and_openclaw_only(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-3",
                "openclaw-user",
                "management-user",
                {
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                },
                {
                    "channel": "clawchat",
                    "chat_type": "group",
                    "target_id": "group-1",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "sender-user", "group-3"),
                ("app-id", "openclaw-user", "group-3"),
            ],
        )

    def test_generic_root_child_group_members_do_not_prejoin_management_user(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-4",
                "openclaw-user",
                "management-user",
                {
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                },
                None,
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "openclaw-user", "group-4"),
            ],
        )

    def test_router_assistant_forwarding_uses_chatbot_user(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=123) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-1",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                },
                "assistant",
                "hello from child",
            )

        mocked_join.assert_called_once_with("app-id", "chatbot-user", "group-1")
        self.assertEqual(mocked_send.call_args.args[2], "chatbot-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-1")

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=456) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "chatbot-user",
                "assistant",
                "hello direct",
                "agent:main:clawchat-router:direct:sender-user",
            )

        self.assertEqual(mocked_send.call_args.args[2], "chatbot-user")
        self.assertEqual(mocked_send.call_args.args[3], "sender-user")

    def test_direct_root_assistant_forwarding_uses_openclaw_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=321) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-1",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:direct:sender-user",
                },
                "assistant",
                "hello from child",
            )

        mocked_join.assert_called_once_with("app-id", "openclaw-user", "group-1")
        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-1")

    def test_generic_root_user_forwarding_uses_management_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=222) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:main",
                },
                "user",
                "first line",
            )

        mocked_join.assert_called_once_with("app-id", "management-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "management-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_openclaw_group_user_forwarding_uses_management_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_group_admin", return_value=True) as mocked_ensure_admin, \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=223) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:group:group-9",
                },
                "user",
                "question from session",
            )

        mocked_ensure_admin.assert_called_once_with("app-id", "management-user", "group-9")
        mocked_join.assert_not_called()
        self.assertEqual(mocked_send.call_args.args[2], "management-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_ensure_user_group_admin_schedules_async_promotion_after_join(self):
        m = lanying_openclaw

        with mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.executor, "submit", return_value=None) as mocked_submit:
            result = m.ensure_user_group_admin("app-id", "management-user", "group-9")

        self.assertTrue(result)
        mocked_join.assert_called_once_with("app-id", "management-user", "group-9")
        mocked_submit.assert_called_once_with(m.ensure_user_group_admin_sync, "app-id", "management-user", "group-9")


    def test_openclaw_group_user_forwarding_promotes_management_user_as_admin(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_group_admin", return_value=True) as mocked_ensure_admin, \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=223) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:clawchat-router:group:group-9",
                    "group_id": "group-9",
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:group:group-9",
                },
                "user",
                "question from OpenClaw console",
            )

        mocked_ensure_admin.assert_called_once_with("app-id", "management-user", "group-9")
        mocked_join.assert_not_called()
        self.assertEqual(mocked_send.call_args.args[2], "management-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_clawchat_group_user_forwarding_uses_observed_sender_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:group:group-9",
                },
                "user",
                "question from user-triggered session",
            )

        mocked_join.assert_called_once_with("app-id", "sender-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_router_group_child_control_ui_user_turn_uses_management_for_non_bootstrap_text(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m, "ensure_user_group_admin", return_value=True) as mocked_admin_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:test-child",
                    "root_session": "agent:main:clawchat-router:group:group-9",
                    "observed_message_type": "control_ui_user",
                    "message": {
                        "role": "user",
                        "content": "哈哈",
                    },
                },
            )

        mocked_admin_join.assert_called_once_with("app-id", "management-user", "group-9")
        mocked_join.assert_not_called()
        self.assertEqual(mocked_send.call_args.args[2], "management-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_control_ui_user_turn_uses_management_without_rewriting_im_mapping(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-9",
            "effective_target_session_key": "agent:main:clawchat:group:group-9",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat:group:group-9",
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "message": {
                        "role": "user",
                        "content": "你好",
                    },
                },
            )

        self.assertEqual(mapping["origin_kind"], "im_user")
        self.assertEqual(mapping["origin_user_id"], "sender-user")
        mocked_join.assert_called_once_with("app-id", "management-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "management-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_gateway_simulated_group_user_turn_keeps_im_sender_mapping(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-9",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-9",
            "effective_target_session_key": "agent:main:clawchat:group:group-9",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat:group:group-9",
                    "observed_message_type": "im_inbound_user",
                    "observed_sender_user_id": "sender-user",
                    "observed_chat_type": "group",
                    "message": {
                        "role": "user",
                        "content": "[Subagent Task]: question from gateway",
                    },
                },
            )

        mocked_join.assert_called_once_with("app-id", "sender-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_im_subagent_bootstrap_group_user_turn_reaches_im_sync_path(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "parent_session_key": "agent:main:clawchat-router:group:group-9",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:test-child",
                    "parent_session": "agent:main:clawchat-router:group:group-9",
                    "root_session": "agent:main:clawchat-router:group:group-9",
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "sync_variant": "im_subagent_bootstrap",
                    "message": {
                        "role": "user",
                        "content": "[Subagent Context]\n\n[Subagent Task]: question from IM group",
                    },
                },
            )

        mocked_join.assert_called_once_with("app-id", "sender-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_control_ui_direct_user_turn_keeps_existing_direct_sender(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:direct:sender-user",
            "group_id": "",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat:direct:sender-user",
                    "observed_message_type": "control_ui_user",
                    "message": {
                        "role": "user",
                        "content": "direct control text",
                    },
                },
            )

        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "openclaw-user")

    def test_should_send_control_ui_user_as_management_skips_router_group_child_bootstrap(self):
        m = lanying_openclaw

        should_override = m.should_send_control_ui_user_as_management(
            {
                "observed_message_type": "control_ui_user",
                "observed_message_text": "[Subagent Context]\n\n[Subagent Task]: question from control ui",
            },
            {
                "session_key": "agent:main:subagent:test-child",
                "root_session_key": "agent:main:clawchat-router:group:group-9",
                "origin_kind": "im_user",
                "origin_user_id": "sender-user",
                "management_user_id": "management-user",
            },
        )

        self.assertFalse(should_override)

    def test_should_send_control_ui_user_as_management_uses_management_for_router_group_child_non_bootstrap(self):
        m = lanying_openclaw

        should_override = m.should_send_control_ui_user_as_management(
            {
                "observed_message_type": "control_ui_user",
                "observed_message_text": "哈哈",
            },
            {
                "session_key": "agent:main:subagent:test-child",
                "root_session_key": "agent:main:clawchat-router:group:group-9",
                "origin_kind": "im_user",
                "origin_user_id": "sender-user",
                "management_user_id": "management-user",
            },
        )

        self.assertTrue(should_override)

    def test_openclaw_group_subsession_sync_sender_sequence(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        user_triggered_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-9",
        }
        console_mapping = dict(user_triggered_mapping)
        console_mapping["origin_kind"] = "openclaw_control"
        console_mapping["origin_user_id"] = ""

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                user_triggered_mapping,
                "user",
                "question from IM user",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                user_triggered_mapping,
                "assistant",
                "reply from OpenClaw",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                console_mapping,
                "user",
                "question from OpenClaw console",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                console_mapping,
                "assistant",
                "reply from OpenClaw console",
            )

        sent = [call.args for call in mocked_send.call_args_list]
        self.assertEqual([args[2] for args in sent], [
            "sender-user",
            "openclaw-user",
            "management-user",
            "openclaw-user",
        ])
        self.assertEqual([args[3] for args in sent], ["group-9"] * 4)
        self.assertEqual([call.args[1] for call in mocked_join.call_args_list], [
            "sender-user",
            "openclaw-user",
            "management-user",
            "openclaw-user",
        ])

    def test_router_group_child_user_forwarding_keeps_sender_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=225) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:group:group-9",
                },
                "user",
                "[Subagent Task]: question from IM group",
            )

        mocked_join.assert_called_once_with("app-id", "sender-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_chatbot_group_subsession_sync_sender_sequence(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        user_triggered_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
        }
        console_mapping = dict(user_triggered_mapping)
        console_mapping["origin_kind"] = "openclaw_control"
        console_mapping["origin_user_id"] = ""

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=225) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                user_triggered_mapping,
                "user",
                "question from IM user",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                user_triggered_mapping,
                "assistant",
                "reply from chatbot",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                console_mapping,
                "user",
                "question from OpenClaw console",
            )
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                console_mapping,
                "assistant",
                "reply from chatbot after console message",
            )

        sent = [call.args for call in mocked_send.call_args_list]
        self.assertEqual([args[2] for args in sent], [
            "sender-user",
            "chatbot-user",
            "management-user",
            "chatbot-user",
        ])
        self.assertEqual([args[3] for args in sent], ["group-9"] * 4)
        self.assertEqual([call.args[1] for call in mocked_join.call_args_list], [
            "sender-user",
            "chatbot-user",
            "management-user",
            "chatbot-user",
        ])

    def test_openclaw_direct_subsession_sync_sender_sequence(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        user_triggered_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=226) as mocked_send:
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "user", "question from IM user")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "assistant", "reply from OpenClaw")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "user", "question from OpenClaw console")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "assistant", "reply from OpenClaw console")

        sent = [call.args for call in mocked_send.call_args_list]
        self.assertEqual([args[2] for args in sent], [
            "sender-user",
            "openclaw-user",
            "sender-user",
            "openclaw-user",
        ])
        self.assertEqual([args[3] for args in sent], ["group-9"] * 4)
        self.assertEqual([call.args[1] for call in mocked_join.call_args_list], [
            "sender-user",
            "openclaw-user",
            "sender-user",
            "openclaw-user",
        ])

    def test_chatbot_direct_subsession_sync_sender_sequence(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        user_triggered_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=227) as mocked_send:
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "user", "question from IM user")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "assistant", "reply from chatbot")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "user", "question from OpenClaw console")
            m.forward_session_sync_to_group("app-id", node_info, user_triggered_mapping, "assistant", "reply from chatbot after console message")

        sent = [call.args for call in mocked_send.call_args_list]
        self.assertEqual([args[2] for args in sent], [
            "sender-user",
            "chatbot-user",
            "sender-user",
            "chatbot-user",
        ])
        self.assertEqual([args[3] for args in sent], ["group-9"] * 4)
        self.assertEqual([call.args[1] for call in mocked_join.call_args_list], [
            "sender-user",
            "chatbot-user",
            "sender-user",
            "chatbot-user",
        ])

    def test_handle_session_message_sync_event_subagent_sequence_matrix(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        scenario_cases = [
            {
                "name": "group_openclaw_subagent",
                "mapping": {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:group:group-9",
                    "effective_target_session_key": "agent:main:subagent:test-child",
                },
                "expected_from_to": [
                    ("sender-user", "group-9"),
                    ("openclaw-user", "group-9"),
                    ("management-user", "group-9"),
                    ("openclaw-user", "group-9"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [],
            },
            {
                "name": "group_chatbot_subagent",
                "mapping": {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-19",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:group:group-19",
                    "effective_target_session_key": "agent:main:subagent:test-child",
                },
                "expected_from_to": [
                    ("sender-user", "group-19"),
                    ("chatbot-user", "group-19"),
                    ("management-user", "group-19"),
                    ("chatbot-user", "group-19"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [],
            },
            {
                "name": "direct_openclaw_subagent",
                "mapping": {
                    "session_key": "agent:main:subagent:test-child",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:direct:sender-user",
                    "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
                },
                "expected_from_to": [
                    ("sender-user", "openclaw-user"),
                    ("openclaw-user", "sender-user"),
                    ("sender-user", "openclaw-user"),
                    ("openclaw-user", "sender-user"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [],
            },
            {
                "name": "direct_chatbot_subagent",
                "mapping": {
                    "session_key": "agent:main:subagent:test-child",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                    "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
                },
                "expected_from_to": [
                    ("sender-user", "chatbot-user"),
                    ("sender-user", "chatbot-user"),
                ],
                "expected_router_direct_replies": [
                    ("sender-user", "reply from child session"),
                    ("sender-user", "reply from OpenClaw console"),
                ],
                "expected_router_group_replies": [],
            },
        ]

        event_counter = {"value": 0}

        def _build_event(source, role, content, observed_message_type):
            event_counter["value"] += 1
            return {
                "type": "session_transcript_observed",
                "source": source,
                "session": "agent:main:subagent:test-child",
                "root_session": current_case["mapping"]["root_session_key"],
                "message_id": f"{current_case['name']}:{event_counter['value']}",
                "message": {
                    "role": role,
                    "content": content,
                },
                "observed_message_type": observed_message_type,
                "sync_variant": (
                    "im_subagent_bootstrap"
                    if source == "control_ui_user" and observed_message_type == "im_inbound_user"
                    else ""
                ),
            }

        for current_case in scenario_cases:
            with self.subTest(current_case=current_case["name"]):
                m.recent_session_ai_dynamic_stream_by_key.clear()
                m.recent_session_ai_dynamic_dedupe_by_key.clear()
                m.recent_request_debug_stream_by_key.clear()
                with m.recent_session_ai_dynamic_lock_registry_lock:
                    m.recent_session_ai_dynamic_lock_by_key.clear()
                sent_calls = []
                router_direct_reply_calls = []
                router_group_reply_calls = []

                def _capture_send(_config, _app_id, from_user_id, to_user_id, _send_type, _ctype, text, _extra):
                    sent_calls.append((from_user_id, to_user_id, text))
                    return len(sent_calls) + 100

                def _capture_router_direct_reply(_app_id, _node_info, target_user_id, text, delivery_ext=None):
                    router_direct_reply_calls.append((target_user_id, text, delivery_ext))
                    return 1

                def _capture_router_group_reply(_app_id, _node_info, mapping, text, delivery_ext=None):
                    router_group_reply_calls.append((str((mapping or {}).get("session_key", "")).strip(), text, delivery_ext))
                    return 1

                with mock.patch.object(m, "get_session_mapping_by_session", return_value=current_case["mapping"]), \
                     mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": current_case["mapping"]}), \
                     mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
                     mock.patch.object(m, "ensure_user_group_admin", return_value=True), \
                     mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
                     mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=_capture_send), \
                     mock.patch.object(m, "forward_session_sync_router_direct_reply", side_effect=_capture_router_direct_reply), \
                     mock.patch.object(m, "forward_session_sync_router_group_reply", side_effect=_capture_router_group_reply):
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        _build_event("control_ui_user", "user", "[Subagent Task]: do work\n[Subagent Context]: ...", "im_inbound_user"),
                    )
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        _build_event("control_ui_reply", "assistant", "reply from child session", "control_ui_reply"),
                    )
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        _build_event("control_ui_user", "user", "question from OpenClaw console", "control_ui_user"),
                    )
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        _build_event("control_ui_reply", "assistant", "reply from OpenClaw console", "control_ui_reply"),
                    )

                self.assertEqual(
                    [(item[0], item[1]) for item in sent_calls],
                    current_case["expected_from_to"],
                )
                self.assertEqual(
                    [(item[0], item[1]) for item in router_direct_reply_calls],
                    current_case["expected_router_direct_replies"],
                )
                self.assertEqual(
                    [(item[0], item[1]) for item in router_group_reply_calls],
                    current_case["expected_router_group_replies"],
                )

    def test_openclaw_direct_child_control_ui_user_sync_uses_sender_identity(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=1) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:test-child",
                    "root_session": "agent:main:clawchat:direct:sender-user",
                    "observed_message_type": "control_ui_user",
                    "message": {
                        "role": "user",
                        "content": "direct child control text",
                    },
                },
            )

        mocked_direct.assert_called_once()
        self.assertEqual(mocked_direct.call_args.args[2], "sender-user")
        self.assertEqual(mocked_direct.call_args.args[3], "sender-user")
        self.assertEqual(mocked_direct.call_args.args[5], "user")

    def test_handle_session_message_sync_event_root_console_sequence_matrix(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        scenario_cases = [
            {
                "name": "group_openclaw_root",
                "mapping": {
                    "session_key": "agent:main:clawchat:group:group-9",
                    "group_id": "group-9",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:group:group-9",
                    "effective_target_session_key": "agent:main:clawchat:group:group-9",
                },
                "expected_from_to": [
                    ("management-user", "group-9"),
                    ("openclaw-user", "group-9"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [],
            },
            {
                "name": "group_chatbot_root",
                "mapping": {
                    "session_key": "agent:main:clawchat-router:group:group-19",
                    "group_id": "group-19",
                    "origin_kind": "im_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:group:group-19",
                    "effective_target_session_key": "agent:main:clawchat-router:group:group-19",
                },
                "expected_from_to": [
                    ("management-user", "group-19"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [
                    ("agent:main:clawchat-router:group:group-19", "reply from OpenClaw console"),
                ],
            },
            {
                "name": "direct_openclaw_root",
                "mapping": {
                    "session_key": "agent:main:clawchat:direct:sender-user",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:direct:sender-user",
                    "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
                },
                "expected_from_to": [
                    ("sender-user", "openclaw-user"),
                    ("openclaw-user", "sender-user"),
                ],
                "expected_router_direct_replies": [],
                "expected_router_group_replies": [],
            },
            {
                "name": "direct_chatbot_root",
                "mapping": {
                    "session_key": "agent:main:clawchat-router:direct:sender-user",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                    "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
                },
                "expected_from_to": [
                    ("sender-user", "chatbot-user"),
                ],
                "expected_router_direct_replies": [
                    ("sender-user", "reply from OpenClaw console"),
                ],
                "expected_router_group_replies": [],
            },
        ]

        for current_case in scenario_cases:
            with self.subTest(current_case=current_case["name"]):
                sent_calls = []
                router_direct_reply_calls = []
                router_group_reply_calls = []

                def _capture_send(_config, _app_id, from_user_id, to_user_id, _send_type, _ctype, text, _extra):
                    sent_calls.append((from_user_id, to_user_id, text))
                    return len(sent_calls) + 200

                def _capture_router_direct_reply(_app_id, _node_info, target_user_id, text, delivery_ext=None):
                    router_direct_reply_calls.append((target_user_id, text, delivery_ext))
                    return 1

                def _capture_router_group_reply(_app_id, _node_info, mapping, text, delivery_ext=None):
                    router_group_reply_calls.append((str((mapping or {}).get("session_key", "")).strip(), text, delivery_ext))
                    return 1

                with mock.patch.object(m, "get_session_mapping_by_session", return_value=current_case["mapping"]), \
                     mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": current_case["mapping"]}), \
                     mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
                     mock.patch.object(m, "ensure_user_group_admin", return_value=True), \
                     mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
                     mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=_capture_send), \
                     mock.patch.object(m, "forward_session_sync_router_direct_reply", side_effect=_capture_router_direct_reply), \
                     mock.patch.object(m, "forward_session_sync_router_group_reply", side_effect=_capture_router_group_reply):
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        {
                            "type": "session_transcript_observed",
                            "source": "control_ui_user",
                            "session": current_case["mapping"]["session_key"],
                            "root_session": current_case["mapping"]["root_session_key"],
                            "message_id": f"{current_case['name']}:1",
                            "observed_message_type": "control_ui_user",
                            "message": {
                                "role": "user",
                                "content": "question from OpenClaw console",
                            },
                        },
                    )
                    m.handle_session_message_sync_event(
                        "app-id",
                        node_info,
                        {
                            "type": "session_transcript_observed",
                            "source": "control_ui_reply",
                            "session": current_case["mapping"]["session_key"],
                            "root_session": current_case["mapping"]["root_session_key"],
                            "message_id": f"{current_case['name']}:2",
                            "message": {
                                "role": "assistant",
                                "content": "reply from OpenClaw console",
                            },
                        },
                    )

                self.assertEqual(
                    [(item[0], item[1]) for item in sent_calls],
                    current_case["expected_from_to"],
                )
                self.assertEqual(
                    [(item[0], item[1]) for item in router_direct_reply_calls],
                    current_case["expected_router_direct_replies"],
                )
                self.assertEqual(
                    [(item[0], item[1]) for item in router_group_reply_calls],
                    current_case["expected_router_group_replies"],
                )

    def test_router_direct_child_control_ui_user_sync_reaches_im_send_path(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=301) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:test-child",
                    "root_session": "agent:main:clawchat-router:direct:sender-user",
                    "observed_message_type": "control_ui_user",
                    "message": {
                        "role": "user",
                        "content": "你是谁",
                    },
                },
            )

        mocked_send.assert_called_once()
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "chatbot-user")

    def test_group_user_forwarding_does_not_fallback_between_management_and_sender(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m, "ensure_user_joined_group") as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send:
            openclaw_group_result = m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "openclaw_control",
                    "origin_user_id": "sender-user",
                    "management_user_id": "",
                    "root_session_key": "agent:main:clawchat:group:group-9",
                },
                "user",
                "question from OpenClaw group",
            )
            router_group_result = m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "",
                    "origin_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:group:group-9",
                },
                "user",
                "question from IM group",
            )

        self.assertEqual(openclaw_group_result, 0)
        self.assertEqual(router_group_result, 0)
        mocked_join.assert_not_called()
        mocked_send.assert_not_called()

    def test_existing_mapping_sender_is_repaired_by_explicit_sender_only(self):
        m = lanying_openclaw
        existing = {
            "session_key": "agent:main:subagent:test-child",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
        }
        lineage = {
            "parent_session_key": "agent:main:clawchat-router:group:group-9",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
        }

        repaired = m.merge_existing_session_mapping(
            existing,
            lineage,
            "agent:main:subagent:test-child",
            {
                "origin_kind": "im_user",
                "origin_user_id": "sender-user",
                "source": "explicit",
            },
        )
        preserved = m.merge_existing_session_mapping(
            repaired,
            lineage,
            "agent:main:subagent:test-child",
            {
                "origin_kind": "openclaw_control",
                "origin_user_id": "",
                "source": "management",
            },
        )

        self.assertEqual(repaired["origin_kind"], "im_user")
        self.assertEqual(repaired["origin_user_id"], "sender-user")
        self.assertEqual(preserved["origin_kind"], "im_user")
        self.assertEqual(preserved["origin_user_id"], "sender-user")

    def test_clawchat_direct_user_forwarding_keeps_sender_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-9",
                    "origin_kind": "direct_user",
                    "origin_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:direct:sender-user",
                },
                "user",
                "direct question from session",
            )

        mocked_join.assert_called_once_with("app-id", "sender-user", "group-9")
        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-9")

    def test_session_sync_forwarding_marks_visible_delivery(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:direct:sender-user",
            "control_ui_user",
            "user",
            "msg-1",
            "",
            "agent:main:session-parent",
            "agent:main:session-root",
        )

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=222) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "chatbot-user",
                "user",
                "question text",
                "agent:main:clawchat-router:direct:sender-user",
                delivery_ext,
            )

        extra = mocked_send.call_args.args[7]
        self.assertEqual(extra["ext"]["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(extra["ext"]["openclaw"]["session"], "agent:main:clawchat:direct:sender-user")
        self.assertEqual(extra["ext"]["openclaw"]["source"], "control_ui_user")
        self.assertEqual(extra["ext"]["openclaw"]["role"], "user")
        self.assertEqual(extra["ext"]["openclaw"]["message_id"], "msg-1")
        self.assertNotIn("trigger_msg_id", extra["ext"]["openclaw"])
        self.assertEqual(extra["ext"]["openclaw"]["parent_session"], "agent:main:session-parent")
        self.assertEqual(extra["ext"]["openclaw"]["root_session"], "agent:main:session-root")
        self.assertEqual(extra["ext"]["ai"]["ai_generate"], False)
        self.assertEqual(extra["skip_antispam_prompt"], True)

    def test_session_sync_forwarding_visible_delivery_carries_sync_variant(self):
        m = lanying_openclaw
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:subagent:test-child",
            "control_ui_user",
            "user",
            "msg-sync-variant-1",
            "",
            "agent:main:clawchat-router:group:group-9",
            "agent:main:clawchat-router:group:group-9",
            "im_subagent_bootstrap",
        )

        self.assertEqual(delivery_ext["openclaw"]["sync_variant"], "im_subagent_bootstrap")

    def test_session_sync_forwarding_visible_delivery_carries_display_kind(self):
        m = lanying_openclaw
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:subagent:test-child",
            "control_ui_reply",
            "assistant",
            "msg-yield-1",
            "",
            "agent:main:clawchat-router:group:group-9",
            "agent:main:clawchat-router:group:group-9",
            "",
            "yield_result",
        )

        self.assertEqual(delivery_ext["openclaw"]["display_kind"], "yield_result")

    def test_control_ui_user_manual_sync_delivery_ext_does_not_include_stale_request_ids(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-9",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-9",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "group_id": "group-9",
        }
        target = {
            "kind": "group",
            "mapping": mapping,
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "ensure_user_group_admin", return_value=True), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=301) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat-router:group:group-9",
                    "message_id": "manual-control-1",
                    "trigger_msg_id": "old-im-request-1",
                    "message": {
                        "role": "user",
                        "content": "控制台里后来手动发的一条消息",
                    },
                },
            )

        sent_ext = mocked_send.call_args.args[7]["ext"]["openclaw"]
        self.assertEqual(sent_ext["source"], "control_ui_user")
        self.assertEqual(sent_ext["role"], "user")
        self.assertNotIn("trigger_msg_id", sent_ext)
        self.assertNotIn("request_msg_id", sent_ext)

    def test_control_ui_user_im_origin_delivery_ext_keeps_request_ids(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-9",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "group_id": "group-9",
        }
        target = {
            "kind": "group",
            "mapping": mapping,
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=301) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat-router:group:group-9",
                    "message_id": "im-origin-1",
                    "trigger_msg_id": "im-request-1",
                    "observed_message_type": "im_inbound_user",
                    "message": {
                        "role": "user",
                        "content": "来自 IM 的用户问题",
                    },
                },
            )

        sent_ext = mocked_send.call_args.args[7]["ext"]["openclaw"]
        self.assertEqual(sent_ext["trigger_msg_id"], "im-request-1")
        self.assertEqual(sent_ext["request_msg_id"], "im-request-1")

    def test_session_sync_user_forwarding_targets_chatbot_in_direct(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat-router:direct:sender-user",
            "control_ui_user",
            "user",
            "msg-user-1",
        )

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=223) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "",
                "user",
                "question text",
                "agent:main:clawchat-router:direct:sender-user",
                delivery_ext,
            )

        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "chatbot-user")

    def test_session_sync_user_forwarding_targets_node_user_for_clawchat_direct(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:direct:sender-user",
            "control_ui_user",
            "user",
            "msg-user-2",
        )

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value=""), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=224) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "",
                "user",
                "question text",
                "agent:main:clawchat:direct:sender-user",
                delivery_ext,
            )

        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "openclaw-user")

    def test_session_sync_user_forwarding_does_not_fallback_without_required_direct_identity(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value=""), \
             mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send:
            missing_sender_result = m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "",
                "",
                "user",
                "question text",
                "agent:main:clawchat:direct:sender-user",
            )
            missing_chatbot_result = m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "",
                "user",
                "question text",
                "agent:main:clawchat-router:direct:sender-user",
            )

        self.assertEqual(missing_sender_result, 0)
        self.assertEqual(missing_chatbot_result, 0)
        mocked_send.assert_not_called()

    def test_session_sync_assistant_forwarding_keeps_openclaw_sender_without_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:direct:sender-user",
            "control_ui_reply",
            "assistant",
            "msg-assistant-1",
        )

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=225) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "",
                "assistant",
                "assistant reply",
                "agent:main:clawchat:direct:sender-user",
                delivery_ext,
            )

        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "sender-user")

    def test_session_sync_user_forwarding_clawchat_direct_does_not_target_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:direct:sender-user",
            "control_ui_user",
            "user",
            "msg-user-3",
        )

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=226) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "",
                "user",
                "question text",
                "agent:main:clawchat:direct:sender-user",
                delivery_ext,
            )

        self.assertEqual(mocked_send.call_args.args[2], "sender-user")
        self.assertEqual(mocked_send.call_args.args[3], "openclaw-user")

    def test_session_sync_delivery_ext_marks_reply_as_no_generate(self):
        m = lanying_openclaw
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:direct:sender-user",
            "control_ui_reply",
            "assistant",
            "msg-2",
            "",
            "",
            "agent:main:clawchat:direct:sender-user",
        )
        self.assertEqual(delivery_ext["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(delivery_ext["openclaw"]["source"], "control_ui_reply")
        self.assertEqual(delivery_ext["openclaw"]["role"], "assistant")
        self.assertEqual(delivery_ext["openclaw"]["message_id"], "msg-2")
        self.assertNotIn("parent_session", delivery_ext["openclaw"])
        self.assertEqual(
            delivery_ext["openclaw"]["root_session"],
            "agent:main:clawchat:direct:sender-user",
        )
        self.assertEqual(delivery_ext["ai"]["ai_generate"], False)

    def test_session_sync_delivery_ext_marks_im_inbound_as_no_generate(self):
        m = lanying_openclaw
        delivery_ext = m.build_session_sync_delivery_ext(
            "agent:main:clawchat:group:group-9",
            "im_inbound_reply",
            "assistant",
            "msg-im-2",
            "",
            "agent:main:subagent:test-parent",
            "agent:main:clawchat:group:group-9",
        )
        self.assertEqual(delivery_ext["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(delivery_ext["openclaw"]["source"], "im_inbound_reply")
        self.assertEqual(delivery_ext["openclaw"]["role"], "assistant")
        self.assertEqual(delivery_ext["openclaw"]["message_id"], "msg-im-2")
        self.assertEqual(delivery_ext["openclaw"]["parent_session"], "agent:main:subagent:test-parent")

    def test_handle_session_message_sync_event_for_sessions_yield_routes_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-9",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "parent_session_key": "agent:main:clawchat-router:group:group-9",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={"kind": "group", "mapping": mapping}), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value={"result": "ok", "data": {
                 "lanying_admin_token": "admin-token",
                 "app_id": "app-id",
                 "reply_msg_type": "GROUPCHAT",
                 "reply_from": "management-user",
                 "reply_to": "group-9",
                 "request_msg_id": "yield-msg-1",
                 "target_kind": "group",
                 "target_id": "group-9",
             }}), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=301) as mocked_send, \
             mock.patch.object(m, "should_forward_group_sync_via_router_reply", return_value=None), \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=301) as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message_id": "yield-msg-1",
                    "parent_session": "agent:main:clawchat-router:group:group-9",
                    "root_session": "agent:main:clawchat-router:group:group-9",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "id": "call-yield-1",
                                "name": "sessions_yield",
                                "arguments": {
                                    "message": "我完成了：讲了一个关于数字3的笑话。",
                                },
                            }
                        ],
                    },
                },
            )

        mocked_group_forward.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(sent_extra["ext"]["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(sent_extra["ext"]["openclaw"]["display_kind"], "yield_result")
        self.assertEqual(sent_extra["ext"]["openclaw"]["message_id"], "yield-msg-1")
        self.assertEqual(sent_extra["ext"]["openclaw"]["root_session"], "agent:main:clawchat-router:group:group-9")
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertIn("Yield", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_routes_group_openclaw_tool_call_to_group_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:clawchat:group:group-42",
                 "mapping": mapping,
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "group-openclaw-tool-call-1",
                    "trigger_msg_id": "request-group-openclaw-1",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "name": "exec",
                                "arguments": {
                                    "command": "pwd",
                                },
                            }
                        ],
                    },
                },
            )

        mocked_group_forward.assert_not_called()
        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-42")
        self.assertEqual(mocked_send.call_args.args[4], 2)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_routes_group_router_tool_call_to_chatbot_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-42",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:clawchat-router:group:group-42",
                 "mapping": mapping,
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "message_id": "group-router-tool-call-1",
                    "trigger_msg_id": "request-group-router-1",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "name": "exec",
                                "arguments": {
                                    "command": "pwd",
                                },
                            }
                        ],
                    },
                },
            )

        mocked_group_forward.assert_not_called()
        self.assertEqual(mocked_send.call_args.args[2], "chatbot-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-42")
        self.assertEqual(mocked_send.call_args.args[4], 2)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_resolve_session_transcript_ai_dynamic_target_prefers_target_session_key_for_clawchat_direct(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
            "chatbot_user_id": "chatbot-user",
        }
        target = {
            "kind": "direct",
            "session_key": "agent:main:clawchat:direct:sender-user",
            "target_user_id": "sender-user",
        }

        with mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"):
            result = m.resolve_session_transcript_ai_dynamic_target(
                "app-id",
                node_info,
                mapping,
                target,
                "request-direct-openclaw-1",
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["reply_msg_type"], "CHAT")
        self.assertEqual(result["data"]["reply_from"], "openclaw-user")
        self.assertEqual(result["data"]["reply_to"], "sender-user")

    def test_resolve_session_transcript_ai_dynamic_target_prefers_target_session_key_for_router_direct(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
            "chatbot_user_id": "chatbot-user",
        }
        target = {
            "kind": "direct",
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "target_user_id": "sender-user",
        }

        with mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"):
            result = m.resolve_session_transcript_ai_dynamic_target(
                "app-id",
                node_info,
                mapping,
                target,
                "request-direct-chatbot-1",
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["reply_msg_type"], "CHAT")
        self.assertEqual(result["data"]["reply_from"], "chatbot-user")
        self.assertEqual(result["data"]["reply_to"], "sender-user")

    def test_resolve_session_transcript_ai_dynamic_target_allows_debug_without_status_bar(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
            "chatbot_user_id": "chatbot-user",
        }
        target = {
            "kind": "direct",
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "target_user_id": "sender-user",
        }

        with mock.patch.object(
            m,
            "resolve_chatbot_status_bar_enabled",
            return_value=(False, {"user_id": "chatbot-user", "preset": {"ext": {"debug": True}}}),
        ), mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"):
            result = m.resolve_session_transcript_ai_dynamic_target(
                "app-id",
                node_info,
                mapping,
                target,
                "request-direct-chatbot-debug-only-1",
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["reply_msg_type"], "CHAT")
        self.assertEqual(result["data"]["reply_from"], "chatbot-user")
        self.assertEqual(result["data"]["reply_to"], "sender-user")
        self.assertTrue(result["data"]["is_debug"])
        self.assertFalse(result["data"]["status_bar"])

    def test_resolve_session_transcript_ai_dynamic_target_uses_openclaw_user_for_clawchat_group(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
        }
        target = {
            "kind": "group",
            "session_key": "agent:main:clawchat:group:group-42",
            "mapping": mapping,
        }

        with mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"):
            result = m.resolve_session_transcript_ai_dynamic_target(
                "app-id",
                node_info,
                mapping,
                target,
                "request-group-openclaw-1",
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["reply_msg_type"], "GROUPCHAT")
        self.assertEqual(result["data"]["reply_from"], "openclaw-user")
        self.assertEqual(result["data"]["reply_to"], "group-42")
        mocked_join.assert_called_once_with("app-id", "openclaw-user", "group-42")

    def test_resolve_session_transcript_ai_dynamic_target_uses_chatbot_user_for_router_group(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-42",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
        }
        target = {
            "kind": "group",
            "session_key": "agent:main:clawchat-router:group:group-42",
            "mapping": mapping,
        }

        with mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"):
            result = m.resolve_session_transcript_ai_dynamic_target(
                "app-id",
                node_info,
                mapping,
                target,
                "request-group-chatbot-1",
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["reply_msg_type"], "GROUPCHAT")
        self.assertEqual(result["data"]["reply_from"], "chatbot-user")
        self.assertEqual(result["data"]["reply_to"], "group-42")
        mocked_join.assert_called_once_with("app-id", "chatbot-user", "group-42")

    def test_extract_session_sync_text_parses_sessions_yield_json_arguments(self):
        m = lanying_openclaw

        with mock.patch.object(
            m.lanying_utils,
            "safe_json_loads",
            side_effect=lambda raw, default=None: json.loads(raw),
        ):
            text = m.extract_session_sync_text([
                {
                    "type": "toolCall",
                    "name": "sessions_yield",
                    "arguments": json.dumps({
                        "message": "我完成了：讲了 JSON 参数里的笑话。",
                    }, ensure_ascii=False),
                }
            ])

        self.assertEqual(text, "我完成了：讲了 JSON 参数里的笑话。")

    def test_extract_session_sync_text_keeps_only_current_message_from_runtime_context(self):
        m = lanying_openclaw

        text = m.extract_session_sync_text(
            "\n".join([
                "[Retrieved knowledge context]",
                "internal knowledge should not be displayed",
                "[End knowledge context]",
                "[Group context messages since last trigger]",
                "[AI] previous internal context",
                "",
                "[Current message]",
                "@chatbot_qkyimzwkzd git clone git@github.com:maxim-top/openclaw-channel-clawchat.git 到/tmp/目录",
            ])
        )

        self.assertEqual(
            text,
            "@chatbot_qkyimzwkzd git clone git@github.com:maxim-top/openclaw-channel-clawchat.git 到/tmp/目录",
        )

    def test_summarize_session_sync_intermediate_message_formats_tool_call(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": "with waiting for subagent to finish",
                    },
                    {
                        "type": "toolCall",
                        "name": "sessions_yield",
                        "arguments": {
                            "message": "waiting for subagent to finish",
                        },
                    },
                ],
            }
        )

        self.assertTrue(summary["is_intermediate"])
        self.assertEqual(summary["transcript_kind"], "yield")
        self.assertEqual(summary["tool_name"], "sessions_yield")
        self.assertIn("Yield", summary["text"])
        self.assertIn("Tool input", summary["text"])
        self.assertIn("waiting for subagent to finish", summary["text"])

    def test_summarize_session_sync_intermediate_message_formats_subagent_tool_call_context(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "toolCall",
                        "name": "sessions_spawn",
                        "arguments": {
                            "taskName": "direct_chatbot_subagent_ai_dynamic_1782207152809",
                            "task": "do something",
                        },
                    },
                ],
            }
        )

        self.assertEqual(summary["transcript_kind"], "tool_call")
        self.assertIn("Sub-agent", summary["text"])
        self.assertTrue(summary["text"].startswith("Sub-agent"))
        self.assertNotIn("direct_chatbot_subagent_ai_dynamic_1782207152809", summary["text"].split("\n", 1)[0])
        self.assertNotIn("\n\nwith ", summary["text"])

    def test_summarize_session_sync_intermediate_message_prefers_subagent_label_for_context(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "toolCall",
                        "name": "sessions_spawn",
                        "arguments": {
                            "taskName": "direct_chatbot_subagent",
                            "label": "direct_chatbot_subagent_ai_dynamic_1782212759267",
                            "task": "do something",
                        },
                    },
                ],
            }
        )

        self.assertTrue(summary["text"].startswith("Sub-agent"))
        self.assertNotIn("direct_chatbot_subagent_ai_dynamic_1782212759267", summary["text"].split("\n", 1)[0])
        self.assertNotIn("\n\nwith ", summary["text"])

    def test_summarize_session_sync_intermediate_message_formats_tool_result(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "toolResult",
                "toolName": "heartbeat_respond",
                "content": [
                    {
                        "type": "text",
                        "text": "{\"status\":\"accepted\",\"notify\":false}",
                    }
                ],
            }
        )

        self.assertTrue(summary["is_intermediate"])
        self.assertEqual(summary["transcript_kind"], "heartbeat")
        self.assertEqual(summary["status_kind"], "heartbeat")
        self.assertNotIn("Tool output", summary["text"])
        self.assertIn("accepted", summary["text"])

    def test_summarize_session_sync_intermediate_message_humanizes_generic_tool_name(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "toolCall",
                        "name": "exec",
                        "arguments": {
                            "command": "ls",
                        },
                    }
                ],
            }
        )

        self.assertTrue(summary["is_intermediate"])
        self.assertEqual(summary["transcript_kind"], "tool_call")
        self.assertTrue(summary["text"].startswith("Exec"))

    def test_summarize_session_sync_intermediate_message_pretty_prints_json_string_tool_result(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "toolResult",
                "toolName": "session_status",
                "content": "{\"status\":\"error\",\"message\":\"connection refused\"}",
            }
        )

        self.assertTrue(summary["is_intermediate"])
        self.assertEqual(summary["transcript_kind"], "status")
        self.assertIn('"status": "error"', summary["text"])
        self.assertIn('"message": "connection refused"', summary["text"])
        self.assertNotIn("Tool output", summary["text"])

    def test_summarize_session_sync_intermediate_message_treats_plain_text_tool_result_status_as_status(self):
        m = lanying_openclaw

        summary = m.summarize_session_sync_intermediate_message(
            {
                "role": "toolResult",
                "content": "处理开始",
            }
        )

        self.assertTrue(summary["is_intermediate"])
        self.assertEqual(summary["transcript_kind"], "status")
        self.assertEqual(summary["status_kind"], "status")
        self.assertEqual(summary["text"], "处理开始")
        self.assertNotIn("Tool output", summary["text"])

    def test_resolve_session_transcript_intermediate_payload_classifies_plain_text_heartbeat(self):
        m = lanying_openclaw

        poll_summary = m.resolve_session_transcript_intermediate_payload(
            {
                "source": "control_ui_user",
                "transcript_kind": "heartbeat",
                "status_kind": "heartbeat",
                "intermediate_text": "[OpenClaw heartbeat poll]",
            },
            {
                "role": "user",
                "content": "[OpenClaw heartbeat poll]",
            },
        )
        self.assertEqual(poll_summary["transcript_kind"], "heartbeat")
        self.assertEqual(poll_summary["status_kind"], "heartbeat")
        self.assertTrue(poll_summary["is_intermediate"])

        ok_summary = m.resolve_session_transcript_intermediate_payload(
            {
                "source": "control_ui_reply",
                "transcript_kind": "text",
                "intermediate_text": "HEARTBEAT_OK",
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": "HEARTBEAT_OK",
                    }
                ],
            },
        )
        self.assertEqual(ok_summary["transcript_kind"], "heartbeat")
        self.assertEqual(ok_summary["status_kind"], "heartbeat")
        self.assertTrue(ok_summary["is_intermediate"])

    def test_router_mapping_signal_carries_origin_and_chatbot_user_id(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "user_id": "openclaw-user",
            "session_map_sync": "on",
        }

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=1) as mocked_send:
            result = m.send_session_mapping_signal(
                node_info,
                "session_mapping_sync",
                [
                    {
                        "session_key": "agent:main:subagent:test-child",
                        "group_id": "group-1",
                        "openclaw_user_id": "openclaw-user",
                        "origin_kind": "im_user",
                        "origin_user_id": "sender-user",
                        "chatbot_user_id": "chatbot-user",
                    }
                ],
            )

        self.assertEqual(result["result"], "ok")
        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(
            ext["openclaw"]["mappings"][0]["origin_kind"],
            "im_user",
        )
        self.assertEqual(
            ext["openclaw"]["mappings"][0]["origin_user_id"],
            "sender-user",
        )
        self.assertEqual(
            ext["openclaw"]["mappings"][0]["chatbot_user_id"],
            "chatbot-user",
        )

    def test_session_mapping_signal_is_chunked_under_size_limit(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "user_id": "openclaw-user",
            "session_map_sync": "on",
        }
        large_suffix = "x" * 4000
        mappings = [
            {
                "session_key": f"agent:main:subagent:test-child-{idx}",
                "group_id": f"group-{idx}",
                "openclaw_user_id": "openclaw-user",
                "origin_kind": "im_user",
                "origin_user_id": f"sender-user-{idx}",
                "chatbot_user_id": f"chatbot-{large_suffix}-{idx}",
                "parent_session_key": f"agent:main:session-parent-{large_suffix}-{idx}",
                "root_session_key": f"agent:main:session-root-{large_suffix}-{idx}",
                "effective_target_session_key": f"agent:main:session-target-{large_suffix}-{idx}",
            }
            for idx in range(3)
        ]

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[11, 12, 13]) as mocked_send:
            result = m.send_session_mapping_signal(
                node_info,
                "session_mapping_snapshot",
                mappings,
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["msg_ids"], [11, 12, 13])
        self.assertEqual(result["data"]["chunk_count"], 3)
        self.assertEqual(mocked_send.call_count, 3)
        delivered_mappings = []
        for call in mocked_send.call_args_list:
            ext = call.args[7]["ext"]
            payload_size = len(json.dumps(ext, ensure_ascii=False).encode("utf-8"))
            self.assertLessEqual(payload_size, m.SESSION_MAPPING_SIGNAL_CHUNK_MAX_BYTES)
            delivered_mappings.extend(ext["openclaw"]["mappings"])
        self.assertEqual(
            [mapping["session_key"] for mapping in delivered_mappings],
            [mapping["session_key"] for mapping in mappings],
        )
        self.assertEqual(
            [mapping["effective_target_session_key"] for mapping in delivered_mappings],
            [mapping["effective_target_session_key"] for mapping in mappings],
        )

    def test_router_group_materialization_uses_chatbot_user(self):
        m = lanying_openclaw

        with mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join:
            result = m.maybe_materialize_existing_clawchat_group_mapping(
                "app-id",
                {
                    "session_key": "agent:main:clawchat-router:group:group-1",
                    "group_id": "group-1",
                    "chatbot_user_id": "chatbot-user",
                },
                "management-user",
                True,
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(mocked_join.call_args.args[1], "chatbot-user")

    def test_openclaw_group_materialization_does_not_prejoin_management_user(self):
        m = lanying_openclaw

        with mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join:
            result = m.maybe_materialize_existing_clawchat_group_mapping(
                "app-id",
                {
                    "session_key": "agent:main:clawchat:group:group-1",
                    "group_id": "group-1",
                    "chatbot_user_id": "",
                },
                "management-user",
                True,
            )

        self.assertEqual(result["result"], "ok")
        mocked_join.assert_not_called()

    def test_ensure_session_mapping_openclaw_group_root_does_not_prejoin_management_user(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "parent_session_key": "",
                 "root_session_key": "agent:main:clawchat:group:group-1",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "openclaw_control",
                 "origin_user_id": "",
                 "chatbot_user_id": "",
                 "source": "control_ui",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join:
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:clawchat:group:group-1",
                root_session_key="agent:main:clawchat:group:group-1",
                observed_origin={
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "provenance",
                    "observed_message_text": "哈哈",
                },
                should_materialize_clawchat_group=True,
            )

        self.assertEqual(result["result"], "ok")
        mocked_join.assert_not_called()

    def test_ensure_session_mapping_router_group_im_inbound_does_not_prejoin_management_user(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "parent_session_key": "",
                 "root_session_key": "agent:main:clawchat-router:group:group-1",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "im_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
                 "source": "explicit",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=True) as mocked_ensure_admin, \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join:
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:clawchat-router:group:group-1",
                observed_origin={
                    "observed_sender_user_id": "sender-user",
                    "observed_from_user_id": "sender-user",
                    "observed_to_id": "group-1",
                    "observed_chat_type": "group",
                    "observed_channel": "clawchat-router",
                    "observed_message_type": "im_inbound_user",
                    "observed_message_text": "",
                },
                should_materialize_clawchat_group=False,
            )

        self.assertEqual(result["result"], "ok")
        mocked_ensure_admin.assert_not_called()
        mocked_join.assert_not_called()

    def test_ensure_session_mapping_router_group_child_session_does_not_add_management_user(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "session_key": "agent:main:subagent:test-child",
                 "parent_session_key": "agent:main:clawchat-router:group:group-1",
                 "root_session_key": "agent:main:clawchat-router:group:group-1",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "im_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
                 "source": "explicit",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "create_openclaw_session_group", return_value="group-9"), \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=True) as mocked_ensure_admin_sync, \
             mock.patch.object(m, "ensure_user_group_admin") as mocked_ensure_admin_async, \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join, \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"):
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:subagent:test-child",
                parent_session_key="agent:main:clawchat-router:group:group-1",
                root_session_key="agent:main:clawchat-router:group:group-1",
                observed_origin={
                    "observed_sender_user_id": "sender-user",
                    "observed_from_user_id": "sender-user",
                    "observed_to_id": "group-1",
                    "observed_chat_type": "group",
                    "observed_channel": "clawchat-router",
                    "observed_message_type": "im_inbound_user",
                    "observed_message_text": "[Subagent Task]: hi",
                },
            )

        self.assertEqual(result["result"], "ok")
        mocked_ensure_admin_sync.assert_not_called()
        mocked_ensure_admin_async.assert_not_called()
        self.assertEqual(
            [call.args[1] for call in mocked_join.call_args_list],
            ["sender-user", "chatbot-user"],
        )

    def test_ensure_session_mapping_passes_openclaw_session_group_metadata(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "session_key": "agent:main:subagent:test-child",
                 "parent_session_key": "agent:main:clawchat-router:group:group-1",
                 "root_session_key": "agent:main:clawchat-router:group:group-1",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "im_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
                 "source": "explicit",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "create_openclaw_session_group", return_value="group-9") as mocked_create_group, \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=True), \
             mock.patch.object(m, "ensure_user_group_admin"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"):
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:subagent:test-child",
                parent_session_key="agent:main:clawchat-router:group:group-1",
                root_session_key="agent:main:clawchat-router:group:group-1",
                observed_origin={
                    "observed_sender_user_id": "sender-user",
                    "observed_from_user_id": "sender-user",
                    "observed_to_id": "group-1",
                    "observed_chat_type": "group",
                    "observed_channel": "clawchat-router",
                    "observed_message_type": "im_inbound_user",
                    "observed_message_text": "[Subagent Task]: hi",
                },
            )

        self.assertEqual(result["result"], "ok")
        metadata = mocked_create_group.call_args.kwargs["metadata"]
        self.assertEqual(metadata["scene"], "openclaw_session_group")
        self.assertEqual(metadata["peer_user_id"], "sender-user")
        self.assertEqual(metadata["created_by_user_id"], "chatbot-user")
        self.assertGreater(metadata["created_at"], 0)
        self.assertEqual(metadata["name_source"], "openclaw_session_group")
        self.assertEqual(metadata["peer_name_snapshot"], "sender-user")
        self.assertEqual(metadata["session_key"], "agent:main:subagent:test-child")
        self.assertEqual(metadata["root_session_key"], "agent:main:clawchat-router:group:group-1")
        self.assertEqual(metadata["parent_session_key"], "agent:main:clawchat-router:group:group-1")
        self.assertNotIn("node_id", metadata)
        self.assertNotIn("node_name", metadata)
        self.assertNotIn("owner_user_id", metadata)
        self.assertNotIn("origin_kind", metadata)
        self.assertNotIn("origin_user_id", metadata)
        self.assertNotIn("effective_target_session_key", metadata)
        self.assertNotIn("mapping_mode", metadata)
        self.assertEqual(mocked_create_group.call_args.kwargs["log_context"]["session_key"], "agent:main:subagent:test-child")

    def test_serialize_openclaw_session_group_metadata_value_stays_within_group_ext_limit(self):
        m = lanying_openclaw
        value = m.serialize_openclaw_session_group_metadata_value({
            "scene": "openclaw_session_group",
            "peer_user_id": "6653564517632",
            "created_by_user_id": "6657853037888",
            "created_at": 1710000000000,
            "name_source": "openclaw_session_group",
            "peer_name_snapshot": "6653564517632",
            "session_key": "agent:main:subagent:b9ca470b-0802-474a-a558-8410a61f5be7",
            "root_session_key": "agent:main:clawchat-router:group:6653565697409",
            "parent_session_key": "agent:main:clawchat-router:group:6653565697409",
            "effective_target_session_key": "agent:main:subagent:b9ca470b-0802-474a-a558-8410a61f5be7",
            "mapping_mode": "create_temp_group",
        })
        self.assertNotEqual(value, "")
        self.assertLessEqual(len(value), m.OPENCLAW_SESSION_GROUP_METADATA_MAX_LENGTH)
        parsed = json.loads(value)[m.OPENCLAW_SESSION_GROUP_METADATA_KEY]
        self.assertEqual(parsed["scene"], "openclaw_session_group")
        self.assertEqual(parsed["peer_user_id"], "6653564517632")
        self.assertEqual(parsed["created_by_user_id"], "6657853037888")
        self.assertEqual(parsed["created_at"], 1710000000000)
        self.assertEqual(parsed["name_source"], "openclaw_session_group")
        self.assertEqual(parsed["peer_name_snapshot"], "6653564517632")
        self.assertEqual(parsed["session_key"], "agent:main:subagent:b9ca470b-0802-474a-a558-8410a61f5be7")
        self.assertEqual(parsed["root_session_key"], "agent:main:clawchat-router:group:6653565697409")
        self.assertNotIn("parent_session_key", parsed)

    def test_ensure_session_mapping_promotes_management_user_as_group_admin(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "parent_session_key": "",
                 "root_session_key": "agent:main:main",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "openclaw_control",
                 "origin_user_id": "",
                 "chatbot_user_id": "",
                 "source": "control_ui",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "create_openclaw_session_group", return_value="group-9"), \
             mock.patch.object(m, "ensure_session_mapping_group_members", return_value={"result": "ok"}), \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=True) as mocked_ensure_admin, \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"):
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:main",
                observed_origin={
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "provenance",
                    "observed_message_text": "hello",
                },
            )

        self.assertEqual(result["result"], "ok")
        mocked_ensure_admin.assert_called_once_with("app-id", "management-user", "group-9")

    def test_ensure_session_mapping_admin_promotion_failure_blocks_generic_session(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "parent_session_key": "",
                 "root_session_key": "agent:main:main",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "openclaw_control",
                 "origin_user_id": "",
                 "chatbot_user_id": "",
                 "source": "control_ui",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "create_openclaw_session_group", return_value="group-9"), \
             mock.patch.object(m, "ensure_session_mapping_group_members", return_value={"result": "ok"}), \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=False), \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"):
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:main",
                observed_origin={
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "provenance",
                    "observed_message_text": "hello",
                },
            )

        self.assertEqual(result["result"], "error")
        self.assertEqual(result["message"], "management user must join group and become group admin")

    def test_ensure_session_mapping_existing_generic_mapping_requires_management_user_group_admin(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15", "session_map_sync": "on"}

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "parent_session_key": "",
                 "root_session_key": "agent:main:main",
             }), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "resolve_inherited_origin_identity", return_value={
                 "origin_kind": "openclaw_control",
                 "origin_user_id": "",
                 "chatbot_user_id": "",
                 "source": "control_ui",
             }), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value={
                 "session_key": "agent:main:main",
                 "group_id": "group-9",
                 "app_id": "app-id",
                 "node_id": "15",
                 "openclaw_user_id": "openclaw-user",
                 "management_user_id": "management-user",
                 "root_session_key": "agent:main:main",
             }), \
             mock.patch.object(m, "merge_existing_session_mapping", side_effect=lambda existing, lineage, effective_target_session_key, inherited_identity: dict(existing, effective_target_session_key=effective_target_session_key)), \
             mock.patch.object(m, "maybe_materialize_existing_clawchat_group_mapping", return_value={"result": "ok"}), \
             mock.patch.object(m, "ensure_user_group_admin_sync", return_value=True) as mocked_ensure_admin, \
             mock.patch.object(m, "set_session_mapping", side_effect=lambda app_id, node_id, mapping: {
                 "result": "ok",
                 "data": dict(mapping, updated_at=1),
             }), \
             mock.patch.object(m, "sync_session_mapping_to_node"):
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:main",
                observed_origin={
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "provenance",
                    "observed_message_text": "hello",
                },
            )

        self.assertEqual(result["result"], "ok")
        mocked_ensure_admin.assert_called_once_with("app-id", "management-user", "group-9")

    def test_router_direct_assistant_reply_uses_router_reply_signal(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=99) as mocked_send:
            result = m.forward_session_sync_router_direct_reply(
                "app-id",
                node_info,
                "sender-user",
                "child reply",
                {
                    "openclaw": {
                        "type": "session_sync_delivery",
                        "session": "agent:main:clawchat-router:direct:sender-user",
                        "source": "control_ui_reply",
                        "role": "assistant",
                        "request_source": "control_ui_user",
                    },
                    "ai": {
                        "ai_generate": False
                    }
                },
            )

        self.assertEqual(result, 99)
        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[5], 6)
        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(ext["openclaw"]["type"], "router_reply")
        self.assertEqual(ext["openclaw"]["message"]["to"], "sender-user")
        self.assertEqual(ext["openclaw"]["message"]["toType"], "roster")
        payload_ext = json.loads(ext["openclaw"]["message"]["ext"])
        self.assertEqual(payload_ext["openclaw"]["session"], "agent:main:clawchat-router:direct:sender-user")

    def test_router_direct_assistant_reply_strips_reply_to_current_prefix(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=99) as mocked_send:
            m.forward_session_sync_router_direct_reply(
                "app-id",
                node_info,
                "sender-user",
                "[[reply_to_current]] reply content",
                {
                    "openclaw": {
                        "type": "session_sync_delivery",
                        "session": "agent:main:clawchat-router:direct:sender-user",
                        "source": "control_ui_reply",
                        "role": "assistant",
                    },
                    "ai": {
                        "ai_generate": False
                    }
                },
            )

        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(ext["openclaw"]["message"]["content"], "reply content")

    def test_router_reply_message_carries_openclaw_delivery_context(self):
        m = lanying_openclaw
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_utils, "safe_json_loads", return_value={
                 "openclaw": {
                     "type": "session_sync_delivery",
                     "session": "agent:main:clawchat-router:direct:sender-user",
                     "parent_session": "agent:main:session-parent",
                     "root_session": "agent:main:session-root",
                     "source": "control_ui_user",
                     "role": "user",
                     "message_id": "oc-req-1",
                 }
             }), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=100) as mocked_send:
            m.router_reply_message(
                "app-id",
                node_info,
                {
                    "type": "CHAT",
                    "content": "reply content",
                    "msgId": "im-req-1",
                    "ext": '{"openclaw":{"type":"session_sync_delivery","session":"agent:main:clawchat-router:direct:sender-user","source":"control_ui_user","role":"user","message_id":"oc-req-1"}}',
                    "to": {"uid": "sender-user"},
                },
            )

        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(ext["ai"]["role"], "ai")
        self.assertEqual(ext["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(ext["openclaw"]["session"], "agent:main:clawchat-router:direct:sender-user")
        self.assertEqual(ext["openclaw"]["source"], "control_ui_reply")
        self.assertEqual(ext["openclaw"]["role"], "assistant")
        self.assertEqual(ext["openclaw"]["parent_session"], "agent:main:session-parent")
        self.assertEqual(ext["openclaw"]["root_session"], "agent:main:session-root")
        self.assertEqual(ext["openclaw"]["request_source"], "control_ui_user")
        self.assertEqual(ext["openclaw"]["request_role"], "user")
        self.assertEqual(ext["openclaw"]["request_message_id"], "oc-req-1")
        self.assertNotIn("trigger_msg_id", ext["openclaw"])
        self.assertNotIn("request_msg_id", ext["openclaw"])
        m.recent_visible_reply_materialization_by_key.clear()

    def test_router_reply_message_strips_reply_to_current_prefix_before_send(self):
        m = lanying_openclaw
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_utils, "safe_json_loads", return_value={
                 "openclaw": {
                     "type": "session_sync_delivery",
                     "session": "agent:main:clawchat-router:direct:sender-user",
                     "source": "control_ui_reply",
                     "role": "assistant",
                     "message_id": "router-reply-prefix-1",
                 }
             }), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=100) as mocked_send:
            m.router_reply_message(
                "app-id",
                node_info,
                {
                    "type": "CHAT",
                    "content": "[[reply_to_current]] reply content",
                    "msgId": "im-req-1",
                    "ext": '{"openclaw":{"type":"session_sync_delivery","session":"agent:main:clawchat-router:direct:sender-user","source":"control_ui_reply","role":"assistant","message_id":"router-reply-prefix-1"}}',
                    "to": {"uid": "sender-user"},
                },
            )

        self.assertEqual(mocked_send.call_args.args[6], "reply content")
        m.recent_visible_reply_materialization_by_key.clear()

    def test_router_reply_message_suppresses_duplicate_visible_reply_materialization(self):
        m = lanying_openclaw
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        message = {
            "type": "CHAT",
            "content": "reply content",
            "msgId": "im-req-1",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "request_msg_id": "im-request-1",
                }
            }),
            "to": {"uid": "sender-user"},
        }

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=100) as mocked_send:
            m.router_reply_message("app-id", node_info, message)
            m.router_reply_message("app-id", node_info, message)

        self.assertEqual(mocked_send.call_count, 1)
        m.recent_visible_reply_materialization_by_key.clear()

    def test_router_reply_message_finishes_ai_dynamic_before_final_visible_reply(self):
        m = lanying_openclaw
        m.recent_visible_reply_materialization_by_key.clear()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-9",
            "origin_kind": "group",
            "group_id": "group-9",
            "management_user_id": "chatbot-user",
            "chatbot_user_id": "chatbot-user",
            "root_session_key": "agent:main:clawchat-router:group:group-9",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-9",
        }
        target = {
            "kind": "group",
            "mapping": mapping,
        }
        m.remember_request_debug_stream_state(
            "im-request-1",
            777,
            5,
            "[蓝莺AI][14:29:06.690] 处理开始\n\nExec\nTool input\n{}",
        )
        message = {
            "type": "GROUPCHAT",
            "content": "final reply content",
            "msgId": "router-reply-1",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat-router:group:group-9",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "router-reply-1",
                    "request_msg_id": "im-request-1",
                }
            }),
            "to": {"uid": "group-9"},
        }

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "ensure_user_group_admin", return_value=True), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 902]) as mocked_send:
            m.router_reply_message("app-id", node_info, message)

        self.assertEqual(mocked_send.call_count, 1)
        self.assertEqual(mocked_send.call_args.args[6], "final reply content")
        m.recent_visible_reply_materialization_by_key.clear()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()

    def test_router_root_direct_assistant_sync_prefers_router_reply(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "im_user",
            "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=1) as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "child reply",
                    },
                },
            )

        mocked_router_reply.assert_called_once()
        mocked_direct.assert_not_called()

    def test_control_ui_reply_no_reply_is_not_forwarded_to_direct_target(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "resolve_effective_session_sync_target") as mocked_target, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": " NO_REPLY ",
                    },
                },
            )

        mocked_target.assert_not_called()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_not_called()

    def test_control_ui_reply_no_reply_is_not_forwarded_to_router_reply(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "resolve_effective_session_sync_target") as mocked_target, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "no_reply",
                    },
                },
            )

        mocked_target.assert_not_called()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()

    def test_router_root_direct_control_ui_user_sync_uses_direct_forwarding(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
            "management_user_id": "management-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": mapping}), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=1) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_user",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "root_session": "agent:main:clawchat-router:direct:sender-user",
                    "observed_message_type": "control_ui_user",
                    "message": {
                        "role": "user",
                        "content": "你是谁",
                    },
                },
            )

        mocked_direct.assert_called_once()
        self.assertEqual(mocked_direct.call_args.args[2], "sender-user")
        self.assertEqual(mocked_direct.call_args.args[3], "sender-user")
        self.assertEqual(mocked_direct.call_args.args[5], "user")

    def test_legacy_router_root_session_mode_is_router_direct(self):
        m = lanying_openclaw

        mode = m.resolve_root_session_sync_mode(
            m.parse_clawchat_session_identity("agent:main:router:direct:6632092019520")
        )

        self.assertEqual(mode, "router_direct")

    def test_nested_assistant_reply_prefers_parent_group_over_direct_root(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        child_mapping = {
            "session_key": "agent:main:subagent:test-grandchild",
            "parent_session_key": "agent:main:subagent:test-parent",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        parent_mapping = {
            "session_key": "agent:main:subagent:test-parent",
            "group_id": "parent-group",
        }

        def _mapping_lookup(app_id, node_id, session_key):
            if session_key == "agent:main:subagent:test-grandchild":
                return child_mapping
            if session_key == "agent:main:subagent:test-parent":
                return parent_mapping
            return None

        with mock.patch.object(m, "get_session_mapping_by_session", side_effect=_mapping_lookup), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "im_user",
            "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=1) as mocked_direct_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=1) as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=1) as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-grandchild",
                    "message": {
                        "role": "assistant",
                        "content": "nested child reply",
                    },
                },
            )

        mocked_group_reply.assert_not_called()
        mocked_direct_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_called_once_with(
            "app-id",
            node_info,
            parent_mapping,
            "assistant",
            "nested child reply",
            {
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:subagent:test-grandchild",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "parent_session": "agent:main:subagent:test-parent",
                    "root_session": "agent:main:clawchat-router:direct:sender-user",
                },
                "ai": {
                    "ai_generate": False,
                }
            },
        )

    def test_control_ui_reply_materializes_missing_child_mapping_before_forward(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        child_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "temporary-session-group",
            "parent_session_key": "agent:main:clawchat-router:group:group-42",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "ensure_session_mapping", return_value={
                 "result": "ok",
                 "data": child_mapping,
             }) as mocked_ensure_mapping, \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "session_key": "agent:main:subagent:test-child",
                 "parent_session_key": "agent:main:clawchat-router:group:group-42",
                 "root_session_key": "agent:main:clawchat-router:group:group-42",
             }), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": child_mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=1) as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "parent_session": "agent:main:clawchat-router:group:group-42",
                    "root_session": "agent:main:clawchat-router:group:group-42",
                    "message_id": "evt-1",
                    "message": {
                        "role": "assistant",
                        "content": "reply should materialize child mapping first",
                    },
                },
            )

        mocked_ensure_mapping.assert_called_once_with(
            "app-id",
            node_info,
            "agent:main:subagent:test-child",
            "agent:main:clawchat-router:group:group-42",
            "agent:main:clawchat-router:group:group-42",
            {
                "observed_sender_user_id": "",
                "observed_from_user_id": "",
                "observed_to_id": "",
                "observed_chat_type": "",
                "observed_channel": "",
                "observed_message_type": "",
                "observed_message_type_source": "",
                "sync_variant": "",
                "observed_message_text": "reply should materialize child mapping first",
            },
            True,
        )
        mocked_group_forward.assert_called_once_with(
            "app-id",
            node_info,
            child_mapping,
            "assistant",
            "reply should materialize child mapping first",
            {
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:subagent:test-child",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "evt-1",
                    "parent_session": "agent:main:clawchat-router:group:group-42",
                    "root_session": "agent:main:clawchat-router:group:group-42",
                },
                "ai": {
                    "ai_generate": False,
                }
            },
        )

    def test_session_transcript_observed_materializes_child_mapping_with_inherited_origin_facts(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "api_version": "3"}
        parent_mapping = {
            "session_key": "agent:main:clawchat-router:group:group-42",
            "group_id": "group-42",
            "origin_user_id": "real-user",
            "chatbot_user_id": "chatbot-user",
        }
        child_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "temporary-session-group",
            "parent_session_key": "agent:main:clawchat-router:group:group-42",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        def _mapping_lookup(app_id, node_id, session_key):
            if session_key == "agent:main:clawchat-router:group:group-42":
                return parent_mapping
            return None

        with mock.patch.object(m, "get_session_mapping_by_session", side_effect=_mapping_lookup), \
             mock.patch.object(m, "ensure_session_mapping", return_value={"result": "ok", "data": child_mapping}) as mocked_ensure_mapping, \
             mock.patch.object(m, "resolve_session_lineage", return_value={
                 "session_key": "agent:main:subagent:test-child",
                 "parent_session_key": "agent:main:clawchat-router:group:group-42",
                 "root_session_key": "agent:main:clawchat-router:group:group-42",
             }), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": child_mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=1):
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:test-child",
                    "parent_session": "agent:main:clawchat-router:group:group-42",
                    "root_session": "agent:main:clawchat-router:group:group-42",
                    "observed_message_type": "control_ui_user",
                    "observed_message_type_source": "fallback",
                    "message": {
                        "role": "user",
                        "content": "[Subagent Context] inherited question",
                    },
                },
            )

        mocked_ensure_mapping.assert_called_once_with(
            "app-id",
            node_info,
            "agent:main:subagent:test-child",
            "agent:main:clawchat-router:group:group-42",
            "agent:main:clawchat-router:group:group-42",
            {
                "observed_sender_user_id": "real-user",
                "observed_from_user_id": "real-user",
                "observed_to_id": "chatbot-user",
                "observed_chat_type": "groupchat",
                "observed_channel": "clawchat",
                "observed_message_type": "im_inbound_user",
                "observed_message_type_source": "inherited_mapping",
                "sync_variant": "im_subagent_bootstrap",
                "observed_message_text": "[Subagent Context] inherited question",
            },
            True,
        )

    def test_handle_client_event_accepts_session_transcript_observed(self):
        m = lanying_openclaw
        event = {
            "type": "session_transcript_observed",
            "session": "agent:main:clawchat:direct:user-1",
            "source": "control_ui_user",
            "message": {
                "role": "user",
                "content": "hello",
            },
        }
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "api_version": "3"}

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "handle_session_message_sync_event") as mocked_handler:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_handler.assert_called_once_with("app-id", node_info, event)

    def test_handle_session_message_sync_event_skips_plugin_suppression_hint(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        with mock.patch.object(m, "resolve_effective_session_sync_target") as mocked_target, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "suppression_reason": "duplicate_parent_after_subagent",
                    "message": {
                        "role": "assistant",
                        "content": "same parent answer",
                    },
                },
            )

        mocked_target.assert_not_called()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_not_called()

    def test_handle_session_message_sync_event_skips_internal_runtime_suppression_marker(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        with mock.patch.object(m, "get_session_mapping_by_session") as mocked_mapping, \
             mock.patch.object(m, "ensure_session_mapping") as mocked_ensure, \
             mock.patch.object(m, "resolve_effective_session_sync_target") as mocked_target, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:main",
                    "message_id": "internal-reply-1",
                    "suppression_reason": "internal_runtime_context_reply",
                    "observed_message_type": "internal_runtime_context_reply",
                    "observed_message_type_source": "plugin_suppression",
                    "message": {
                        "role": "assistant",
                        "content": "reply already delivered through normal outbound",
                    },
                },
            )

        mocked_mapping.assert_not_called()
        mocked_ensure.assert_not_called()
        mocked_target.assert_not_called()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_not_called()

    def test_handle_session_message_sync_event_skips_prompt_context_suppression_marker(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        with mock.patch.object(m, "get_session_mapping_by_session") as mocked_mapping, \
             mock.patch.object(m, "ensure_session_mapping") as mocked_ensure, \
             mock.patch.object(m, "resolve_effective_session_sync_target") as mocked_target, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_user",
                    "session": "agent:main:subagent:child-1",
                    "message_id": "prompt-context-user-1",
                    "suppression_reason": "prompt_context_envelope",
                    "observed_message_type": "prompt_context_envelope",
                    "observed_message_type_source": "plugin_suppression",
                    "message": {
                        "role": "user",
                        "content": "[Retrieved knowledge context]\n...\n[Current message]\n你好",
                    },
                },
            )

        mocked_mapping.assert_not_called()
        mocked_ensure.assert_not_called()
        mocked_target.assert_not_called()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_not_called()

    def test_handle_session_message_sync_event_skips_plugin_owned_visible_delivery(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat-router:group:group-42",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
        }
        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping) as mocked_mapping, \
             mock.patch.object(m, "ensure_session_mapping") as mocked_ensure, \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:clawchat-router:group:group-42",
                 "mapping": mapping,
             }) as mocked_target, \
             mock.patch.object(m, "maybe_deliver_session_transcript_ai_dynamic", return_value={"result": "not_intermediate"}) as mocked_ai_dynamic, \
             mock.patch.object(m, "maybe_finish_session_transcript_ai_dynamic") as mocked_finish, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "message_id": "root-reply-1",
                    "visible_delivery_owner": "plugin",
                    "visible_delivery_reason": "normal_channel_reply",
                    "message": {
                        "role": "assistant",
                        "content": "root reply already sent by plugin",
                    },
                },
            )

        mocked_mapping.assert_called()
        mocked_ensure.assert_not_called()
        mocked_target.assert_called_once()
        mocked_ai_dynamic.assert_called_once()
        mocked_finish.assert_called_once()
        mocked_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_not_called()

    def test_handle_session_message_sync_event_suppresses_duplicate_transcript_materialization(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "openclaw-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat-router:direct:sender-user",
            "message_id": "duplicate-transcript-1",
            "message": {
                "role": "assistant",
                "content": "duplicate transcript content",
            },
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "openclaw-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=0), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct:
            m.handle_session_message_sync_event("app-id", node_info, event)
            m.handle_session_message_sync_event("app-id", node_info, event)

        self.assertEqual(mocked_direct.call_count, 1)
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()

    def test_handle_session_message_sync_event_does_not_suppress_distinct_message_ids(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "openclaw-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "openclaw-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=0), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "distinct-transcript-1",
                    "message": {
                        "role": "assistant",
                        "content": "same text different id",
                    },
                },
            )
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "distinct-transcript-2",
                    "message": {
                        "role": "assistant",
                        "content": "same text different id",
                    },
                },
            )

        self.assertEqual(mocked_direct.call_count, 2)
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()

    def test_router_reply_visible_materialization_is_suppressed_after_direct_transcript_delivery(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "openclaw-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "openclaw-user",
             }), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct, \
             mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=100) as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:direct:sender-user",
                    "message_id": "reply-visible-1",
                    "trigger_msg_id": "im-request-42",
                    "message": {
                        "role": "assistant",
                        "content": "same visible reply",
                    },
                },
            )
            m.router_reply_message(
                "app-id",
                node_info,
                {
                    "type": "CHAT",
                    "content": "same visible reply",
                    "msgId": "router-visible-1",
                    "ext": json.dumps({
                        "openclaw": {
                            "type": "session_sync_delivery",
                            "session": "agent:main:clawchat:direct:sender-user",
                            "source": "control_ui_reply",
                            "role": "assistant",
                            "request_msg_id": "im-request-42",
                        }
                    }),
                    "to": {"uid": "sender-user"},
                },
            )

        mocked_direct.assert_called_once()
        mocked_send.assert_not_called()
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()

    def test_direct_transcript_visible_materialization_is_suppressed_after_router_reply(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "openclaw-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=100) as mocked_send, \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "openclaw-user",
             }), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct:
            m.router_reply_message(
                "app-id",
                node_info,
                {
                    "type": "CHAT",
                    "content": "same visible reply",
                    "msgId": "router-visible-2",
                    "ext": json.dumps({
                        "openclaw": {
                            "type": "session_sync_delivery",
                            "session": "agent:main:clawchat:direct:sender-user",
                            "source": "control_ui_reply",
                            "role": "assistant",
                            "request_msg_id": "im-request-43",
                        }
                    }),
                    "to": {"uid": "sender-user"},
                },
            )
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:direct:sender-user",
                    "message_id": "reply-visible-2",
                    "trigger_msg_id": "im-request-43",
                    "message": {
                        "role": "assistant",
                        "content": "same visible reply",
                    },
                },
            )

        mocked_send.assert_called_once()
        mocked_direct.assert_not_called()
        m.recent_session_transcript_materialization_by_key.clear()
        m.recent_visible_reply_materialization_by_key.clear()

    def test_handle_session_message_sync_event_keeps_legacy_root_reply_without_owner_hint(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "openclaw-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "openclaw-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=0), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "legacy-root-reply-1",
                    "message": {
                        "role": "assistant",
                        "content": "legacy root reply keeps old connector behavior",
                    },
                },
            )

        mocked_direct.assert_called_once()

    def test_handle_session_message_sync_event_keeps_subagent_assistant_reply_sync(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "parent_session_key": "agent:main:clawchat-router:group:group-42",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={"kind": "group", "mapping": mapping}), \
             mock.patch.object(m, "should_forward_group_sync_via_router_reply", return_value=None), \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=301) as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "parent_session": "agent:main:clawchat-router:group:group-42",
                    "root_session": "agent:main:clawchat-router:group:group-42",
                    "message_id": "child-reply-1",
                    "message": {
                        "role": "assistant",
                        "content": "child result should still sync",
                    },
                },
            )

        mocked_group.assert_called_once()

    def test_handle_session_message_sync_event_routes_tool_call_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "tool-call-1",
                    "trigger_msg_id": "request-1",
                    "transcript_kind": "yield",
                    "tool_name": "sessions_yield",
                    "intermediate_text": "Yield\nTool input\n{\"message\":\"waiting\"}",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "name": "sessions_yield",
                                "arguments": {
                                    "message": "waiting",
                                },
                            }
                        ],
                    },
                },
            )

        mocked_direct.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(mocked_send.call_args.args[5], 0)
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertIn("Yield", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_summarize_session_sync_tool_call_context_line_formats_exec_for_readability(self):
        m = lanying_openclaw
        context_line = m.summarize_session_sync_tool_call_context_line(
            "exec",
            {
                "command": "ls -la ~/.openclaw/workspace",
                "timeout": 120000,
            },
        )

        self.assertEqual(context_line, "")

    def test_summarize_session_sync_tool_call_heading_formats_exec_for_readability(self):
        m = lanying_openclaw
        heading = m.summarize_session_sync_tool_call_heading(
            "exec",
            {
                "command": "ls -la ~/.openclaw/workspace",
                "timeout": 120000,
            },
        )

        self.assertEqual(heading, "Exec `ls -la ~/.openclaw/workspace`")

    def test_summarize_session_sync_intermediate_message_formats_exec_tool_call_as_markdown(self):
        m = lanying_openclaw
        summary = m.summarize_session_sync_intermediate_message({
            "content": [
                {
                    "type": "toolCall",
                    "name": "exec",
                    "arguments": {
                        "command": "ls -la ~/.openclaw/workspace",
                        "timeout": 120000,
                    },
                }
            ],
        })

        self.assertEqual(summary["transcript_kind"], "tool_call")
        self.assertIn("Exec `ls -la ~/.openclaw/workspace`", summary["text"])
        self.assertIn("Tool input", summary["text"])
        self.assertIn("```json", summary["text"])
        self.assertIn('"command": "ls -la ~/.openclaw/workspace"', summary["text"])

    def test_handle_session_message_sync_event_routes_tool_result_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        m.remember_request_debug_stream_state("request-tool-result-1", 777, 2, "[蓝莺AI] 处理完成")

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "tool-result-1",
                    "trigger_msg_id": "request-tool-result-1",
                    "transcript_kind": "tool_result",
                    "tool_name": "exec",
                    "intermediate_text": "Exec\n\nTool output\nworkspace-state.json",
                    "message": {
                        "role": "toolResult",
                        "toolName": "exec",
                        "content": "workspace-state.json",
                    },
                },
            )

        mocked_direct.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(mocked_send.call_args.args[5], 0)
        self.assertNotIn("related_mid", sent_extra)
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertEqual(sent_extra["ext"]["ai"]["request_msg_id"], "request-tool-result-1")
        self.assertEqual(sent_extra["ext"]["ai"]["seq"], 1)
        self.assertNotIn("[蓝莺AI] 处理完成", mocked_send.call_args.args[6])
        self.assertEqual(mocked_send.call_args.args[6], "[蓝莺AI] Exec")
        self.assertNotIn("workspace-state.json", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()

    def test_ai_dynamic_event_id_dedupes_with_redis(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "run-1:tool_result:call-1:4",
            "event_id": "agent:main:clawchat:group:group-42|run-1|tool_result|call-1|4",
            "stream_id": "agent:main:clawchat:group:group-42|run-1",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nSuccessfully wrote 171 bytes to IDENTITY.md",
        }
        message = {"role": "toolResult", "toolName": "exec", "content": "Successfully wrote 171 bytes to IDENTITY.md"}
        delivery_ext = {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}}

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send:
            first = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id", node_info, mapping, target, None, event, message, delivery_ext,
            )
            second = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id", node_info, mapping, target, None, event, message, delivery_ext,
            )

        self.assertEqual(first["result"], "delivered")
        self.assertEqual(second["result"], "duplicate")
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertNotIn("request_msg_id", sent_extra["ext"]["ai"])
        self.assertEqual(sent_extra["ext"]["ai"]["stream_id"], "agent:main:clawchat:group:group-42|run-1")
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_ai_dynamic_event_id_dedupe_is_not_scoped_by_node_id(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        event = {
            "session": "agent:main:clawchat:group:group-42",
            "event_id": "agent:main:clawchat:group:group-42|run-1|tool_result|call-1|4",
            "message_id": "run-1:tool_result:call-1:4",
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis):
            first = m.reserve_session_ai_dynamic_delivery("app-id", "15", event, "tool_result")
            second = m.reserve_session_ai_dynamic_delivery("app-id", "16", event, "tool_result")

        self.assertFalse(first.get("duplicate"))
        self.assertTrue(second.get("duplicate"))

    def test_ai_dynamic_request_msg_id_takes_over_connector_debug_message(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "request_msg_id": "request-1",
                "status_bar": True,
                "is_debug": False,
            },
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "run-1:tool_result:call-1:4",
            "event_id": "agent:main:clawchat:group:group-42|run-1|tool_result|call-1|4",
            "stream_id": "agent:main:clawchat:group:group-42|run-1",
            "request_msg_id": "request-1",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nSuccessfully wrote 171 bytes to IDENTITY.md",
        }
        message = {"role": "toolResult", "toolName": "exec", "content": "Successfully wrote 171 bytes to IDENTITY.md"}
        delivery_ext = {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}}

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send:
            m.remember_request_debug_stream_state(
                "request-1",
                777,
                1,
                "[蓝莺AI] 处理开始",
                app_id="app-id",
                target_kind="group",
                target_id="group-42",
                target_sender_id="openclaw-user",
            )
            result = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id", node_info, mapping, target, None, event, message, delivery_ext,
            )

        self.assertEqual(result["result"], "delivered")
        mocked_send.assert_called_once()
        self.assertEqual(mocked_send.call_args.args[5], 11)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 777)
        sent_ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(sent_ext["ai"]["request_msg_id"], "request-1")
        self.assertEqual(sent_ext["ai"]["stream_id"], "request-1")

    def test_ai_dynamic_request_stream_key_ignores_session_when_request_msg_id_exists(self):
        m = lanying_openclaw
        target_config = {
            "target_kind": "group",
            "target_id": "group-42",
        }
        parent_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            {"node_id": "15"},
            {"session": "agent:main:clawchat:group:group-42"},
            "request-same-1",
            target_config,
            request_msg_id="request-same-1",
        )
        child_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            {"node_id": "15"},
            {"session": "agent:main:subagent:child"},
            "request-same-1",
            target_config,
            request_msg_id="request-same-1",
        )

        self.assertEqual(parent_key, child_key)

    def test_ai_dynamic_request_debug_state_is_scoped_by_request_msg_id(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        m.remember_request_debug_stream_state(
            "request-sender-1",
            777,
            1,
            "[蓝莺AI] 处理开始",
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        first = m.resolve_request_debug_stream_state(
            "request-sender-1",
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-b",
            redis=fake_redis,
        )
        second = m.resolve_request_debug_stream_state(
            "request-sender-1",
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        self.assertEqual(first["last_msg_id"], 777)
        self.assertEqual(second["last_msg_id"], 777)

    def test_remember_request_debug_stream_state_does_not_overwrite_newer_stream_state(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "",
            "request-sender-2",
            "group",
            "group-42",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 888,
            "content": "[蓝莺AI] newer status",
            "seq": 2,
            "updated_at": 2000,
        }, fake_redis))

        m.remember_request_debug_stream_state(
            "request-sender-2",
            777,
            1,
            "[蓝莺AI] older status",
            now_ms=1000,
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        state = m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)
        self.assertEqual(state["last_msg_id"], 888)
        self.assertEqual(state["seq"], 2)
        self.assertEqual(state["content"], "[蓝莺AI] newer status")

    def test_remember_request_debug_stream_state_does_not_drop_existing_items(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "",
            "request-sender-3",
            "group",
            "group-42",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 888,
            "content": "[蓝莺AI][12:00:00.000] 处理开始\n[蓝莺AI][12:00:01.000] Sub-agent",
            "items": [
                {
                    "event_id": "event-1",
                    "seq": 1,
                    "sort_ts": 1000,
                    "rendered_text": "[蓝莺AI][12:00:00.000] 处理开始",
                },
                {
                    "event_id": "event-2",
                    "seq": 2,
                    "sort_ts": 2000,
                    "rendered_text": "[蓝莺AI][12:00:01.000] Sub-agent",
                },
            ],
            "seq": 2,
            "updated_at": 2000,
        }, fake_redis))

        m.remember_request_debug_stream_state(
            "request-sender-3",
            999,
            3,
            "[蓝莺AI][12:00:02.000] 处理完成",
            now_ms=3000,
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        state = m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)
        self.assertEqual(state["last_msg_id"], 999)
        self.assertEqual(state["seq"], 3)
        self.assertEqual(len(state["items"]), 2)
        self.assertEqual(state["items"][0]["event_id"], "event-1")
        self.assertEqual(state["items"][1]["event_id"], "event-2")
        self.assertEqual(
            state["content"],
            "[蓝莺AI][12:00:00.000] 处理开始\n\n[蓝莺AI][12:00:01.000] Sub-agent",
        )

    def test_remember_request_debug_stream_state_freezes_connector_prefix_after_runtime_starts(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "",
            "request-sender-4",
            "group",
            "group-42",
        )
        connector_prefix = (
            "[蓝莺AI][12:00:00.000] 处理开始\n\n"
            "[蓝莺AI][12:00:00.100] 当前预设为: openclaw-jp"
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 888,
            "content": f"{connector_prefix}\n\n[蓝莺AI][12:00:01.000] Exec `pwd`",
            "items": [
                {
                    "event_id": "__request_debug_snapshot__",
                    "stream_id": "request-sender-4",
                    "transcript_kind": "status",
                    "status_kind": "status",
                    "text": connector_prefix,
                    "rendered_text": connector_prefix,
                },
                {
                    "event_id": "event-runtime-1",
                    "stream_id": "request-sender-4",
                    "transcript_kind": "tool_call",
                    "tool_name": "exec",
                    "text": "Exec `pwd`",
                    "rendered_text": "[蓝莺AI][12:00:01.000] Exec `pwd`",
                    "message_timestamp": 1001,
                    "received_at": 1001,
                },
            ],
            "seq": 3,
            "updated_at": 2000,
        }, fake_redis))

        m.remember_request_debug_stream_state(
            "request-sender-4",
            888,
            4,
            f"{connector_prefix}\n\n[蓝莺AI][12:00:02.000] 处理完成",
            now_ms=3000,
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        state = m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)
        self.assertEqual(state["items"][0]["rendered_text"], connector_prefix)
        self.assertNotIn("处理完成", state["content"])
        self.assertTrue(state["content"].endswith("[蓝莺AI][12:00:01.000] Exec `pwd`"))

    def test_ai_dynamic_debug_mode_appends_after_connector_debug_message(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-debug-1",
            "group",
            "group-42",
        )
        target_config = {
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "target_kind": "group",
            "target_id": "group-42",
            "request_msg_id": "request-debug-1",
            "status_bar": False,
            "is_debug": True,
        }
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 777,
            "content": "[蓝莺AI][12:00:00.000] 处理开始\n",
            "seq": 1,
            "updated_at": 1000,
        }, fake_redis))

        with mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=778) as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "event-debug-2",
                "stream_id": "request-debug-1",
                "transcript_kind": "tool_result",
                "text": "Exec\n\nTool output\nok",
                "target_config": target_config,
                "delivery_ext": {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
                "request_msg_id": "request-debug-1",
            }, fake_redis, 2000)

        self.assertEqual(result["send_count"], 1)
        self.assertEqual(mocked_send.call_args.args[5], 11)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 777)
        self.assertEqual(mocked_send.call_args.args[7]["online_only"], True)
        self.assertTrue(mocked_send.call_args.args[6].startswith("\n"))
        self.assertIn("Tool output", mocked_send.call_args.args[6])
        self.assertNotIn("[蓝莺AI][12:00:00.000] 处理开始", mocked_send.call_args.args[6])
        state = m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)
        self.assertEqual(len(state["items"]), 2)
        self.assertEqual(state["items"][0]["event_id"], "__preserved_existing_content__")
        self.assertIn("[蓝莺AI][12:00:00.000] 处理开始", state["content"])

    def test_ai_dynamic_debug_mode_suppresses_duplicate_filtered_append(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-debug-duplicate-start-1",
            "group",
            "group-42",
        )
        target_config = {
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "target_kind": "group",
            "target_id": "group-42",
            "request_msg_id": "request-debug-duplicate-start-1",
            "status_bar": False,
            "is_debug": True,
        }
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 777,
            "content": "[蓝莺AI][12:00:00.000] 处理开始",
            "items": [
                {
                    "event_id": "__request_debug_snapshot__",
                    "stream_id": "request-debug-duplicate-start-1",
                    "transcript_kind": "status",
                    "tool_name": "",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][12:00:00.000] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1000,
                },
            ],
            "seq": 1,
            "updated_at": 1000,
        }, fake_redis))

        with mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "event-debug-duplicate-start-2",
                "stream_id": "request-debug-duplicate-start-1",
                "transcript_kind": "status",
                "status_kind": "status",
                "text": "处理开始",
                "seq_id": 2,
                "message_seq": 2,
                "message_timestamp": 2,
                "target_config": target_config,
                "delivery_ext": {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
                "request_msg_id": "request-debug-duplicate-start-1",
            }, fake_redis, 2000)

        self.assertEqual(result["result"], "suppressed")
        self.assertEqual(result["send_count"], 0)
        mocked_send.assert_not_called()

    def test_ai_dynamic_debug_mode_uses_replace_when_rebuild_reorders_history(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-debug-reorder-1",
            "group",
            "group-42",
        )
        target_config = {
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "target_kind": "group",
            "target_id": "group-42",
            "request_msg_id": "request-debug-reorder-1",
            "status_bar": False,
            "is_debug": True,
        }
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 777,
            "content": "[蓝莺AI][12:00:02.000] Tool output\n```\nok\n```",
            "items": [
                {
                    "event_id": "event-tool-result",
                    "stream_id": "request-debug-reorder-1",
                    "transcript_kind": "tool_result",
                    "tool_name": "exec",
                    "status_kind": "",
                    "text": "Exec\n\nTool output\nok",
                    "rendered_text": "[蓝莺AI][12:00:02.000] Tool output\n```\nok\n```",
                    "seq_id": 4,
                    "message_seq": 4,
                    "message_timestamp": 4,
                    "received_at": 1000,
                },
            ],
            "seq": 1,
            "updated_at": 1000,
        }, fake_redis))

        with mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=778) as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "event-tool-call",
                "stream_id": "request-debug-reorder-1",
                "transcript_kind": "tool_call",
                "tool_name": "exec",
                "text": "Exec\n\nTool input\npwd",
                "seq_id": 2,
                "message_seq": 2,
                "message_timestamp": 2,
                "target_config": target_config,
                "delivery_ext": {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
                "request_msg_id": "request-debug-reorder-1",
            }, fake_redis, 2000)

        self.assertEqual(result["send_count"], 1)
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertIn("Tool input", mocked_send.call_args.args[6])
        self.assertIn("Tool output", mocked_send.call_args.args[6])
        self.assertLess(
            mocked_send.call_args.args[6].find("Tool input"),
            mocked_send.call_args.args[6].find("Tool output"),
        )

    def test_remember_request_debug_stream_state_stores_standardized_snapshot_item(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "",
            "request-debug-structured-1",
            "group",
            "group-42",
        )

        m.remember_request_debug_stream_state(
            "request-debug-structured-1",
            777,
            2,
            "[蓝莺AI][12:00:00.000] 处理开始\n\n[蓝莺AI][12:00:01.000] 当前预设为: openclaw-jp",
            now_ms=1000,
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user-a",
            redis=fake_redis,
        )

        state = m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)
        self.assertEqual(state["last_msg_id"], 777)
        self.assertEqual(state["seq"], 2)
        self.assertEqual(len(state["items"]), 1)
        self.assertEqual(state["items"][0]["event_id"], "__request_debug_snapshot__")
        self.assertIn("当前预设为: openclaw-jp", state["items"][0]["rendered_text"])
        self.assertEqual(state["content"], state["items"][0]["rendered_text"])

    def test_ai_dynamic_direct_delivery_uses_replace_after_first_message(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "agent:main:clawchat:group:group-42|run-1",
            "group",
            "group-42",
        )
        target_config = {
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "target_kind": "group",
            "target_id": "group-42",
            "status_bar": True,
            "is_debug": False,
        }
        delivery_ext = {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}}
        with mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 902]) as mocked_send:
            first = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "event-4",
                "stream_id": "agent:main:clawchat:group:group-42|run-1",
                "transcript_kind": "tool_call",
                "text": "Exec\n\nTool input\nfirst",
                "target_config": target_config,
                "delivery_ext": delivery_ext,
            }, fake_redis, 1999)
            second = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "event-5",
                "stream_id": "agent:main:clawchat:group:group-42|run-1",
                "transcript_kind": "tool_result",
                "text": "Exec\n\nTool output\nsecond",
                "target_config": target_config,
                "delivery_ext": delivery_ext,
            }, fake_redis, 2000)

        self.assertEqual(first["result"], "delivered")
        self.assertEqual(second["result"], "delivered")
        self.assertEqual(mocked_send.call_args_list[0].args[5], 0)
        self.assertEqual(mocked_send.call_args_list[1].args[5], 12)
        self.assertEqual(mocked_send.call_args_list[1].args[7]["related_mid"], 901)
        self.assertEqual(m.load_session_ai_dynamic_stream_state(stream_key, fake_redis)["seq"], 2)

    def test_ai_dynamic_send_failure_releases_dedupe(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "event_id": "event-send-fail",
            "message_id": "message-send-fail",
            "stream_id": "agent:main:clawchat:group:group-42|run-fail",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nfailed once",
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[0, 901]):
            failed = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id", node_info, mapping, target, None, event,
                {"role": "toolResult", "toolName": "exec", "content": "failed once"},
                {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
            )
            retried = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id", node_info, mapping, target, None, event,
                {"role": "toolResult", "toolName": "exec", "content": "failed once"},
                {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
            )

        self.assertEqual(failed["result"], "send_failed")
        self.assertEqual(retried["result"], "delivered")

    def test_ai_dynamic_redis_unavailable_does_not_send_im(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "run-1:tool_result:call-1:4",
            "event_id": "agent:main:clawchat:group:group-42|run-1|tool_result|call-1|4",
            "stream_id": "agent:main:clawchat:group:group-42|run-1",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nSuccessfully wrote 171 bytes to IDENTITY.md",
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=None), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send:
            result = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                event,
                {"role": "toolResult", "toolName": "exec", "content": "Successfully wrote 171 bytes to IDENTITY.md"},
                {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
            )

        self.assertEqual(result["result"], "redis_unavailable")
        mocked_send.assert_not_called()

    def test_ai_dynamic_stream_state_uses_redis_across_process_cache(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        delivery_ext = {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}}

        def _event(event_id, message_id, text):
            return {
                "type": "session_transcript_observed",
                "source": "control_ui_reply",
                "session": "agent:main:clawchat:group:group-42",
                "message_id": message_id,
                "event_id": event_id,
                "stream_id": "agent:main:clawchat:group:group-42|run-1",
                "transcript_kind": "tool_result",
                "tool_name": "exec",
                "intermediate_text": text,
            }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 902]) as mocked_send:
            m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                _event("event-1", "tool-output-1", "Exec\n\nTool output\nfirst"),
                {"role": "toolResult", "toolName": "exec", "content": "first"},
                delivery_ext,
            )
            m.recent_session_ai_dynamic_stream_by_key.clear()
            m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                _event("event-2", "tool-output-2", "Exec\n\nTool output\nsecond"),
                {"role": "toolResult", "toolName": "exec", "content": "second"},
                delivery_ext,
            )

        self.assertEqual(mocked_send.call_args_list[0].args[5], 0)
        self.assertEqual(mocked_send.call_args_list[1].args[5], 12)
        self.assertEqual(mocked_send.call_args_list[1].args[7]["related_mid"], 901)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_ai_dynamic_stream_lock_busy_does_not_consume_event_id(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "tool-output-locked",
            "event_id": "event-locked",
            "stream_id": "agent:main:clawchat:group:group-42|run-locked",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nlocked",
        }
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            node_info,
            event,
            event.get("stream_id", ""),
            target_config["data"],
            request_msg_id=event.get("request_msg_id", ""),
        )
        lock_key = m.build_session_ai_dynamic_stream_lock_redis_key(stream_key)
        fake_redis.set(lock_key, "other-token")

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "SESSION_TRANSCRIPT_AI_DYNAMIC_REDIS_LOCK_WAIT_MS", 1):
            busy = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                event,
                {"role": "toolResult", "toolName": "exec", "content": "locked"},
                {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
            )
            fake_redis.delete(lock_key)
            delivered = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                event,
                {"role": "toolResult", "toolName": "exec", "content": "locked"},
                {"openclaw": {"type": "session_sync_delivery"}, "ai": {"ai_generate": False}},
            )

        self.assertEqual(busy["result"], "busy")
        self.assertEqual(delivered["result"], "delivered")
        mocked_send.assert_called_once()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_ai_dynamic_stream_lock_release_does_not_delete_new_owner(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()
        lock_state = {
            "locked": True,
            "redis_key": "lock-key",
            "token": "old-token",
            "redis": fake_redis,
        }
        fake_redis.set("lock-key", "new-token")

        m.release_session_ai_dynamic_stream_distributed_lock(lock_state)

        self.assertEqual(fake_redis.get("lock-key"), "new-token")

    def test_handle_session_message_sync_event_rolls_back_materialization_dedupe_on_ai_dynamic_busy(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "busy-tool-1",
            "event_id": "busy-event-1",
            "stream_id": "agent:main:clawchat:group:group-42|run-busy",
            "transcript_kind": "tool_result",
            "tool_name": "exec",
            "intermediate_text": "Exec\n\nTool output\nbusy",
            "message": {
                "role": "toolResult",
                "toolName": "exec",
                "content": "busy",
            },
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={"kind": "group", "mapping": mapping}), \
             mock.patch.object(m, "maybe_deliver_session_transcript_ai_dynamic", return_value={
                 "result": "busy",
                 "stream_key": "stream-key",
             }) as mocked_dynamic:
            m.handle_session_message_sync_event("app-id", node_info, dict(event))
            m.handle_session_message_sync_event("app-id", node_info, dict(event))

        self.assertEqual(mocked_dynamic.call_count, 2)
        self.assertEqual(m.recent_session_transcript_materialization_by_key, {})

    def test_handle_session_message_sync_event_acquires_processing_lock_before_mapping_for_clawchat(self):
        m = lanying_openclaw
        call_order = []
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:direct:user-1",
            "message_id": "tool-input-1",
            "event_id": "event-tool-input-1",
            "stream_id": "agent:main:clawchat:direct:user-1|run-1",
            "trigger_msg_id": "request-1",
            "seq_id": 2,
            "message_seq": 2,
            "message_timestamp": 1002,
            "transcript_kind": "tool_call",
            "tool_name": "exec",
            "intermediate_text": "Exec\nTool input\n{\n  \"command\": \"pwd\"\n}",
            "message": {
                "role": "assistant",
                "content": [{
                    "type": "toolCall",
                    "id": "call-1",
                    "name": "exec",
                    "arguments": {"command": "pwd"},
                }],
            },
            "visible_delivery_owner": "connector",
            "visible_delivery_reason": "transcript_sync",
        }
        mapping = {
            "session_key": "agent:main:clawchat:direct:user-1",
            "origin_kind": "direct_user",
            "origin_user_id": "user-1",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:direct:user-1",
            "effective_target_session_key": "agent:main:clawchat:direct:user-1",
        }
        fake_lock_state = {
            "locked": True,
            "redis_key": "lock-key",
            "token": "token-1",
            "redis": object(),
            "enabled": True,
        }

        def _record_lock(*args, **kwargs):
            call_order.append("lock")
            return fake_lock_state

        def _record_ensure(*args, **kwargs):
            call_order.append("ensure")
            return {"result": "ok", "data": mapping}

        with mock.patch.object(m, "is_session_map_sync_enabled", return_value=True), \
             mock.patch.object(m, "acquire_session_ai_dynamic_stream_lock_until_ready", side_effect=_record_lock), \
             mock.patch.object(m, "release_session_ai_dynamic_stream_distributed_lock"), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "ensure_session_mapping", side_effect=_record_ensure), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "user-1",
                 "origin_user_id": "user-1",
                 "chatbot_user_id": "",
                 "session_key": "agent:main:clawchat:direct:user-1",
             }), \
             mock.patch.object(m, "maybe_deliver_session_transcript_ai_dynamic", return_value={
                 "result": "delivered",
                 "stream_key": "stream-key",
             }):
            m.handle_session_message_sync_event("app-id", node_info, dict(event))

        self.assertEqual(call_order[:2], ["lock", "ensure"])

    def test_maybe_deliver_session_transcript_ai_dynamic_serializes_same_stream_updates(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {
            "kind": "group",
            "session_key": "agent:main:clawchat:group:group-42",
            "mapping": mapping,
        }
        delivery_ext = {
            "openclaw": {
                "type": "session_sync_delivery",
                "session": "agent:main:clawchat:group:group-42",
                "source": "control_ui_reply",
                "role": "assistant",
                "request_msg_id": "request-concurrent-1",
            },
            "ai": {
                "ai_generate": False,
            },
        }
        tool_call_event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "tool-call-concurrent-1",
            "trigger_msg_id": "request-concurrent-1",
        }
        tool_call_message = {
            "role": "assistant",
            "content": [
                {
                    "type": "toolCall",
                    "name": "exec",
                    "arguments": {
                        "command": "pwd",
                    },
                }
            ],
        }
        tool_result_event = {
            "type": "session_transcript_observed",
            "source": "control_ui_reply",
            "session": "agent:main:clawchat:group:group-42",
            "message_id": "tool-result-concurrent-1",
            "trigger_msg_id": "request-concurrent-1",
        }
        tool_result_message = {
            "role": "toolResult",
            "toolName": "exec",
            "content": "workspace-state.json",
        }
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-concurrent-1",
                "target_kind": "group",
                "target_id": "group-42",
            },
        }
        send_call_args = []

        def _send_message_sync(*args, **kwargs):
            send_call_args.append(args)
            time.sleep(0.05)
            return 901

        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=_send_message_sync):
            thread_one = threading.Thread(
                target=m.maybe_deliver_session_transcript_ai_dynamic,
                args=("app-id", node_info, mapping, target, None, tool_call_event, tool_call_message, delivery_ext),
            )
            thread_two = threading.Thread(
                target=m.maybe_deliver_session_transcript_ai_dynamic,
                args=("app-id", node_info, mapping, target, None, tool_result_event, tool_result_message, delivery_ext),
            )
            thread_one.start()
            time.sleep(0.01)
            thread_two.start()
            thread_one.join()
            thread_two.join()

        self.assertEqual(len(send_call_args), 2)
        self.assertEqual(send_call_args[0][5], 0)
        self.assertEqual(send_call_args[1][5], 11)
        self.assertEqual(send_call_args[1][7]["related_mid"], 901)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()

    def test_maybe_deliver_session_transcript_ai_dynamic_delivers_in_arrival_order(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {
            "kind": "group",
            "session_key": "agent:main:clawchat:group:group-42",
            "mapping": mapping,
        }
        delivery_ext = {
            "openclaw": {
                "type": "session_sync_delivery",
                "session": "agent:main:clawchat:group:group-42",
                "source": "control_ui_reply",
                "role": "assistant",
                "request_msg_id": "request-arrival-1",
            },
            "ai": {
                "ai_generate": False,
            },
        }
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-arrival-1",
                "target_kind": "group",
                "target_id": "group-42",
            },
        }
        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 902]) as mocked_send:
            first_result = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "tool-result-arrival-1",
                    "trigger_msg_id": "request-arrival-1",
                },
                {
                    "role": "toolResult",
                    "toolName": "exec",
                    "content": "workspace-state.json",
                },
                delivery_ext,
            )
            delivered_result = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "tool-call-arrival-1",
                    "trigger_msg_id": "request-arrival-1",
                },
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "toolCall",
                            "name": "sessions_yield",
                            "arguments": {
                                "message": "Waiting for subagent completion",
                            },
                        }
                    ],
                },
                delivery_ext,
            )

        self.assertEqual(first_result["result"], "delivered")
        self.assertEqual(delivered_result["result"], "delivered")
        self.assertEqual(mocked_send.call_count, 2)
        self.assertIn("Tool output", mocked_send.call_args_list[0].args[6])
        self.assertIn("workspace-state.json", mocked_send.call_args_list[0].args[6])
        self.assertIn("Yield", mocked_send.call_args_list[1].args[6])
        self.assertIn("Waiting for subagent completion", mocked_send.call_args_list[1].args[6])
        self.assertNotIn("Tool output", mocked_send.call_args_list[1].args[6])
        self.assertTrue(mocked_send.call_args_list[1].args[6].startswith("\n"))
        self.assertEqual(mocked_send.call_args_list[1].args[5], 11)
        self.assertEqual(mocked_send.call_args_list[1].args[7]["related_mid"], 901)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()

    def test_parse_session_sync_order_seq_prefers_seq_id_over_message_seq(self):
        m = lanying_openclaw
        self.assertEqual(
            m.parse_session_sync_order_seq({
                "seq_id": "29",
                "message_seq": "30",
            }),
            29,
        )
        self.assertEqual(
            m.parse_session_sync_order_seq({
                "message_seq": "30",
            }),
            30,
        )

    def test_build_session_transcript_dedupe_identity_prefers_plugin_message_id(self):
        m = lanying_openclaw
        self.assertEqual(
            m.build_session_transcript_dedupe_identity({
                "_server_msg_id": "server-mid-1",
                "message_id": "same-mid",
                "message_seq": 23,
            }),
            "mid:same-mid",
        )
        self.assertEqual(
            m.build_session_transcript_dedupe_identity({
                "_server_msg_id": "server-mid-1",
                "message_seq": 23,
            }),
            "server_mid:server-mid-1",
        )
        self.assertEqual(
            m.build_session_transcript_dedupe_identity({
                "message_seq": 23,
            }),
            "seq:23",
        )

    def test_handle_chat_message_injects_server_message_id_into_openclaw_event(self):
        m = lanying_openclaw
        captured = {}

        def _capture_event(event, app_id, user_id, ctype):
            captured["event"] = event
            captured["app_id"] = app_id
            captured["user_id"] = user_id
            captured["ctype"] = ctype

        with mock.patch.object(m, "handle_client_event", side_effect=_capture_event):
            m.handle_chat_message({
                "msgId": "server-mid-42",
                "from": {"uid": "100"},
                "to": {"uid": "100"},
                "appId": "app-id",
                "ctype": "COMMAND",
                "ext": json.dumps({
                    "openclaw": {
                        "type": "session_transcript_observed",
                        "message_id": "plugin-mid-1",
                    }
                }),
            })

        self.assertEqual(captured["event"]["_server_msg_id"], "server-mid-42")
        self.assertEqual(captured["event"]["message_id"], "plugin-mid-1")
        self.assertEqual(captured["app_id"], "app-id")
        self.assertEqual(captured["user_id"], "100")
        self.assertEqual(captured["ctype"], "COMMAND")

    def test_reserve_recent_session_transcript_materialization_dedupes_same_message_id_even_with_new_message_seq(self):
        m = lanying_openclaw
        m.recent_session_transcript_materialization_by_key.clear()

        first_state = m.reserve_recent_session_transcript_materialization(
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "same-mid",
                "message_seq": 19,
                "source": "control_ui_reply",
                "transcript_kind": "tool_call",
            },
            "assistant",
        )
        second_state = m.reserve_recent_session_transcript_materialization(
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "same-mid",
                "message_seq": 20,
                "source": "control_ui_reply",
                "transcript_kind": "tool_call",
            },
            "assistant",
        )
        next_message_state = m.reserve_recent_session_transcript_materialization(
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "next-mid",
                "message_seq": 21,
                "source": "control_ui_reply",
                "transcript_kind": "tool_call",
            },
            "assistant",
        )

        self.assertFalse(first_state["duplicate"])
        self.assertTrue(second_state["duplicate"])
        self.assertFalse(next_message_state["duplicate"])
        m.recent_session_transcript_materialization_by_key.clear()

    def test_reserve_session_ai_dynamic_delivery_dedupes_same_message_id_even_with_new_message_seq(self):
        m = lanying_openclaw
        fake_redis = FakeRedis()

        first_state = m.reserve_session_ai_dynamic_delivery(
            "app-id",
            "15",
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "same-mid",
                "message_seq": 23,
            },
            "yield",
            fake_redis,
        )
        second_state = m.reserve_session_ai_dynamic_delivery(
            "app-id",
            "16",
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "same-mid",
                "message_seq": 24,
            },
            "yield",
            fake_redis,
        )
        next_message_state = m.reserve_session_ai_dynamic_delivery(
            "app-id",
            "15",
            {
                "session": "agent:main:clawchat-router:group:group-42",
                "message_id": "next-mid",
                "message_seq": 25,
            },
            "yield",
            fake_redis,
        )

        self.assertFalse(first_state["duplicate"])
        self.assertTrue(second_state["duplicate"])
        self.assertFalse(next_message_state["duplicate"])

    def test_maybe_finish_session_transcript_ai_dynamic_keeps_completion_after_immediate_delivery(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {
            "kind": "group",
            "session_key": "agent:main:clawchat:group:group-42",
            "mapping": mapping,
        }
        delivery_ext = {
            "openclaw": {
                "type": "session_sync_delivery",
                "session": "agent:main:clawchat:group:group-42",
                "source": "control_ui_reply",
                "role": "assistant",
                "request_msg_id": "request-finish-1",
            },
            "ai": {
                "ai_generate": False,
            },
        }
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-finish-1",
                "target_kind": "group",
                "target_id": "group-42",
            },
        }
        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 902]) as mocked_send:
            first_result = m.maybe_deliver_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "tool-result-finish-1",
                    "trigger_msg_id": "request-finish-1",
                },
                {
                    "role": "toolResult",
                    "toolName": "exec",
                    "content": "workspace-state.json",
                },
                delivery_ext,
            )
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "final-finish-1",
                    "trigger_msg_id": "request-finish-1",
                },
                delivery_ext,
            )

        self.assertEqual(first_result["result"], "delivered")
        self.assertEqual(mocked_send.call_count, 2)
        self.assertIn("Tool output", mocked_send.call_args_list[0].args[6])
        self.assertIn("workspace-state.json", mocked_send.call_args_list[0].args[6])
        self.assertEqual(mocked_send.call_args_list[1].args[5], 12)
        self.assertIn("处理完成", mocked_send.call_args_list[1].args[6])
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            node_info,
            {
                "session": "agent:main:clawchat:group:group-42",
            },
            "request-finish-1",
            target_config["data"],
            "request-finish-1",
        )
        stream_state = m.load_session_ai_dynamic_stream_state(stream_key)
        self.assertTrue(stream_state["finished"])
        self.assertIn("处理完成", stream_state["content"])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()

    def test_deliver_session_ai_dynamic_item_appends_to_existing_message(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        delivery_ext = {
            "openclaw": {
                "type": "session_sync_delivery",
                "session": "agent:main:clawchat:group:group-42",
                "source": "control_ui_reply",
                "role": "assistant",
                "request_msg_id": "request-late-seq-1",
            },
            "ai": {
                "ai_generate": False,
            },
        }
        target_config = {
            "lanying_admin_token": "admin-token",
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "request_msg_id": "request-late-seq-1",
            "target_kind": "group",
            "target_id": "group-42",
        }
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-late-seq-1",
            "group",
            "group-42",
        )
        m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "Tool output\n```\nWaiting for subagent completion\n```",
            "items": [
                {
                    "event_id": "existing-1",
                    "stream_id": "request-late-seq-1",
                    "transcript_kind": "tool_result",
                    "tool_name": "sessions_yield",
                    "status_kind": "",
                    "text": "Tool output\n```\nWaiting for subagent completion\n```",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1000,
                },
            ],
            "seq": 1,
            "updated_at": 1000,
        })
        with mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "existing-2",
                "transcript_kind": "yield",
                "text": "Yield\n\nwith Waiting for subagent completion\n\nTool input\n```json\n{\n  \"message\": \"Waiting for subagent completion\"\n}\n```",
                "target_config": target_config,
                "delivery_ext": delivery_ext,
                "request_msg_id": "request-late-seq-1",
                "stream_id": "request-late-seq-1",
            }, None, 1401)

        self.assertEqual(result["send_count"], 1)
        self.assertEqual(mocked_send.call_count, 1)
        self.assertIn("Waiting for subagent completion", mocked_send.call_args.args[6])
        self.assertIn("Yield", mocked_send.call_args.args[6])
        self.assertEqual(mocked_send.call_args.args[5], 11)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 901)
        stream_state = m.load_session_ai_dynamic_stream_state(stream_key)
        self.assertEqual(stream_state["seq"], 2)
        self.assertEqual(stream_state["last_msg_id"], 901)
        self.assertEqual(len(stream_state["items"]), 2)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()

    def test_deliver_session_ai_dynamic_item_keeps_same_message_for_late_event_after_finish(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        delivery_ext = {
            "openclaw": {
                "type": "session_sync_delivery",
                "session": "agent:main:clawchat:group:group-42",
                "source": "control_ui_reply",
                "role": "assistant",
                "request_msg_id": "request-finish-late-1",
            },
            "ai": {
                "ai_generate": False,
            },
        }
        target_config = {
            "lanying_admin_token": "admin-token",
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "request_msg_id": "request-finish-late-1",
            "target_kind": "group",
            "target_id": "group-42",
            "is_debug": True,
        }
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-finish-late-1",
            "group",
            "group-42",
        )
        m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI][18:00:00.000] Tool output\n```\nWaiting\n```\n\n[蓝莺AI][18:00:00.100] 处理完成",
            "items": [
                {
                    "event_id": "existing-finished-1",
                    "stream_id": "request-finish-late-1",
                    "transcript_kind": "tool_result",
                    "tool_name": "sessions_yield",
                    "status_kind": "",
                    "text": "Tool output\n```\nWaiting\n```",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1000,
                },
            ],
            "finished": True,
            "seq": 9,
            "updated_at": 1000,
        })
        with mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "event_id": "late-finished-2",
                "transcript_kind": "tool_result",
                "tool_name": "message",
                "status_kind": "",
                "text": "Tool output\n```json\n{\n  \"channel\": \"clawchat\"\n}\n```",
                "target_config": target_config,
                "delivery_ext": delivery_ext,
                "request_msg_id": "request-finish-late-1",
                "stream_id": "request-finish-late-1",
            }, None, 1401)

        self.assertEqual(result["send_count"], 1)
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 901)
        self.assertEqual(mocked_send.call_args.args[7]["online_only"], False)
        self.assertEqual(mocked_send.call_args.args[7]["ext"]["ai"]["finish"], True)
        self.assertIn("Waiting", mocked_send.call_args.args[6])
        self.assertIn("\"channel\": \"clawchat\"", mocked_send.call_args.args[6])
        self.assertTrue(mocked_send.call_args.args[6].rstrip().endswith("处理完成"))
        stream_state = m.load_session_ai_dynamic_stream_state(stream_key)
        self.assertTrue(stream_state["finished"])
        self.assertEqual(stream_state["seq"], 10)
        self.assertEqual(len(stream_state["items"]), 2)

    def test_build_session_ai_dynamic_debug_content_prefers_rendered_text_to_keep_original_timestamps(self):
        m = lanying_openclaw
        content = m.build_session_ai_dynamic_debug_content(
            [
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][18:38:14.532] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1,
                },
                {
                    "transcript_kind": "tool_call",
                    "tool_name": "sessions_spawn",
                    "status_kind": "",
                    "text": "Sub-agent\n\nwith direct_chatbot_subagent_ai_dynamic_1782207152809\n\nTool input\n```json\n{\n  \"taskName\": \"direct_chatbot_subagent_ai_dynamic_1782207152809\"\n}\n```",
                    "rendered_text": "[蓝莺AI][18:38:14.825] Sub-agent\n\nwith direct_chatbot_subagent_ai_dynamic_1782207152809\n\nTool input\n```json\n{\n  \"taskName\": \"direct_chatbot_subagent_ai_dynamic_1782207152809\"\n}\n```",
                    "seq_id": 2,
                    "message_seq": 2,
                    "message_timestamp": 2,
                    "received_at": 2,
                },
            ],
            {"is_debug": True, "status_bar": True},
        )

        self.assertIn("[蓝莺AI][18:38:14.532] 处理开始", content)
        self.assertIn("[蓝莺AI][18:38:14.825] Sub-agent", content)

    def test_format_session_transcript_ai_dynamic_chunk_respects_debug_and_status_bar_modes(self):
        m = lanying_openclaw
        debug_text = m.format_session_transcript_ai_dynamic_chunk(
            "Tool output\n```text\nok\n```",
            {"is_debug": True, "status_bar": False},
        )
        status_bar_text = m.format_session_transcript_ai_dynamic_chunk(
            "Tool output\n```text\nok\n```",
            {"is_debug": False, "status_bar": True},
        )

        self.assertIn("[蓝莺AI][", debug_text)
        self.assertIn("Tool output", debug_text)
        self.assertEqual(status_bar_text, "[蓝莺AI] ok")

    def test_build_session_ai_dynamic_debug_content_sorts_items_without_activity_header(self):
        m = lanying_openclaw
        content = m.build_session_ai_dynamic_debug_content(
            [
                {
                    "transcript_kind": "tool_result",
                    "tool_name": "sessions_yield",
                    "status_kind": "",
                    "text": "Yield\n\nTool output\n```json\n{\n  \"status\": \"yielded\"\n}\n```",
                    "seq_id": 4,
                    "message_seq": 4,
                    "message_timestamp": 4,
                    "received_at": 4,
                },
                {
                    "transcript_kind": "yield",
                    "tool_name": "sessions_yield",
                    "status_kind": "",
                    "text": "Yield\n\nwith waiting for child...\n\nTool input\n```json\n{\n  \"message\": \"waiting for child...\"\n}\n```",
                    "seq_id": 3,
                    "message_seq": 3,
                    "message_timestamp": 3,
                    "received_at": 3,
                },
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1,
                },
                {
                    "transcript_kind": "tool_call",
                    "tool_name": "sessions_spawn",
                    "status_kind": "",
                    "text": "Sub-agent - direct_chatbot_subagent_ai_dynamic_1782207152809\n\nTool input\n```json\n{\n  \"taskName\": \"direct_chatbot_subagent_ai_dynamic_1782207152809\"\n}\n```",
                    "seq_id": 2,
                    "message_seq": 2,
                    "message_timestamp": 2,
                    "received_at": 2,
                },
            ],
            {"is_debug": True, "status_bar": True},
        )

        self.assertNotIn("Activity:", content)
        self.assertTrue(content.index("Sub-agent - direct_chatbot_subagent_ai_dynamic_1782207152809") < content.index('"status": "yielded"'))
        self.assertTrue(content.index("waiting for child...") < content.index('"status": "yielded"'))

    def test_build_session_ai_dynamic_debug_content_sorts_by_timestamp_across_streams(self):
        m = lanying_openclaw
        content = m.build_session_ai_dynamic_debug_content(
            [
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][11:07:36.195] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1782270456523,
                    "received_at": 1782270456523,
                },
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][11:07:56.002] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1782270475865,
                    "received_at": 1782270476002,
                },
                {
                    "transcript_kind": "tool_call",
                    "tool_name": "sessions_spawn",
                    "status_kind": "",
                    "text": "Sub-agent - direct_chatbot_subagent_ai_dynamic_1782270455643\n\nTool input\n```json\n{\n  \"task\": \"You are a subagent running an integration test.\"\n}\n```",
                    "rendered_text": "[蓝莺AI][11:07:50.968] Sub-agent - direct_chatbot_subagent_ai_dynamic_1782270455643\n\nTool input\n```json\n{\n  \"task\": \"You are a subagent running an integration test.\"\n}\n```",
                    "seq_id": 2,
                    "message_seq": 2,
                    "message_timestamp": 1782270470766,
                    "received_at": 1782270470968,
                },
            ],
            {"is_debug": True, "status_bar": True},
        )

        self.assertTrue(content.index("[蓝莺AI][11:07:36.195] 处理开始") < content.index("[蓝莺AI][11:07:50.968] Sub-agent - direct_chatbot_subagent_ai_dynamic_1782270455643"))
        self.assertNotIn("[蓝莺AI][11:07:56.002] 处理开始", content)

    def test_session_ai_dynamic_event_sort_key_prefers_timestamp_over_cross_run_seq(self):
        m = lanying_openclaw
        earlier_with_larger_seq = {
            "transcript_kind": "tool_call",
            "seq_id": 9,
            "message_seq": 9,
            "message_timestamp": 1000,
            "received_at": 1100,
        }
        later_with_reset_seq = {
            "transcript_kind": "tool_call",
            "seq_id": 1,
            "message_seq": 1,
            "message_timestamp": 1001,
            "received_at": 1101,
        }

        self.assertLess(
            m.session_ai_dynamic_event_sort_key(earlier_with_larger_seq),
            m.session_ai_dynamic_event_sort_key(later_with_reset_seq),
        )

    def test_build_session_ai_dynamic_debug_content_dedupes_duplicate_processing_start(self):
        m = lanying_openclaw
        content = m.build_session_ai_dynamic_debug_content(
            [
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][11:07:36.195] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1782270456523,
                    "received_at": 1782270456523,
                },
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][11:07:56.002] 处理开始",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1782270475865,
                    "received_at": 1782270476002,
                },
            ],
            {"is_debug": True, "status_bar": True},
        )

        self.assertEqual(content.count("处理开始"), 1)

    def test_build_session_ai_dynamic_debug_content_dedupes_processing_start_against_preserved_content(self):
        m = lanying_openclaw
        content = m.build_session_ai_dynamic_debug_content(
            [
                {
                    "event_id": "__preserved_existing_content__",
                    "transcript_kind": "status",
                    "tool_name": "",
                    "status_kind": "status",
                    "text": "[蓝莺AI][11:53:30.852] 处理开始\n\n[蓝莺AI][11:53:30.986] 当前预设为: openclaw-jp\n\n[蓝莺AI][11:53:32.845] prompt信息如下: ...",
                    "rendered_text": "[蓝莺AI][11:53:30.852] 处理开始\n\n[蓝莺AI][11:53:30.986] 当前预设为: openclaw-jp\n\n[蓝莺AI][11:53:32.845] prompt信息如下: ...",
                    "seq_id": 0,
                    "message_seq": 0,
                    "message_timestamp": 0,
                    "received_at": 0,
                },
                {
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "text": "处理开始",
                    "rendered_text": "[蓝莺AI][11:53:33.494] 处理开始",
                    "seq_id": 4,
                    "message_seq": 4,
                    "message_timestamp": 1782273213494,
                    "received_at": 1782273213494,
                },
            ],
            {"is_debug": True, "status_bar": True},
        )

        self.assertEqual(content.count("处理开始"), 1)
        self.assertNotIn("[蓝莺AI][11:53:33.494] 处理开始", content)

    def test_resolve_session_transcript_stream_id_prefers_request_msg_id_over_runtime_stream(self):
        m = lanying_openclaw
        stream_id = m.resolve_session_transcript_stream_id(
            {
                "stream_id": "runtime-stream-1",
                "trigger_msg_id": "trigger-msg-1",
                "message_id": "legacy-message-1",
            },
            {
                "openclaw": {
                    "stream_id": "ext-stream-1",
                }
            },
            "request-msg-1",
        )

        self.assertEqual(stream_id, "request-msg-1")

    def test_build_session_transcript_ai_dynamic_completion_line_prefers_debug_style(self):
        m = lanying_openclaw
        completion_line = m.build_session_transcript_ai_dynamic_completion_line(
            "[蓝莺AI] Tool output\n```text\nok\n```",
            {"is_debug": True, "status_bar": True},
        )
        self.assertIn("[蓝莺AI][", completion_line)
        self.assertTrue(completion_line.endswith("处理完成"))

    def test_deliver_session_ai_dynamic_item_uses_replace_mode_for_status_bar(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        stream_key = m.build_session_ai_dynamic_stream_key(
            "app-id",
            "15",
            "agent:main:clawchat:group:group-42",
            "request-status-bar-1",
            "group",
            "group-42",
        )
        target_config = {
            "app_id": "app-id",
            "reply_msg_type": "GROUPCHAT",
            "reply_from": "openclaw-user",
            "reply_to": "group-42",
            "request_msg_id": "request-status-bar-1",
            "target_kind": "group",
            "target_id": "group-42",
            "status_bar": True,
            "is_debug": False,
        }
        m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI] old status",
            "seq": 1,
            "updated_at": 1000,
        })

        with mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            result = m.deliver_session_ai_dynamic_item(stream_key, {
                "transcript_kind": "tool_result",
                "text": "Tool output\n```text\nnew status\n```",
                "target_config": target_config,
                "delivery_ext": {"ai": {"ai_generate": False}},
                "request_msg_id": "request-status-bar-1",
                "stream_id": "request-status-bar-1",
            }, None, 1400)

        self.assertEqual(result["send_count"], 1)
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[6], "[蓝莺AI] new status")
        self.assertEqual(m.load_session_ai_dynamic_stream_state(stream_key)["content"], "[蓝莺AI] new status")

    def test_maybe_finish_session_transcript_ai_dynamic_replaces_content_for_status_bar(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            {"node_id": "15"},
            {"session": "agent:main:clawchat:group:group-42"},
            "request-status-bar-finish-1",
            {
                "target_kind": "group",
                "target_id": "group-42",
            },
            request_msg_id="request-status-bar-finish-1",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI] Tool output\n```text\nnew status\n```",
            "seq": 1,
            "updated_at": int(time.time() * 1000),
        }))
        target_config = {
            "result": "ok",
            "data": {
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-status-bar-finish-1",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"},
                {"session_key": "agent:main:clawchat:group:group-42"},
                {"kind": "group", "mapping": {"group_id": "group-42"}},
                None,
                {
                    "session": "agent:main:clawchat:group:group-42",
                    "trigger_msg_id": "request-status-bar-finish-1",
                },
                {"openclaw": {"request_msg_id": "request-status-bar-finish-1"}},
            )

        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[6], "[蓝莺AI] 处理完成")

    def test_maybe_finish_session_transcript_ai_dynamic_finishes_direct_openclaw_without_completion_line(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        with m.recent_session_ai_dynamic_lock_registry_lock:
            m.recent_session_ai_dynamic_lock_by_key.clear()
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            {"node_id": "15"},
            {"session": "agent:main:clawchat:direct:sender-user"},
            "request-direct-openclaw-finish-1",
            {
                "target_kind": "direct",
                "target_id": "sender-user",
            },
            request_msg_id="request-direct-openclaw-finish-1",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI][18:11:02.111] Exec `pwd`",
            "seq": 1,
            "updated_at": int(time.time() * 1000),
        }))
        target_config = {
            "result": "ok",
            "data": {
                "app_id": "app-id",
                "reply_msg_type": "CHAT",
                "reply_from": "openclaw-user",
                "reply_to": "sender-user",
                "request_msg_id": "request-direct-openclaw-finish-1",
                "target_kind": "direct",
                "target_id": "sender-user",
                "status_bar": True,
                "is_debug": False,
            },
        }
        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"},
                {"session_key": "agent:main:clawchat:direct:sender-user"},
                {"kind": "direct", "session_key": "agent:main:clawchat:direct:sender-user", "target_user_id": "sender-user"},
                {"channel": "clawchat", "chat_type": "direct", "target_id": "sender-user"},
                {
                    "session": "agent:main:clawchat:direct:sender-user",
                    "trigger_msg_id": "request-direct-openclaw-finish-1",
                },
                {"openclaw": {"request_msg_id": "request-direct-openclaw-finish-1"}},
            )

        mocked_send.assert_called_once()
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[6], "[蓝莺AI][18:11:02.111] Exec `pwd`")
        self.assertEqual(mocked_send.call_args.args[7]["ext"]["ai"]["finish"], True)
        saved_state = m.load_session_ai_dynamic_stream_state(stream_key)
        self.assertEqual(saved_state["content"], "[蓝莺AI][18:11:02.111] Exec `pwd`")
        self.assertTrue(bool(saved_state.get("finished", False)))

    def test_maybe_finish_session_transcript_ai_dynamic_takes_over_connector_debug_without_intermediate(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-finish-only-1",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }

        m.remember_request_debug_stream_state(
            "request-finish-only-1",
            777,
            1,
            "[蓝莺AI] 处理开始",
            app_id="app-id",
            target_kind="group",
            target_id="group-42",
            target_sender_id="openclaw-user",
        )
        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=778) as mocked_send:
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "session": "agent:main:subagent:child",
                    "request_msg_id": "request-finish-only-1",
                    "message_id": "final-1",
                },
                {"openclaw": {"request_msg_id": "request-finish-only-1"}},
            )

        mocked_send.assert_called_once()
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 777)
        self.assertEqual(mocked_send.call_args.args[6], "[蓝莺AI] 处理完成")

    def test_maybe_finish_session_transcript_ai_dynamic_prefers_items_over_stale_content(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-finish-items-1",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": False,
                "is_debug": True,
            },
        }
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            node_info,
            {"session": "agent:main:clawchat:group:group-42"},
            "request-finish-items-1",
            target_config["data"],
            request_msg_id="request-finish-items-1",
        )
        fake_redis = FakeRedis()
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI][18:00:00.000] stale content that should not win",
            "items": [
                {
                    "event_id": "__request_debug_snapshot__",
                    "stream_id": "request-finish-items-1",
                    "transcript_kind": "status",
                    "tool_name": "",
                    "status_kind": "status",
                    "text": "[蓝莺AI][18:00:00.000] 处理开始",
                    "rendered_text": "[蓝莺AI][18:00:00.000] 处理开始",
                    "seq_id": 0,
                    "message_seq": 0,
                    "message_timestamp": 0,
                    "received_at": 0,
                },
                {
                    "event_id": "event-2",
                    "stream_id": "request-finish-items-1",
                    "transcript_kind": "tool_call",
                    "tool_name": "subagent_spawn",
                    "status_kind": "",
                    "text": "Sub-agent",
                    "rendered_text": "[蓝莺AI][18:00:01.000] Sub-agent",
                    "seq_id": 1,
                    "message_seq": 1,
                    "message_timestamp": 1,
                    "received_at": 1,
                },
            ],
            "seq": 2,
            "updated_at": int(time.time() * 1000),
        }, fake_redis))

        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m, "get_session_ai_dynamic_redis", return_value=fake_redis), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "session": "agent:main:clawchat:group:group-42",
                    "trigger_msg_id": "request-finish-items-1",
                },
                {"openclaw": {"request_msg_id": "request-finish-items-1"}},
            )

        mocked_send.assert_called_once()
        self.assertIn("[蓝莺AI][18:00:00.000] 处理开始", mocked_send.call_args.args[6])
        self.assertIn("[蓝莺AI][18:00:01.000] Sub-agent", mocked_send.call_args.args[6])
        self.assertNotIn("stale content that should not win", mocked_send.call_args.args[6])
        self.assertTrue(mocked_send.call_args.args[6].endswith("处理完成"))

    def test_maybe_finish_session_transcript_ai_dynamic_retries_until_lock_acquired(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {"kind": "group", "mapping": mapping}
        target_config = {
            "result": "ok",
            "data": {
                "app_id": "app-id",
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "request-finish-retry-1",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": True,
                "is_debug": False,
            },
        }
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            "app-id",
            node_info,
            {"session": "agent:main:clawchat:group:group-42"},
            "request-finish-retry-1",
            target_config["data"],
            request_msg_id="request-finish-retry-1",
        )
        fake_redis = FakeRedis()
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI] new status",
            "seq": 1,
            "updated_at": int(time.time() * 1000),
        }, fake_redis))

        busy_lock_state = {
            "locked": False,
            "redis_key": m.build_session_ai_dynamic_stream_lock_redis_key(stream_key),
            "token": "",
            "redis": fake_redis,
            "enabled": True,
        }
        ready_lock_state = {
            "locked": True,
            "redis_key": m.build_session_ai_dynamic_stream_lock_redis_key(stream_key),
            "token": "token-1",
            "redis": fake_redis,
            "enabled": True,
        }

        with mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m, "acquire_session_ai_dynamic_stream_distributed_lock", side_effect=[busy_lock_state, ready_lock_state]), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            m.maybe_finish_session_transcript_ai_dynamic(
                "app-id",
                node_info,
                mapping,
                target,
                None,
                {
                    "session": "agent:main:clawchat:group:group-42",
                    "trigger_msg_id": "request-finish-retry-1",
                },
                {"openclaw": {"request_msg_id": "request-finish-retry-1"}},
            )

        mocked_send.assert_called_once()
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 901)

    def test_handle_session_message_sync_event_formats_tool_result_text_without_outer_json(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "tool-result-exec-1",
                    "trigger_msg_id": "request-exec-2",
                    "message": {
                        "role": "toolResult",
                        "toolName": "exec",
                        "content": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "total 48\nAGENTS.md\n",
                                }
                            ],
                            "details": {
                                "status": "running",
                                "tail": "tail should not be preferred when text exists",
                            },
                        },
                    },
                },
            )

        mocked_direct.assert_not_called()
        sent_text = mocked_send.call_args.args[6]
        self.assertEqual(sent_text, "[蓝莺AI] Exec")
        self.assertNotIn("AGENTS.md", sent_text)
        self.assertNotIn("```\ntotal 48", sent_text)
        self.assertNotIn('"details"', sent_text)
        self.assertNotIn('"content"', sent_text)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_prefers_structured_tool_result_text_over_event_intermediate_text(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "tool-result-exec-raw-1",
                    "trigger_msg_id": "request-exec-raw-1",
                    "transcript_kind": "tool_result",
                    "tool_name": "exec",
                    "intermediate_text": "Exec\n\nTool output\n{\n  \"content\": [{\"type\":\"text\",\"text\":\"raw old text\"}]\n}",
                    "message": {
                        "role": "toolResult",
                        "content": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "total 48\nAGENTS.md\n",
                                }
                            ],
                            "details": {
                                "tail": "tail fallback text",
                            },
                        },
                    },
                },
            )

        mocked_direct.assert_not_called()
        sent_text = mocked_send.call_args.args[6]
        self.assertIn("total 48", sent_text)
        self.assertNotIn("AGENTS.md", sent_text)
        self.assertNotIn("```\ntotal 48", sent_text)
        self.assertNotIn("raw old text", sent_text)
        self.assertNotIn('"content"', sent_text)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_routes_heartbeat_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "heartbeat-1",
                    "trigger_msg_id": "request-heartbeat-1",
                    "transcript_kind": "heartbeat",
                    "tool_name": "heartbeat_respond",
                    "status_kind": "heartbeat",
                    "intermediate_text": "Heartbeat\n\nTool output\n{\"status\":\"accepted\",\"notify\":false}",
                    "message": {
                        "role": "toolResult",
                        "toolName": "heartbeat_respond",
                        "content": "{\"status\":\"accepted\",\"notify\":false}",
                    },
                },
            )

        mocked_direct.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(mocked_send.call_args.args[5], 0)
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertIn("Heartbeat", mocked_send.call_args.args[6])
        self.assertNotIn("accepted", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()

    def test_handle_session_message_sync_event_drops_control_ui_user_heartbeat_poll(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:main",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "effective_target_session_key": "agent:main:main",
            "root_session_key": "agent:main:main",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:main",
                 "mapping": mapping,
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, None)), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_user",
                    "session": "agent:main:main",
                    "message_id": "heartbeat-poll-1",
                    "transcript_kind": "heartbeat",
                    "status_kind": "heartbeat",
                    "intermediate_text": "[OpenClaw heartbeat poll]",
                    "message": {
                        "role": "user",
                        "content": "[OpenClaw heartbeat poll]",
                    },
                },
            )

        mocked_group.assert_not_called()
        mocked_send.assert_not_called()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_routes_plain_text_heartbeat_ok_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:main",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "effective_target_session_key": "agent:main:main",
            "root_session_key": "agent:main:main",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:main",
                 "mapping": mapping,
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, None)), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m, "ensure_user_joined_group", return_value=True), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:main",
                    "message_id": "heartbeat-ok-1",
                    "transcript_kind": "text",
                    "intermediate_text": "HEARTBEAT_OK",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "HEARTBEAT_OK",
                            }
                        ],
                    },
                },
            )

        mocked_group.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["stream"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertEqual(sent_extra["ext"]["ai"]["need_antispam_check"], False)
        self.assertIn("HEARTBEAT_OK", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_routes_status_to_ai_dynamic(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "status-1",
                    "trigger_msg_id": "request-status-1",
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "intermediate_text": "Status\n\nTool output\nLLM request failed: connection refused by the provider endpoint.",
                    "message": {
                        "role": "toolResult",
                        "toolName": "session_status",
                        "content": "LLM request failed: connection refused by the provider endpoint.",
                    },
                },
            )

        mocked_direct.assert_not_called()
        mocked_send.assert_called_once()
        sent_extra = mocked_send.call_args.args[7]
        self.assertEqual(mocked_send.call_args.args[5], 0)
        self.assertEqual(sent_extra["ext"]["ai"]["is_debug_msg"], True)
        self.assertEqual(sent_extra["ext"]["ai"]["finish"], False)
        self.assertIn("LLM request failed: connection refused by the provider endpoint.", mocked_send.call_args.args[6])
        self.assertNotIn("Tool output", mocked_send.call_args.args[6])
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_marks_completion_intermediate_as_finish(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 901]) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "status-complete-1",
                    "trigger_msg_id": "request-status-complete-1",
                    "transcript_kind": "status",
                    "tool_name": "session_status",
                    "status_kind": "status",
                    "intermediate_text": "处理完成",
                    "message": {
                        "role": "assistant",
                        "content": "处理完成",
                    },
                },
            )

        mocked_direct.assert_not_called()
        self.assertEqual(mocked_send.call_count, 2)
        finish_extra = mocked_send.call_args_list[1].args[7]
        self.assertEqual(mocked_send.call_args_list[1].args[5], 12)
        self.assertEqual(finish_extra["ext"]["ai"]["finish"], True)
        self.assertEqual(mocked_send.call_args_list[1].args[6], "[蓝莺AI] 处理完成")
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_does_not_fallback_intermediate_direct_to_visible_reply(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat-router:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value={"result": "missing_sender"}), \
             mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "direct-tool-call-no-fallback-1",
                    "trigger_msg_id": "request-direct-no-fallback-1",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "name": "exec",
                                "arguments": {
                                    "command": "pwd",
                                },
                            }
                        ],
                    },
                },
            )

        mocked_send.assert_not_called()
        mocked_direct.assert_not_called()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_does_not_fallback_intermediate_group_to_visible_reply(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "origin_kind": "im_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "session_key": "agent:main:clawchat:group:group-42",
                 "mapping": mapping,
             }), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value={"result": "sender_not_ready"}), \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group, \
             mock.patch.object(m.lanying_im_api, "send_message_sync") as mocked_send:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat:group:group-42",
                    "message_id": "group-tool-result-no-fallback-1",
                    "trigger_msg_id": "request-group-no-fallback-1",
                    "message": {
                        "role": "toolResult",
                        "toolName": "exec",
                        "content": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "workspace-state.json",
                                }
                            ],
                        },
                    },
                },
            )

        mocked_send.assert_not_called()
        mocked_group.assert_not_called()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()

    def test_handle_session_message_sync_event_finishes_ai_dynamic_before_final_reply(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        target = {
            "kind": "direct",
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "target_user_id": "sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
        }
        m.remember_request_debug_stream_state("request-2", 777, 2, "[蓝莺AI][18:11:02.111] 处理开始")

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", side_effect=[901, 901]) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=0), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "tool-call-2",
                    "trigger_msg_id": "request-2",
                    "transcript_kind": "yield",
                    "tool_name": "sessions_yield",
                    "intermediate_text": "Yield\nTool input\n{\"message\":\"waiting\"}",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "toolCall",
                                "name": "sessions_yield",
                                "arguments": {
                                    "message": "waiting",
                                },
                            }
                        ],
                    },
                },
            )
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "final-reply-2",
                    "trigger_msg_id": "request-2",
                    "message": {
                        "role": "assistant",
                        "content": "final answer",
                    },
                },
            )

        self.assertEqual(mocked_send.call_count, 2)
        finish_extra = mocked_send.call_args_list[1].args[7]
        self.assertEqual(mocked_send.call_args_list[1].args[5], 12)
        self.assertEqual(finish_extra["ext"]["ai"]["finish"], True)
        self.assertIn("处理完成", mocked_send.call_args_list[1].args[6])
        mocked_direct.assert_called_once()
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()

    def test_handle_session_message_sync_event_finishes_ai_dynamic_from_request_state_when_session_changes(self):
        m = lanying_openclaw
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        target = {
            "kind": "direct",
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "target_user_id": "sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
        }
        m.remember_request_debug_stream_state("request-finish-fallback", 777, 2, "[蓝莺AI][18:11:02.111] 处理开始")

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "resolve_chatbot_status_bar_enabled", return_value=(True, {"user_id": "chatbot-user"})), \
             mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=901) as mocked_send, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=0), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301):
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:child-1",
                    "root_session": "agent:main:clawchat-router:direct:sender-user",
                    "parent_session": "agent:main:clawchat-router:direct:sender-user",
                    "message_id": "final-reply-fallback",
                    "trigger_msg_id": "request-finish-fallback",
                    "message": {
                        "role": "assistant",
                        "content": "final answer",
                    },
                },
            )

        self.assertEqual(mocked_send.call_count, 0)
        m.recent_session_ai_dynamic_stream_by_key.clear()
        m.recent_session_ai_dynamic_dedupe_by_key.clear()
        m.recent_request_debug_stream_by_key.clear()

    def test_handle_client_event_skips_router_reply_plugin_suppression_hint(self):
        m = lanying_openclaw
        event = {
            "type": "router_reply",
            "suppression_reason": "duplicate_parent_after_subagent",
            "message": {
                "id": "router-reply-1",
                "from": "openclaw-user",
                "to": "sender-user",
                "content": "suppressed reply",
                "type": "text",
                "toType": "roster",
            },
        }
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "convert_from_meta_message") as mocked_convert, \
             mock.patch.object(m, "router_reply_message") as mocked_router_reply:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_convert.assert_not_called()
        mocked_router_reply.assert_not_called()

    def test_handle_session_message_sync_event_plugin_owned_reply_still_finishes_ai_dynamic(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:direct:sender-user",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "is_session_map_sync_enabled", return_value=True), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "resolve_parent_group_session_sync_target", return_value=None), \
             mock.patch.object(m, "maybe_deliver_session_transcript_ai_dynamic", return_value={"result": "not_intermediate"}), \
             mock.patch.object(m, "maybe_finish_session_transcript_ai_dynamic") as mocked_finish, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_forward:
            m.handle_session_message_sync_event("app-id", node_info, {
                "type": "session_message_sync",
                "source": "control_ui_reply",
                "session": "agent:main:clawchat:direct:sender-user",
                "message_id": "visible-final-1",
                "trigger_msg_id": "request-1",
                "visible_delivery_owner": "plugin",
                "message": {
                    "role": "assistant",
                    "content": "SYNC_OK",
                },
            })

        mocked_finish.assert_called_once()
        mocked_forward.assert_not_called()

    def test_build_router_reply_delivery_ext_reads_nested_role_from_session_transcript_observed(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "im-req-2",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_transcript_observed",
                    "session": "agent:main:clawchat-router:direct:sender-user",
                    "source": "control_ui_user",
                    "message_id": "oc-req-2",
                    "trigger_msg_id": "trigger-im-2",
                    "message": {
                        "role": "user",
                        "content": "hello",
                    },
                }
            }),
        })

        self.assertEqual(ext["openclaw"]["type"], "session_sync_delivery")
        self.assertEqual(ext["openclaw"]["session"], "agent:main:clawchat-router:direct:sender-user")
        self.assertEqual(ext["openclaw"]["request_source"], "control_ui_user")
        self.assertEqual(ext["openclaw"]["request_role"], "user")
        self.assertEqual(ext["openclaw"]["request_message_id"], "oc-req-2")
        self.assertEqual(ext["openclaw"]["trigger_msg_id"], "trigger-im-2")
        self.assertEqual(ext["openclaw"]["request_msg_id"], "trigger-im-2")

    def test_build_router_reply_delivery_ext_prefers_router_request_sid_for_request_msg_id(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-reply-visible-1",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "router-reply-oc-1",
                    "router_request_sid": "im-request-42",
                }
            }),
        })

        self.assertNotIn("request_source", ext["openclaw"])
        self.assertNotIn("request_role", ext["openclaw"])
        self.assertNotIn("request_message_id", ext["openclaw"])
        self.assertEqual(ext["openclaw"]["request_msg_id"], "im-request-42")

    def test_build_router_reply_delivery_ext_does_not_treat_reply_as_request_context(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-reply-visible-2",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "router_reply_123_1",
                    "visible_delivery_owner": "plugin",
                },
                "ai": {
                    "ai_generate": False,
                },
            }),
        })

        self.assertEqual(ext["openclaw"]["source"], "control_ui_reply")
        self.assertEqual(ext["openclaw"]["role"], "assistant")
        self.assertEqual(ext["openclaw"]["visible_delivery_owner"], "plugin")
        self.assertNotIn("request_source", ext["openclaw"])
        self.assertNotIn("request_role", ext["openclaw"])
        self.assertNotIn("request_message_id", ext["openclaw"])
        self.assertNotIn("request_msg_id", ext["openclaw"])
        self.assertEqual(ext["ai"]["ai_generate"], False)

    def test_build_router_reply_delivery_ext_marks_router_delivery_ai_generate_false_without_openclaw(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-reply-visible-no-openclaw",
            "ext": "",
        })

        self.assertEqual(ext["ai"]["role"], "ai")
        self.assertEqual(ext["ai"]["ai_generate"], False)
        self.assertNotIn("openclaw", ext)

    def test_build_router_reply_delivery_ext_preserves_im_reply_delivery_without_session(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-reply-visible-im-delivery",
            "ext": json.dumps({
                "openclaw": {
                    "type": "im_reply_delivery",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "router_reply_456_1",
                    "visible_delivery_owner": "plugin",
                    "request_msg_id": "im-request-456",
                },
                "ai": {
                    "role": "ai",
                    "ai_generate": False,
                },
            }),
        })

        self.assertEqual(ext["ai"]["role"], "ai")
        self.assertEqual(ext["ai"]["ai_generate"], False)
        self.assertEqual(ext["openclaw"]["type"], "im_reply_delivery")
        self.assertEqual(ext["openclaw"]["source"], "control_ui_reply")
        self.assertEqual(ext["openclaw"]["role"], "assistant")
        self.assertEqual(ext["openclaw"]["message_id"], "router_reply_456_1")
        self.assertEqual(ext["openclaw"]["visible_delivery_owner"], "plugin")
        self.assertEqual(ext["openclaw"]["request_msg_id"], "im-request-456")
        self.assertNotIn("session", ext["openclaw"])

    def test_build_router_reply_delivery_ext_preserves_im_reply_delivery_diagnostic_session(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-reply-visible-im-delivery-with-session",
            "ext": json.dumps({
                "openclaw": {
                    "type": "im_reply_delivery",
                    "session": "agent:main:subagent:child-1",
                    "source": "im_reply",
                    "role": "assistant",
                    "visible_delivery_owner": "plugin",
                    "trigger_msg_id": "im-trigger-1",
                    "request_msg_id": "im-request-1",
                },
                "ai": {
                    "role": "ai",
                    "ai_generate": False,
                },
            }),
        })

        self.assertEqual(ext["openclaw"]["type"], "im_reply_delivery")
        self.assertEqual(ext["openclaw"]["session"], "agent:main:subagent:child-1")
        self.assertEqual(ext["openclaw"]["source"], "im_reply")
        self.assertEqual(ext["openclaw"]["role"], "assistant")
        self.assertEqual(ext["openclaw"]["visible_delivery_owner"], "plugin")
        self.assertEqual(ext["openclaw"]["trigger_msg_id"], "im-trigger-1")
        self.assertEqual(ext["openclaw"]["request_msg_id"], "im-request-1")

    def test_control_ui_reply_delivery_ext_includes_trigger_msg_id_when_known(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
             }), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=1) as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_transcript_observed",
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "trigger_msg_id": "trigger-im-3",
                    "message": {
                        "role": "assistant",
                        "content": "child reply",
                    },
                },
        )

        mocked_direct.assert_called_once()
        delivery_ext = mocked_direct.call_args.args[8]
        self.assertEqual(delivery_ext["openclaw"]["trigger_msg_id"], "trigger-im-3")
        self.assertEqual(delivery_ext["openclaw"]["request_msg_id"], "trigger-im-3")

    def test_build_router_reply_delivery_ext_preserves_request_msg_id_from_inner_ext(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "router-visible-1",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat-router:group:group-42",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "router-reply-oc-2",
                    "request_msg_id": "original-im-25",
                }
            }),
        })

        self.assertNotIn("request_message_id", ext["openclaw"])
        self.assertEqual(ext["openclaw"]["request_msg_id"], "original-im-25")

    def test_build_router_reply_delivery_ext_keeps_direct_openclaw_reply_as_normal_visible_reply(self):
        m = lanying_openclaw
        ext = m.build_router_reply_delivery_ext({
            "msgId": "direct-openclaw-visible-1",
            "ext": json.dumps({
                "openclaw": {
                    "type": "session_sync_delivery",
                    "session": "agent:main:clawchat:direct:sender-user",
                    "source": "control_ui_reply",
                    "role": "assistant",
                    "message_id": "direct-openclaw-oc-1",
                    "request_msg_id": "im-request-direct-1",
                },
                "ai": {
                    "ai_generate": False,
                },
            }),
        })

        self.assertEqual(ext["openclaw"]["request_msg_id"], "im-request-direct-1")
        self.assertEqual(ext["ai"]["role"], "ai")
        self.assertEqual(ext["ai"]["ai_generate"], False)
        self.assertNotIn("stream", ext["ai"])
        self.assertNotIn("finish", ext["ai"])
        self.assertNotIn("request_msg_id", ext["ai"])
        self.assertNotIn("stream_id", ext["ai"])

    def test_im_reply_delivery_callback_finishes_clawchat_ai_dynamic_stream(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        app_id = "app-id"
        node_info = {"app_id": app_id, "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:direct:user-1",
            "origin_kind": "direct_user",
            "origin_user_id": "user-1",
            "chatbot_user_id": "",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:direct:user-1",
            "effective_target_session_key": "agent:main:clawchat:direct:user-1",
        }
        target = {
            "kind": "direct",
            "target_user_id": "user-1",
            "origin_user_id": "user-1",
            "chatbot_user_id": "",
            "session_key": "agent:main:clawchat:direct:user-1",
        }
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": app_id,
                "reply_msg_type": "CHAT",
                "reply_from": "openclaw-user",
                "reply_to": "user-1",
                "request_msg_id": "im-request-1",
                "target_kind": "direct",
                "target_id": "user-1",
                "status_bar": False,
                "is_debug": True,
            },
        }
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            app_id,
            node_info,
            {"session": "agent:main:clawchat:direct:user-1"},
            "im-request-1",
            target_config["data"],
            request_msg_id="im-request-1",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 901,
            "content": "[蓝莺AI][12:00:00.000] Exec `pwd`\n\nTool input\n```json\n{\n  \"command\": \"pwd\"\n}\n```",
            "seq": 1,
            "updated_at": int(time.time() * 1000),
        }, fake_redis))

        msg = {
            "appId": app_id,
            "msgId": "visible-reply-1",
            "from": {"uid": "openclaw-user"},
            "to": {"uid": "user-1"},
            "content": "SYNC_OK",
            "ext": json.dumps({
                "openclaw": {
                    "type": "im_reply_delivery",
                    "session": "agent:main:clawchat:direct:user-1",
                    "source": "im_reply",
                    "role": "assistant",
                    "visible_delivery_owner": "plugin",
                    "trigger_msg_id": "im-request-1",
                    "request_msg_id": "im-request-1",
                },
                "ai": {
                    "role": "ai",
                    "ai_generate": False,
                },
            }),
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=902) as mocked_send:
            m.maybe_finish_im_reply_delivery_ai_dynamic(msg)

        self.assertEqual(mocked_send.call_count, 1)
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 901)
        self.assertEqual(mocked_send.call_args.args[7]["online_only"], False)
        self.assertTrue(mocked_send.call_args.args[7]["ext"]["ai"]["finish"])
        self.assertEqual(
            mocked_send.call_args.args[7]["ext"]["openclaw"]["type"],
            "session_sync_delivery",
        )

    def test_im_reply_delivery_callback_finishes_clawchat_group_ai_dynamic_stream(self):
        m = lanying_openclaw
        fake_redis = self.default_redis
        app_id = "app-id"
        node_info = {"app_id": app_id, "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:clawchat:group:group-42",
            "group_id": "group-42",
            "management_user_id": "management-user",
            "root_session_key": "agent:main:clawchat:group:group-42",
            "effective_target_session_key": "agent:main:clawchat:group:group-42",
        }
        target = {
            "kind": "group",
            "mapping": mapping,
            "session_key": "agent:main:clawchat:group:group-42",
        }
        target_config = {
            "result": "ok",
            "data": {
                "lanying_admin_token": "admin-token",
                "app_id": app_id,
                "reply_msg_type": "GROUPCHAT",
                "reply_from": "openclaw-user",
                "reply_to": "group-42",
                "request_msg_id": "im-request-group-1",
                "target_kind": "group",
                "target_id": "group-42",
                "status_bar": False,
                "is_debug": True,
            },
        }
        stream_key = m.build_session_ai_dynamic_stream_key_for_event(
            app_id,
            node_info,
            {"session": "agent:main:clawchat:group:group-42"},
            "im-request-group-1",
            target_config["data"],
            request_msg_id="im-request-group-1",
        )
        self.assertTrue(m.save_session_ai_dynamic_stream_state(stream_key, {
            "last_msg_id": 911,
            "content": "[蓝莺AI][12:00:00.000] Sub-agent",
            "seq": 4,
            "updated_at": int(time.time() * 1000),
        }, fake_redis))

        msg = {
            "appId": app_id,
            "msgId": "visible-group-reply-1",
            "from": {"uid": "openclaw-user"},
            "to": {"uid": "group-42"},
            "content": "SYNC_OK",
            "ext": json.dumps({
                "openclaw": {
                    "type": "im_reply_delivery",
                    "session": "agent:main:clawchat:group:group-42",
                    "source": "im_reply",
                    "role": "assistant",
                    "visible_delivery_owner": "plugin",
                    "trigger_msg_id": "im-request-group-1",
                    "request_msg_id": "im-request-group-1",
                },
                "ai": {
                    "role": "ai",
                    "ai_generate": False,
                },
            }),
        }

        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis), \
             mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value=target), \
             mock.patch.object(m, "resolve_session_transcript_ai_dynamic_target", return_value=target_config), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=912) as mocked_send:
            m.maybe_finish_im_reply_delivery_ai_dynamic(msg)

        self.assertEqual(mocked_send.call_count, 1)
        self.assertEqual(mocked_send.call_args.args[5], 12)
        self.assertEqual(mocked_send.call_args.args[7]["related_mid"], 911)
        self.assertEqual(mocked_send.call_args.args[7]["online_only"], False)
        self.assertTrue(mocked_send.call_args.args[7]["ext"]["ai"]["finish"])
        self.assertEqual(
            mocked_send.call_args.args[7]["ext"]["ai"]["stream_id"],
            "im-request-group-1",
        )
        self.assertEqual(
            mocked_send.call_args.args[7]["ext"]["openclaw"]["type"],
            "session_sync_delivery",
        )

    def test_router_group_root_assistant_sync_prefers_router_reply(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=1) as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "reply back to router group",
                    },
                },
            )

        mocked_group_reply.assert_called_once()
        forwarded_mapping = mocked_group_reply.call_args.args[2]
        self.assertEqual(forwarded_mapping["session_key"], "agent:main:clawchat-router:group:group-42")
        mocked_group_forward.assert_not_called()

    def test_router_group_child_group_assistant_sync_stays_in_child_group(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "temporary-session-group",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=1) as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "reply should stay in child group",
                    },
                },
            )

        mocked_group_reply.assert_not_called()
        mocked_group_forward.assert_called_once()

    def test_direct_parent_session_does_not_override_child_assistant_target_to_parent_group(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "child-group-1",
            "parent_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }
        parent_mapping = {
            "session_key": "agent:main:clawchat:direct:sender-user",
            "group_id": "parent-group-1",
            "origin_kind": "direct_user",
            "origin_user_id": "sender-user",
            "chatbot_user_id": "chatbot-user",
            "effective_target_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }

        def mock_get_session_mapping(_app_id, _node_id, session_key):
            if session_key == "agent:main:subagent:test-child":
                return mapping
            if session_key == "agent:main:clawchat:direct:sender-user":
                return parent_mapping
            return None

        with mock.patch.object(m, "is_session_map_sync_enabled", return_value=True), \
             mock.patch.object(m, "get_session_mapping_by_session", side_effect=mock_get_session_mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "target_user_id": "sender-user",
                 "origin_kind": "direct_user",
                 "origin_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "maybe_deliver_session_transcript_ai_dynamic", return_value={"result": "not_intermediate"}), \
             mock.patch.object(m, "maybe_finish_session_transcript_ai_dynamic"), \
             mock.patch.object(m, "forward_session_sync_to_direct", return_value=301) as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group", return_value=0) as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "type": "session_message_sync",
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "trigger_msg_id": "request-direct-parent-1",
                    "message": {
                        "role": "assistant",
                        "content": "child reply should go back to the direct parent chat",
                    },
                },
            )

        mocked_direct.assert_called_once()
        mocked_group.assert_not_called()

    def test_router_child_mapping_errors_without_bound_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15"}
        lineage = {
            "session_key": "agent:main:subagent:test-child",
            "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m, "resolve_session_lineage", return_value=lineage), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "get_node_chatbot_id", return_value=""), \
             mock.patch.object(m, "create_openclaw_session_group") as mocked_create_group:
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:subagent:test-child",
                lineage["parent_session_key"],
                lineage["root_session_key"],
                "sender-user",
            )

        self.assertEqual(result["result"], "error")
        self.assertEqual(result["message"], "router chatbot user not ready")
        mocked_create_group.assert_not_called()

class ConfigBatchSyncTests(unittest.TestCase):
    def test_build_config_batch_entries_from_patch_config_flattens_nested_scalars_and_arrays(self):
        m = lanying_openclaw
        patch_config = {
            "models": {
                "providers": {
                    "lanying": {
                        "baseUrl": "https://connector.lanyingim.com/v1",
                        "apiKey": "token-1",
                        "models": [
                            {
                                "id": "openai/gpt-5-mini",
                                "name": "openai/gpt-5-mini",
                            }
                        ],
                    }
                }
            },
            "agents": {
                "defaults": {
                    "model": {
                        "primary": "lanying/openai/gpt-5-mini",
                        "fallbacks": ["lanying/volcengine/DeepSeek-R1"],
                    }
                }
            }
        }

        batch_entries = m.build_config_batch_entries_from_patch_config(patch_config)

        self.assertEqual(batch_entries, [
            {
                "path": "models.providers.lanying.baseUrl",
                "value": "https://connector.lanyingim.com/v1",
            },
            {
                "path": "models.providers.lanying.apiKey",
                "value": "token-1",
            },
            {
                "path": "models.providers.lanying.models",
                "value": [
                    {
                        "id": "openai/gpt-5-mini",
                        "name": "openai/gpt-5-mini",
                    }
                ],
            },
            {
                "path": "agents.defaults.model.primary",
                "value": "lanying/openai/gpt-5-mini",
            },
            {
                "path": "agents.defaults.model.fallbacks",
                "value": ["lanying/volcengine/DeepSeek-R1"],
            },
        ])

    def test_build_config_batch_entries_from_patch_config_keeps_null_values_in_config_set_shape(self):
        m = lanying_openclaw
        patch_config = {
            "models": {
                "providers": {
                    "lanying": {
                        "apiKey": None,
                    }
                }
            }
        }

        batch_entries = m.build_config_batch_entries_from_patch_config(patch_config)

        self.assertEqual(batch_entries, [
            {
                "path": "models.providers.lanying.apiKey",
                "value": None,
            }
        ])

    def test_update_node_config_sends_batch_entries_and_legacy_raw_patch_together(self):
        m = lanying_openclaw
        patch_config = {
            "agents": {
                "defaults": {
                    "model": {
                        "primary": "lanying/openai/gpt-5-mini",
                    }
                }
            }
        }

        with mock.patch.object(m, "get_node", return_value={"user_id": "node-user"}), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=9) as mocked_send:
            result = m.update_node_config("app-id", "node-1", patch_config)

        self.assertEqual(result["result"], "ok")
        send_args = mocked_send.call_args.args
        self.assertEqual(send_args[1], "app-id")
        self.assertEqual(send_args[2], "node-user")
        self.assertEqual(send_args[3], "node-user")
        extra = send_args[7]
        openclaw_ext = extra["ext"]["openclaw"]
        self.assertEqual(openclaw_ext["type"], "config_patch")
        self.assertEqual(openclaw_ext["formatVersion"], 3)
        self.assertEqual(openclaw_ext["restart"], True)
        self.assertEqual(json.loads(openclaw_ext["raw"]), patch_config)
        self.assertEqual(openclaw_ext["batchEntries"], openclaw_ext["batch_entries"])
        self.assertEqual(openclaw_ext["batchEntries"], [
            {
                "path": "agents.defaults.model.primary",
                "value": "lanying/openai/gpt-5-mini",
            }
        ])

    def test_inspect_session_mapping_canonical_states_for_node_repairs_router_direct_root_fields(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        detail = {
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "app_id": "legacy-app",
            "node_id": "legacy-node",
            "openclaw_user_id": "legacy-openclaw",
            "management_user_id": "",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "group_id": "legacy-group",
            "parent_session_key": "",
            "root_session_key": "",
            "effective_target_session_key": "",
        }

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "manager-1"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=[detail]):
            result = m.inspect_session_mapping_canonical_states_for_node("app-id", node_info)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["dirty_mapping_count"], 1)
        report = result["data"]["mapping_reports"][0]
        proposed_by_field = {
            change["field"]: change["to"]
            for change in report["proposed_changes"]
            if change.get("action") == "mapping_field_update"
        }
        self.assertEqual(proposed_by_field["app_id"], "app-id")
        self.assertEqual(proposed_by_field["node_id"], "15")
        self.assertEqual(proposed_by_field["openclaw_user_id"], "openclaw-user")
        self.assertEqual(proposed_by_field["management_user_id"], "manager-1")
        self.assertEqual(proposed_by_field["root_session_key"], "agent:main:clawchat-router:direct:6632092019520")
        self.assertEqual(proposed_by_field["effective_target_session_key"], "agent:main:clawchat-router:direct:6632092019520")
        self.assertEqual(proposed_by_field["group_id"], "")
        self.assertEqual(proposed_by_field["origin_kind"], "direct_user")
        self.assertEqual(proposed_by_field["origin_user_id"], "6632092019520")
        self.assertEqual(proposed_by_field["chatbot_user_id"], "chatbot-user")

    def test_inspect_session_mapping_canonical_states_for_node_repairs_router_group_fields(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        detail = {
            "session_key": "agent:main:clawchat-router:group:group-9",
            "app_id": "app-id",
            "node_id": "15",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "manager-1",
            "origin_kind": "",
            "origin_user_id": "",
            "chatbot_user_id": "",
            "group_id": "wrong-group",
            "parent_session_key": "",
            "root_session_key": "",
            "effective_target_session_key": "",
        }

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "manager-1"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=[detail]):
            result = m.inspect_session_mapping_canonical_states_for_node("app-id", node_info)

        self.assertEqual(result["result"], "ok")
        report = result["data"]["mapping_reports"][0]
        proposed_by_field = {
            change["field"]: change["to"]
            for change in report["proposed_changes"]
            if change.get("action") == "mapping_field_update"
        }
        self.assertEqual(proposed_by_field["group_id"], "group-9")
        self.assertEqual(proposed_by_field["root_session_key"], "agent:main:clawchat-router:group:group-9")
        self.assertEqual(proposed_by_field["effective_target_session_key"], "agent:main:clawchat-router:group:group-9")
        self.assertEqual(proposed_by_field["chatbot_user_id"], "chatbot-user")

    def test_inspect_session_mapping_canonical_states_for_node_repairs_non_empty_wrong_root_session_key(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        detail = {
            "session_key": "agent:main:clawchat-router:group:group-9",
            "app_id": "app-id",
            "node_id": "15",
            "openclaw_user_id": "openclaw-user",
            "management_user_id": "manager-1",
            "origin_kind": "",
            "origin_user_id": "",
            "chatbot_user_id": "chatbot-user",
            "group_id": "group-9",
            "parent_session_key": "",
            "root_session_key": "agent:main:clawchat-router:group:wrong-root",
            "effective_target_session_key": "agent:main:clawchat-router:group:wrong-root",
        }

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "manager-1"}}), \
             mock.patch.object(m.lanying_openclaw, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=[detail]):
            result = m.inspect_session_mapping_canonical_states_for_node("app-id", node_info)

        report = result["data"]["mapping_reports"][0]
        proposed_by_field = {
            change["field"]: change["to"]
            for change in report["proposed_changes"]
            if change.get("action") == "mapping_field_update"
        }
        self.assertEqual(proposed_by_field["root_session_key"], "agent:main:clawchat-router:group:group-9")
        self.assertEqual(proposed_by_field["effective_target_session_key"], "agent:main:clawchat-router:group:group-9")

    def test_render_inspect_session_mapping_canonical_html_for_node_highlights_origin_identity(self):
        m = lanying_openclaw_migration
        details = [{
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "app_id": "uioczdkuvci",
            "node_id": "8",
            "openclaw_user_id": "6760921908880",
            "management_user_id": "6632092019520",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "6674822238512",
            "group_id": "6632098115105",
            "parent_session_key": "",
            "root_session_key": "agent:main:clawchat-router:direct:6632092019520",
            "effective_target_session_key": "agent:main:clawchat-router:direct:6632092019520",
        }]
        inspect_result = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:clawchat-router:direct:6632092019520",
                    "status": "dirty",
                    "root_mode": "router_direct",
                    "target_user_id": "6632092019520",
                    "expected_fields": {
                        "origin_kind": "direct_user",
                        "origin_user_id": "6632092019520",
                    },
                    "issues": [
                        {
                            "severity": "error",
                            "code": "direct_root_origin_kind_mismatch",
                            "message": "direct root lineage 的 origin_kind 与当前规则不一致",
                        },
                        {
                            "severity": "error",
                            "code": "direct_root_origin_user_mismatch",
                            "message": "direct root lineage 的 origin_user_id 与当前规则不一致",
                        },
                    ],
                    "proposed_changes": [],
                }]
            },
        }

        with mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=details), \
             mock.patch.object(m.lanying_openclaw, "get_session_last_message_time", return_value=1234567), \
             mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", return_value=inspect_result):
            html_text = m.render_inspect_session_mapping_canonical_html_for_node("app-id", "8")

        self.assertIn("origin_identity", html_text)
        self.assertIn("current_origin_kind", html_text)
        self.assertIn("openclaw_control", html_text)
        self.assertIn("expected_origin_kind", html_text)
        self.assertIn("direct_user", html_text)
        self.assertIn("expected_origin_user_id", html_text)
        self.assertIn("6632092019520", html_text)
        self.assertIn("origin_repair_reason", html_text)
        self.assertIn("direct root lineage inferred from root_session_key", html_text)
        self.assertIn("last_message_time", html_text)
        self.assertIn("1234567 (1970-01-01 08:20:34)", html_text)

    def test_migrate_inspected_session_mapping_canonical_state_applies_all_mapping_field_updates(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        before_report = {
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "status": "dirty",
            "proposed_changes": [
                {
                    "action": "mapping_field_update",
                    "field": "origin_kind",
                    "to": "direct_user",
                },
                {
                    "action": "mapping_field_update",
                    "field": "origin_user_id",
                    "to": "6632092019520",
                },
                {
                    "action": "mapping_field_update",
                    "field": "chatbot_user_id",
                    "to": "chatbot-user",
                },
            ],
        }
        before_inspect = {
            "result": "ok",
            "data": {"mapping_reports": [before_report]},
        }
        after_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [
                    {
                        "session_key": "agent:main:clawchat-router:direct:6632092019520",
                        "status": "clean",
                        "proposed_changes": [],
                    }
                ]
            },
        }
        mapping = {
            "session_key": "agent:main:clawchat-router:direct:6632092019520",
            "origin_kind": "openclaw_control",
            "origin_user_id": "",
            "chatbot_user_id": "",
        }

        with mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", side_effect=[before_inspect, after_inspect]), \
             mock.patch.object(m, "render_inspect_session_mapping_canonical_html_for_node", side_effect=["before-html", "after-html"]), \
             mock.patch.object(m.lanying_openclaw, "get_session_mapping_by_session", return_value=dict(mapping)), \
             mock.patch.object(m.lanying_openclaw, "rewrite_session_mapping_for_migration", side_effect=lambda app_id, node_id, body, change_source='': {"result": "ok", "data": body}) as mocked_set:
            result = m.migrate_inspected_session_mapping_canonical_state(
                "app-id",
                node_info,
                "agent:main:clawchat-router:direct:6632092019520",
            )

        self.assertEqual(result["result"], "ok")
        saved_mapping = mocked_set.call_args.args[2]
        self.assertEqual(saved_mapping["origin_kind"], "direct_user")
        self.assertEqual(saved_mapping["origin_user_id"], "6632092019520")
        self.assertEqual(saved_mapping["chatbot_user_id"], "chatbot-user")
        self.assertEqual(result["data"]["before_html"], "before-html")
        self.assertEqual(result["data"]["after_html"], "after-html")

    def test_migrate_inspected_session_mapping_canonical_states_for_node_reuses_single_inspect_cycle(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        before_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [
                    {
                        "session_key": "agent:main:clawchat-router:direct:user-1",
                        "status": "dirty",
                        "proposed_changes": [
                            {"action": "mapping_field_update", "field": "origin_kind", "to": "direct_user"},
                        ],
                    },
                    {
                        "session_key": "agent:main:clawchat-router:group:group-1",
                        "status": "dirty",
                        "proposed_changes": [
                            {"action": "mapping_field_update", "field": "group_id", "to": "group-1"},
                        ],
                    },
                ],
            },
        }
        after_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [
                    {
                        "session_key": "agent:main:clawchat-router:direct:user-1",
                        "status": "clean",
                        "proposed_changes": [],
                    },
                    {
                        "session_key": "agent:main:clawchat-router:group:group-1",
                        "status": "clean",
                        "proposed_changes": [],
                    },
                ],
            },
        }
        mapping_lookup = {
            "agent:main:clawchat-router:direct:user-1": {
                "session_key": "agent:main:clawchat-router:direct:user-1",
                "origin_kind": "openclaw_control",
            },
            "agent:main:clawchat-router:group:group-1": {
                "session_key": "agent:main:clawchat-router:group:group-1",
                "group_id": "wrong-group",
            },
        }

        def _get_mapping(app_id, node_id, session_key):
            return dict(mapping_lookup[session_key])

        with mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", side_effect=[before_inspect, after_inspect]) as mocked_inspect, \
             mock.patch.object(m, "render_inspect_session_mapping_canonical_html_for_node") as mocked_render, \
             mock.patch.object(m.lanying_openclaw, "get_session_mapping_by_session", side_effect=_get_mapping), \
             mock.patch.object(m.lanying_openclaw, "rewrite_session_mapping_for_migration", side_effect=lambda app_id, node_id, body, change_source='': {"result": "ok", "data": body}) as mocked_set:
            result = m.migrate_inspected_session_mapping_canonical_states_for_node("app-id", node_info, dry_run=False)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(mocked_inspect.call_count, 2)
        mocked_render.assert_not_called()
        self.assertEqual(mocked_set.call_count, 2)
        self.assertEqual(result["data"]["dirty_before_count"], 2)

    def test_migrate_inspected_session_mapping_canonical_states_for_node_converges_multiple_rounds(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        first_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:child-1",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "mapping_field_update", "field": "root_session_key", "to": "agent:main:clawchat-router:group:group-1"},
                    ],
                }]
            },
        }
        second_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:child-1",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "mapping_field_update", "field": "effective_target_session_key", "to": "agent:main:clawchat-router:group:group-1"},
                    ],
                }]
            },
        }
        clean_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:child-1",
                    "status": "clean",
                    "proposed_changes": [],
                }]
            },
        }
        mapping = {
            "session_key": "agent:main:subagent:child-1",
            "root_session_key": "",
            "effective_target_session_key": "",
        }

        with mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", side_effect=[first_inspect, second_inspect, clean_inspect]) as mocked_inspect, \
             mock.patch.object(m.lanying_openclaw, "get_session_mapping_by_session", return_value=dict(mapping)), \
             mock.patch.object(m.lanying_openclaw, "rewrite_session_mapping_for_migration", side_effect=lambda app_id, node_id, body, change_source='': {"result": "ok", "data": body}) as mocked_set:
            result = m.migrate_inspected_session_mapping_canonical_states_for_node("app-id", node_info, dry_run=False)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["stop_reason"], "clean")
        self.assertEqual(result["data"]["rounds_run"], 3)
        self.assertEqual(mocked_inspect.call_count, 3)
        self.assertEqual(mocked_set.call_count, 2)
        self.assertEqual(
            [entry["round"] for entry in result["data"]["migration_results"][0]["data"]["applied_changes"]],
            [1, 2],
        )

    def test_migrate_inspected_session_mapping_canonical_states_for_node_stops_on_repeated_proposals(self):
        m = lanying_openclaw_migration
        node_info = {"node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}
        repeated_inspect = {
            "result": "ok",
            "data": {
                "mapping_reports": [{
                    "session_key": "agent:main:subagent:loop-1",
                    "status": "dirty",
                    "proposed_changes": [
                        {"action": "mapping_field_update", "field": "root_session_key", "from": "", "to": "agent:main:clawchat-router:group:group-1"},
                    ],
                }]
            },
        }
        mapping = {
            "session_key": "agent:main:subagent:loop-1",
            "root_session_key": "",
        }

        with mock.patch.object(m, "inspect_session_mapping_canonical_states_for_node", side_effect=[repeated_inspect, repeated_inspect, repeated_inspect]) as mocked_inspect, \
             mock.patch.object(m.lanying_openclaw, "get_session_mapping_by_session", return_value=dict(mapping)), \
             mock.patch.object(m.lanying_openclaw, "rewrite_session_mapping_for_migration", side_effect=lambda app_id, node_id, body, change_source='': {"result": "ok", "data": body}) as mocked_set:
            result = m.migrate_inspected_session_mapping_canonical_states_for_node("app-id", node_info, dry_run=False)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["stop_reason"], "repeated_proposed_changes")
        self.assertEqual(mocked_inspect.call_count, 3)
        self.assertEqual(mocked_set.call_count, 1)


if __name__ == "__main__":
    unittest.main()
