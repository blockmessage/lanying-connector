import importlib.util
import json
import pathlib
import sys
import types
import unittest
from unittest import mock


def _fake_requests_call(*args, **kwargs):
    raise RuntimeError("fake requests module: please mock requests.request/post in tests")


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
            get_chatbot=lambda app_id, chatbot_id: {"user_id": "chatbot-user"},
        ),
        "lanying_im_api": types.SimpleNamespace(
            send_message_sync=lambda *args, **kwargs: 1,
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=lambda raw, default=None: default if default is not None else {},
        ),
        "lanying_vendor": types.SimpleNamespace(),
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

def _load_lanying_migration(openclaw_module):
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_migration.py"
    module_name = "lanying_migration_router_identity_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_openclaw": openclaw_module,
        "lanying_chatbot": types.SimpleNamespace(
            get_chatbot=lambda app_id, chatbot_id: {"user_id": "chatbot-user"},
        ),
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: object(),
            redis_keys=lambda *args, **kwargs: [],
            redis_get=lambda *args, **kwargs: None,
        ),
        "lanying_ai_capsule": types.SimpleNamespace(),
        "lanying_grow_ai": types.SimpleNamespace(
            GitBookSummary=lambda summary_text: types.SimpleNamespace(summary_list=[], to_markdown=lambda: summary_text),
        ),
        "requests": types.SimpleNamespace(
            post=_fake_requests_call,
            get=_fake_requests_call,
            request=_fake_requests_call,
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_openclaw = _load_lanying_openclaw()
lanying_migration = _load_lanying_migration(lanying_openclaw)

class FakeRedis:
    def __init__(self):
        self.values = {}
        self.sets = {}

    def set(self, key, value):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

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


class RouterSessionIdentityTests(unittest.TestCase):
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

    def test_migrate_legacy_session_mappings_for_node_reports_conflict(self):
        m = lanying_openclaw
        mm = lanying_migration
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
        conflicting_canonical_mapping = {
            "session_key": canonical_session_key,
            "group_id": "another-group",
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
            json.dumps(conflicting_canonical_mapping),
        )
        redis.sadd(m.get_openclaw_session_mapping_index_key("app-id", "node-1"), old_session_key)

        with mock.patch.object(mm.lanying_redis, "get_redis_connection", return_value=redis), \
             mock.patch.object(mm.lanying_redis, "redis_get", side_effect=lambda r, key: r.values.get(key)):
            result = mm.migrate_legacy_session_mappings_for_node(
                "app-id",
                {"node_id": "node-1"},
                dry_run=False,
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["conflicts"], 1)
        self.assertIn(
            m.get_openclaw_session_mapping_by_session_storage_key("app-id", "node-1", old_session_key),
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

    def test_migrate_session_mapping_group_admins_for_node_uses_sync_repair(self):
        m = lanying_migration
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}

        with mock.patch.object(m.lanying_openclaw, "ensure_openclaw_app_manager_user", return_value={
            "result": "ok",
            "data": {"user_id": "management-user"},
        }), \
             mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=[
                 {"group_id": "group-9"},
                 {"group_id": "group-9"},
                 {"group_id": "group-10"},
             ]), \
             mock.patch.object(m.lanying_openclaw, "ensure_user_group_admin_sync", side_effect=[True, False]) as mocked_repair:
            result = m.migrate_session_mapping_group_admins_for_node("app-id", node_info, dry_run=False)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["total_groups"], 2)
        self.assertEqual(result["data"]["success_groups"], 1)
        self.assertEqual(mocked_repair.call_args_list[0].args, ("app-id", "management-user", "group-9"))
        self.assertEqual(mocked_repair.call_args_list[1].args, ("app-id", "management-user", "group-10"))

    def test_list_openclaw_node_list_app_ids_scans_node_list_keys(self):
        m = lanying_migration
        fake_redis = types.SimpleNamespace(
            scan_iter=lambda match, count=100: iter([
                b"lanying_connector:openclaw:node_list:app-b",
                b"lanying_connector:openclaw:node_list:app-a",
                b"lanying_connector:openclaw:node_list:app-b",
            ])
        )
        with mock.patch.object(m.lanying_redis, "get_redis_connection", return_value=fake_redis):
            app_ids = m.list_openclaw_node_list_app_ids()
        self.assertEqual(app_ids, ["app-a", "app-b"])

    def test_migrate_session_mapping_group_admins_scans_all_apps_when_app_id_empty(self):
        m = lanying_migration
        with mock.patch.object(m, "list_openclaw_node_list_app_ids", return_value=["app-a", "app-b"]), \
             mock.patch.object(m.lanying_openclaw, "get_node_list", side_effect=[
                 {"result": "ok", "data": {"list": [{"node_id": "1"}, {"node_id": "2"}]}},
                 {"result": "ok", "data": {"list": [{"node_id": "3"}]}},
             ]), \
             mock.patch.object(m, "migrate_session_mapping_group_admins_for_node", side_effect=[
                 {"result": "ok", "data": {"total_groups": 2, "success_groups": 2}},
                 {"result": "ok", "data": {"total_groups": 3, "success_groups": 2}},
                 {"result": "ok", "data": {"total_groups": 5, "success_groups": 4}},
             ]):
            result = m.migrate_session_mapping_group_admins("", dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["app_count"], 2)
        self.assertEqual(result["data"]["node_count"], 3)
        self.assertEqual(result["data"]["total_groups"], 10)
        self.assertEqual(result["data"]["success_groups"], 8)
        self.assertEqual(len(result["data"]["app_results"]), 2)

    def test_migrate_session_mapping_management_users_for_node_uses_mapping_management_user(self):
        m = lanying_migration
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user", "session_map_sync": "on"}

        with mock.patch.object(m.lanying_openclaw, "list_session_mappings_for_node", return_value=[
            {"group_id": "group-9", "management_user_id": "manager-a"},
            {"group_id": "group-9", "management_user_id": "manager-a"},
            {"group_id": "group-9", "management_user_id": "manager-b"},
            {"group_id": "group-10", "management_user_id": ""},
        ]), \
             mock.patch.object(m.lanying_openclaw, "ensure_user_group_admin_sync", side_effect=[True, False]) as mocked_repair:
            result = m.migrate_session_mapping_management_users_for_node("app-id", node_info, dry_run=False)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["total_groups"], 2)
        self.assertEqual(result["data"]["success_groups"], 1)
        self.assertEqual(mocked_repair.call_args_list[0].args, ("app-id", "manager-a", "group-9"))
        self.assertEqual(mocked_repair.call_args_list[1].args, ("app-id", "manager-b", "group-9"))

    def test_migrate_session_mapping_management_users_scans_all_apps_when_app_id_empty(self):
        m = lanying_migration
        with mock.patch.object(m, "list_openclaw_node_list_app_ids", return_value=["app-a", "app-b"]), \
             mock.patch.object(m.lanying_openclaw, "get_node_list", side_effect=[
                 {"result": "ok", "data": {"list": [{"node_id": "1"}, {"node_id": "2"}]}},
                 {"result": "ok", "data": {"list": [{"node_id": "3"}]}},
             ]), \
             mock.patch.object(m, "migrate_session_mapping_management_users_for_node", side_effect=[
                 {"result": "ok", "data": {"total_groups": 1, "success_groups": 1}},
                 {"result": "ok", "data": {"total_groups": 2, "success_groups": 1}},
                 {"result": "ok", "data": {"total_groups": 4, "success_groups": 4}},
             ]):
            result = m.migrate_session_mapping_management_users("", dry_run=True)

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["app_count"], 2)
        self.assertEqual(result["data"]["node_count"], 3)
        self.assertEqual(result["data"]["total_groups"], 7)
        self.assertEqual(result["data"]["success_groups"], 6)
        self.assertEqual(len(result["data"]["app_results"]), 2)

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
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
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
        self.assertEqual(extra["ext"]["openclaw"]["parent_session"], "agent:main:session-parent")
        self.assertEqual(extra["ext"]["openclaw"]["root_session"], "agent:main:session-root")
        self.assertEqual(extra["ext"]["ai"]["ai_generate"], False)
        self.assertEqual(extra["skip_antispam_prompt"], True)

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
             mock.patch.object(m, "ensure_user_group_admin", return_value=True) as mocked_ensure_admin, \
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
             mock.patch.object(m, "ensure_user_group_admin", return_value=True) as mocked_ensure_admin, \
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

    def test_ensure_session_mapping_admin_promotion_failure_is_non_blocking(self):
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
             mock.patch.object(m, "ensure_user_group_admin", return_value=False), \
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

    def test_router_reply_message_carries_openclaw_delivery_context(self):
        m = lanying_openclaw
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
        self.assertEqual(ext["openclaw"]["request_msg_id"], "im-req-1")

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
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
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
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_direct_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
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
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
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
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
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


if __name__ == "__main__":
    unittest.main()
