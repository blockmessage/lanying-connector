import importlib.util
import json
import pathlib
import sys
import types
import unittest
from unittest import mock


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.values = {}

    def hmset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[str(field)] = value

    def hsetnx(self, key, field, value):
        bucket = self.hashes.setdefault(key, {})
        if str(field) not in bucket:
            bucket[str(field)] = value

    def hdel(self, key, field):
        self.hashes.setdefault(key, {}).pop(str(field), None)

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def lrem(self, key, count, value):
        items = self.lists.get(key, [])
        removed = 0
        kept = []
        for item in items:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            kept.append(item)
        self.lists[key] = kept

    def incrby(self, key, amount):
        value = int(self.values.get(key, 0)) + int(amount)
        self.values[key] = value
        return value

    def set(self, key, value):
        self.values[key] = value


def _safe_json_loads(raw, default=None):
    if not isinstance(raw, str):
        return default if default is not None else {}
    try:
        return json.loads(raw)
    except Exception:
        return default if default is not None else {}


def _load_lanying_chatbot():
    fake_redis = FakeRedis()
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_chatbot.py"
    module_name = "lanying_chatbot_access_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_hget=lambda redis, key, field: redis.hashes.get(key, {}).get(str(field), ""),
            redis_hgetall=lambda redis, key: dict(redis.hashes.get(key, {})),
            redis_lrange=lambda redis, key, start, end: list(redis.lists.get(key, [])),
        ),
        "lanying_ai_capsule": types.SimpleNamespace(
            generate_capsule_id=lambda app_id, chatbot_id: f"capsule-{chatbot_id}",
        ),
        "lanying_im_api": types.SimpleNamespace(
            set_user_profile=lambda *args, **kwargs: {"code": 200},
            set_user_avatar=lambda *args, **kwargs: {"code": 200},
            set_user_stranger_chat=lambda *args, **kwargs: {"code": 200},
            set_auth_mode=lambda *args, **kwargs: {"code": 200},
            admin_add_roster_direct=lambda *args, **kwargs: {"code": 200},
            roster_delete=lambda *args, **kwargs: {"code": 200},
            get_user_profile=lambda *args, **kwargs: {"code": 200, "data": {"private_info": "{}", "public_info": "{}"}},
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=_safe_json_loads,
        ),
        "lanying_oss": types.SimpleNamespace(),
        "lanying_openclaw": types.SimpleNamespace(
            get_chatbot_node_id=lambda *args, **kwargs: "",
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module, fake_redis


class ChatbotAccessTests(unittest.TestCase):
    def setUp(self):
        self.module, self.fake_redis = _load_lanying_chatbot()

    def create_chatbot(self, access_type="public", user_id=1001, access_list="", show_in_support=None):
        m = self.module
        return m.create_chatbot(
            "app-id",
            "bot-name",
            "Bot Name",
            "bot desc",
            "",
            user_id,
            "https://lanying.link/bot",
            {"messages": [{"role": "system", "content": "hello"}]},
            20,
            1,
            4096,
            100,
            [],
            "welcome",
            "capsule",
            "",
            "all",
            "off",
            "off",
            "whisper-1",
            self.module.get_default_link_profile(),
            "on",
            access_type=access_type,
            access_list=access_list,
            show_in_support=show_in_support,
        )

    def seed_chatbot(self, access_type="public", user_id=1001, access_list="", show_in_support=None):
        m = self.module
        chatbot_id = "chatbot-1"
        if show_in_support is None:
            show_in_support = "off" if access_type == "friend" else "on"
        self.fake_redis.hmset(
            m.get_chatbot_key("app-id", chatbot_id),
            {
                "chatbot_id": chatbot_id,
                "create_time": 1,
                "app_id": "app-id",
                "name": "bot-name",
                "nickname": "Bot Name",
                "desc": "bot desc",
                "avatar": "",
                "user_id": user_id,
                "lanying_link": "https://lanying.link/bot",
                "preset": json.dumps({"messages": [{"role": "system", "content": "hello"}]}, ensure_ascii=False),
                "history_msg_count_max": 20,
                "history_msg_count_min": 1,
                "history_msg_size_max": 4096,
                "message_per_month_per_user": 100,
                "chatbot_ids": json.dumps([], ensure_ascii=False),
                "capsule_id": f"capsule-{chatbot_id}",
                "welcome_message": "welcome",
                "quota_exceed_reply_type": "capsule",
                "quota_exceed_reply_msg": "",
                "group_history_use_mode": "all",
                "audio_to_text": "off",
                "image_vision": "off",
                "audio_to_text_model": "whisper-1",
                "link_profile": json.dumps(self.module.get_default_link_profile(), ensure_ascii=False),
                "content_security": "on",
                "access_type": access_type,
                "access_list": access_list,
                "show_in_support": show_in_support,
            },
        )
        self.fake_redis.rpush(m.get_chatbot_ids_key("app-id"), chatbot_id)
        m.set_user_chatbot_id("app-id", user_id, chatbot_id)
        m.set_name_chatbot_id("app-id", "bot-name", chatbot_id)
        return chatbot_id

    def configure_chatbot(self, chatbot_id, access_type="public", user_id=1001, history_msg_count_max=20, access_list="", show_in_support=None):
        m = self.module
        return m.configure_chatbot(
            "app-id",
            "1",
            "enterprise",
            "advanced",
            chatbot_id,
            "bot-name",
            "Bot Name",
            "bot desc",
            "",
            user_id,
            "https://lanying.link/bot",
            {"messages": [{"role": "system", "content": "hello"}]},
            history_msg_count_max,
            1,
            4096,
            100,
            [],
            "welcome",
            "capsule",
            "",
            "all",
            "off",
            "off",
            "whisper-1",
            self.module.get_default_link_profile(),
            "on",
            access_type=access_type,
            access_list=access_list,
            show_in_support=show_in_support,
        )

    def test_init_chatbot_im_user_setting_public_syncs_im_setting(self):
        m = self.module
        self.seed_chatbot(access_type="public", user_id=1001)
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}) as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}) as mocked_auth:
            result = m.init_chatbot_im_user_setting("app-id", None, {
                "chatbot_id": "chatbot-1",
                "user_id": 1001,
                "access_type": "public",
            })
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_called_once_with("app-id", 1001, 1)
        mocked_auth.assert_not_called()

    def test_init_chatbot_im_user_setting_friend_syncs_im_setting(self):
        m = self.module
        self.seed_chatbot(access_type="friend", user_id=1001)
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}) as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}) as mocked_auth:
            result = m.init_chatbot_im_user_setting("app-id", None, {
                "chatbot_id": "chatbot-1",
                "user_id": 1001,
                "access_type": "friend",
            })
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_called_once_with("app-id", 1001, 2)
        mocked_auth.assert_called_once_with("app-id", 1001, 1)

    def test_init_chatbot_im_user_setting_friend_syncs_access_list(self):
        m = self.module
        self.seed_chatbot(access_type="friend", user_id=1001)
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}), \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}), \
             mock.patch.object(m.lanying_im_api, "admin_add_roster_direct", return_value={"code": 200}) as mocked_add_roster, \
             mock.patch.object(m.lanying_im_api, "roster_delete", return_value={"code": 200}) as mocked_delete_roster:
            result = m.init_chatbot_im_user_setting("app-id", None, {
                "chatbot_id": "chatbot-1",
                "user_id": 1001,
                "access_type": "friend",
                "access_list": "2001 2002",
            })
        self.assertEqual(result["result"], "ok")
        self.assertEqual(mocked_add_roster.call_args_list, [
            mock.call("app-id", 1001, [2001]),
            mock.call("app-id", 1001, [2002]),
        ])
        mocked_delete_roster.assert_not_called()

    def test_init_chatbot_im_user_setting_returns_error_when_sync_fails(self):
        m = self.module
        self.seed_chatbot(access_type="public", user_id=1001)
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 500, "message": "bad request"}):
            result = m.init_chatbot_im_user_setting("app-id", None, {
                "chatbot_id": "chatbot-1",
                "user_id": 1001,
                "access_type": "public",
            })
        self.assertEqual(result["result"], "error")
        self.assertIn("set_user_stranger_chat failed", result["message"])

    def test_create_chatbot_does_not_sync_im_setting(self):
        m = self.module
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}) as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}) as mocked_auth:
            result = self.create_chatbot(access_type="public")
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_not_called()
        mocked_auth.assert_not_called()
        chatbot = m.get_chatbot("app-id", result["data"]["id"])
        self.assertEqual(chatbot["access_type"], "public")
        self.assertEqual(chatbot["show_in_support"], "on")

    def test_create_chatbot_can_store_public_non_support(self):
        m = self.module
        result = self.create_chatbot(access_type="public", show_in_support="off")
        self.assertEqual(result["result"], "ok")
        chatbot = m.get_chatbot("app-id", result["data"]["id"])
        self.assertEqual(chatbot["show_in_support"], "off")

    def test_configure_chatbot_public_to_friend_resyncs_im_setting(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="public")
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}) as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}) as mocked_auth:
            result = self.configure_chatbot(chatbot_id, access_type="friend")
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_called_once_with("app-id", 1001, 2)
        mocked_auth.assert_called_once_with("app-id", 1001, 1)
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["access_type"], "friend")
        self.assertEqual(chatbot["show_in_support"], "off")

    def test_configure_chatbot_friend_access_list_updates_roster(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="friend", access_list="2001 2002")
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat") as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode") as mocked_auth, \
             mock.patch.object(m.lanying_im_api, "admin_add_roster_direct", return_value={"code": 200}) as mocked_add_roster, \
             mock.patch.object(m.lanying_im_api, "roster_delete", return_value={"code": 200}) as mocked_delete_roster:
            result = self.configure_chatbot(chatbot_id, access_type="friend", access_list="2002 2003")
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_not_called()
        mocked_auth.assert_not_called()
        mocked_add_roster.assert_called_once_with("app-id", 1001, [2003])
        mocked_delete_roster.assert_called_once_with("app-id", 1001, 2001)
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["access_list"], "2002 2003")

    def test_configure_chatbot_skips_im_setting_sync_when_access_unchanged(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="public")
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat") as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode") as mocked_auth:
            result = self.configure_chatbot(chatbot_id, access_type="public", history_msg_count_max=30)
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_not_called()
        mocked_auth.assert_not_called()
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["history_msg_count_max"], 30)

    def test_configure_chatbot_public_non_support_is_persisted(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="public")
        result = self.configure_chatbot(chatbot_id, access_type="public", show_in_support="off")
        self.assertEqual(result["result"], "ok")
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["show_in_support"], "off")

    def test_get_chatbot_defaults_show_in_support_on_for_legacy_public(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="public")
        del self.fake_redis.hashes[m.get_chatbot_key("app-id", chatbot_id)]["show_in_support"]
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["show_in_support"], "on")

    def test_get_chatbot_defaults_show_in_support_off_for_legacy_friend(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="friend")
        del self.fake_redis.hashes[m.get_chatbot_key("app-id", chatbot_id)]["show_in_support"]
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["show_in_support"], "off")

    def test_configure_chatbot_resyncs_im_setting_for_new_user_id(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="friend", user_id=1001, access_list="2001 2002")
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}) as mocked_stranger, \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 200}) as mocked_auth, \
             mock.patch.object(m.lanying_im_api, "admin_add_roster_direct", return_value={"code": 200}) as mocked_add_roster:
            result = self.configure_chatbot(chatbot_id, access_type="friend", user_id=2002, access_list="2001 2002")
        self.assertEqual(result["result"], "ok")
        mocked_stranger.assert_called_once_with("app-id", 2002, 2)
        mocked_auth.assert_called_once_with("app-id", 2002, 1)
        self.assertEqual(mocked_add_roster.call_args_list, [
            mock.call("app-id", 2002, [2001]),
            mock.call("app-id", 2002, [2002]),
        ])
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["user_id"], 2002)

    def test_create_chatbot_ignores_im_setting_failures(self):
        m = self.module
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 500, "message": "bad request"}):
            result = self.create_chatbot(access_type="public")
        self.assertEqual(result["result"], "ok")
        chatbot = m.get_chatbot("app-id", result["data"]["id"])
        self.assertEqual(chatbot["access_type"], "public")

    def test_configure_chatbot_returns_error_when_im_setting_sync_fails(self):
        m = self.module
        chatbot_id = self.seed_chatbot(access_type="public")
        with mock.patch.object(m.lanying_im_api, "set_user_stranger_chat", return_value={"code": 200}), \
             mock.patch.object(m.lanying_im_api, "set_auth_mode", return_value={"code": 500, "message": "auth failed"}):
            result = self.configure_chatbot(chatbot_id, access_type="friend")
        self.assertEqual(result["result"], "error")
        self.assertIn("set_auth_mode failed", result["message"])
        chatbot = m.get_chatbot("app-id", chatbot_id)
        self.assertEqual(chatbot["access_type"], "public")


if __name__ == "__main__":
    unittest.main()
