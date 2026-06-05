import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock


def _load_lanying_im_api():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_im_api.py"
    module_name = "lanying_im_api_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_config": types.SimpleNamespace(
            get_lanying_connector=lambda app_id: {"lanying_admin_token": "admin-token"},
            get_lanying_api_endpoint=lambda app_id: "https://api.example.com",
        ),
        "lanying_file_storage": types.SimpleNamespace(),
        "lanying_utils": types.SimpleNamespace(is_lanying_url=lambda url: True),
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: object(),
            redis_get=lambda *args, **kwargs: None,
        ),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
        "requests": types.SimpleNamespace(
            get=lambda *args, **kwargs: None,
            post=lambda *args, **kwargs: None,
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_im_api = _load_lanying_im_api()


class LanyingImApiTests(unittest.TestCase):
    def test_summarize_conversation_result_for_log_omits_message_bodies(self):
        summary = lanying_im_api.summarize_conversation_result_for_log({
            "code": 200,
            "data": {
                "is_last": False,
                "next_msg_id": 12345,
                "messages": [
                    {"msg_id": 11, "timestamp": 1001, "content": "hello"},
                    {"msg_id": 22, "timestamp": 1002, "content": "world"},
                ],
            },
        })

        self.assertEqual(summary["code"], 200)
        self.assertEqual(summary["is_last"], False)
        self.assertEqual(summary["next_msg_id"], 12345)
        self.assertEqual(summary["message_count"], 2)
        self.assertEqual(summary["first_msg_id"], 11)
        self.assertEqual(summary["last_msg_id"], 22)
        self.assertEqual(summary["last_timestamp"], 1002)
        self.assertNotIn("messages", summary)
        self.assertNotIn("content", str(summary))

    def test_fetch_conversation_messages_logs_summary_instead_of_full_messages(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {
            "code": 200,
            "data": {
                "is_last": False,
                "next_msg_id": 12345,
                "messages": [
                    {"msg_id": 11, "timestamp": 1001, "content": "secret-body-1"},
                    {"msg_id": 22, "timestamp": 1002, "content": "secret-body-2"},
                ],
            },
        }

        with mock.patch.object(lanying_im_api.requests, "get", return_value=fake_response), \
             mock.patch.object(lanying_im_api.logging, "info") as mocked_info:
            result = lanying_im_api.fetch_conversation_messages(
                {"lanying_admin_token": "admin-token"},
                "app-id",
                "100",
                "200",
                limit=20,
                msg_id_start=0,
            )

        self.assertEqual(result["code"], 200)
        logged_text = mocked_info.call_args[0][0]
        self.assertIn("result_summary:", logged_text)
        self.assertIn("message_count", logged_text)
        self.assertNotIn("secret-body-1", logged_text)
        self.assertNotIn("secret-body-2", logged_text)
        self.assertNotIn("'messages':", logged_text)

    def test_get_group_info_passes_group_id_as_query_param(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200, "data": {"group_id": "9"}}

        with mock.patch.object(lanying_im_api.requests, "get", return_value=fake_response) as mocked_get:
            result = lanying_im_api.get_group_info("app-id", "9")

        self.assertEqual(result["code"], 200)
        mocked_get.assert_called_once_with(
            "https://api.example.com/group/info",
            headers={"app_id": "app-id", "group_id": "9", "access-token": "admin-token"},
            params={"group_id": "9"},
        )

    def test_group_owner_transfer_uses_current_owner_header_and_new_owner_body(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200}

        with mock.patch.object(lanying_im_api.requests, "post", return_value=fake_response) as mocked_post:
            result = lanying_im_api.group_owner_transfer("app-id", "9", "100", "200")

        self.assertEqual(result["code"], 200)
        mocked_post.assert_called_once_with(
            "https://api.example.com/group/transfer",
            headers={"app_id": "app-id", "group_id": "9", "user_id": "100", "access-token": "admin-token"},
            json={"group_id": "9", "new_owner": 200},
        )

    def test_set_group_ext_uses_group_header_and_value_body(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200}

        with mock.patch.object(lanying_im_api.requests, "post", return_value=fake_response) as mocked_post:
            result = lanying_im_api.set_group_ext("app-id", "9", '{"k":"v"}')

        self.assertEqual(result["code"], 200)
        mocked_post.assert_called_once_with(
            "https://api.example.com/group/info/ext",
            headers={"app_id": "app-id", "access-token": "admin-token", "group_id": "9"},
            json={"group_id": "9", "value": '{"k":"v"}'},
        )

    def test_generate_secret_info_uses_user_context_and_body(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200, "data": {"code": "secret-code"}}

        with mock.patch.object(lanying_im_api.requests, "post", return_value=fake_response) as mocked_post:
            result = lanying_im_api.generate_secret_info("app-id", "88", 300, '{"username":"u","password":"p"}')

        self.assertEqual(result["code"], 200)
        mocked_post.assert_called_once_with(
            "https://api.example.com/app/secret_info",
            headers={"app_id": "app-id", "user_id": "88", "access-token": "admin-token"},
            json={"expire_seconds": 300, "secret_text": '{"username":"u","password":"p"}'},
        )


if __name__ == "__main__":
    unittest.main()
