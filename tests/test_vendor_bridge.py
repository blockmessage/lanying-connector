import unittest
from unittest import mock

try:
    import lanying_vendor
except ModuleNotFoundError:
    lanying_vendor = None


class _LegacyModule:
    SUPPORT_NATIVE_TOOLS = False

    @staticmethod
    def prepare_chat(auth_info, preset):
        return {"seen_preset": preset}

    @staticmethod
    def chat(prepare_info, preset, model_config):
        return {
            "result": "ok",
            "reply": "",
            "function_call": {"name": "f1", "arguments": "{}", "id": "call_1"},
            "finish_reason": "function_call",
            "usage": {"completion_tokens": 1, "prompt_tokens": 1, "total_tokens": 2},
        }


class _NativeModule:
    SUPPORT_NATIVE_TOOLS = True

    @staticmethod
    def prepare_chat(auth_info, preset):
        return {"seen_preset": preset}

    @staticmethod
    def chat(prepare_info, preset, model_config):
        return {
            "result": "ok",
            "reply": "",
            "tool_calls": [
                {
                    "id": "call_2",
                    "type": "function",
                    "function": {"name": "f2", "arguments": "{}"},
                }
            ],
            "finish_reason": "tool_calls",
            "usage": {"completion_tokens": 1, "prompt_tokens": 1, "total_tokens": 2},
        }


@unittest.skipIf(lanying_vendor is None, "optional vendor dependencies are not installed in this environment")
class VendorBridgeTests(unittest.TestCase):
    def setUp(self):
        self.old_map = dict(lanying_vendor.vendor_to_module)
        self.old_get_chat_model_config = lanying_vendor.get_chat_model_config
        self.old_get_vendor = lanying_vendor.get_vendor

    def tearDown(self):
        lanying_vendor.vendor_to_module = self.old_map
        lanying_vendor.get_chat_model_config = self.old_get_chat_model_config
        lanying_vendor.get_vendor = self.old_get_vendor

    def _mock_model_config(self, app_id, vendor, model):
        return {"model": model, "vendor": vendor}

    def test_prepare_chat_legacy_module_receives_legacy_shape(self):
        lanying_vendor.vendor_to_module["legacy_test"] = _LegacyModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {
            "model": "m1",
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "foo", "parameters": {"type": "object"}},
                }
            ],
            "tool_choice": {"type": "function", "function": {"name": "foo"}},
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_x",
                            "type": "function",
                            "function": {"name": "foo", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_x", "content": "ok"},
            ],
        }

        out = lanying_vendor.prepare_chat("app", "legacy_test", {"api_key": "k"}, preset)
        seen = out["seen_preset"]
        self.assertIn("functions", seen)
        self.assertEqual(seen["functions"][0]["name"], "foo")
        self.assertIn("function_call", seen)
        self.assertEqual(seen["messages"][0]["function_call"]["name"], "foo")
        self.assertEqual(seen["messages"][1]["role"], "function")

    def test_prepare_chat_native_module_receives_native_shape(self):
        lanying_vendor.vendor_to_module["native_test"] = _NativeModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {
            "model": "m1",
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "foo", "parameters": {"type": "object"}},
                }
            ],
            "messages": [{"role": "user", "content": "hi"}],
        }

        out = lanying_vendor.prepare_chat("app", "native_test", {"api_key": "k"}, preset)
        seen = out["seen_preset"]
        self.assertIn("tools", seen)
        self.assertNotIn("functions", seen)

    def test_chat_normalizes_legacy_function_call_response(self):
        lanying_vendor.vendor_to_module["legacy_test"] = _LegacyModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {"model": "m1", "messages": [{"role": "user", "content": "hi"}]}
        out = lanying_vendor.chat("app", "legacy_test", {"api_key": "k"}, preset)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(out["finish_reason"], "tool_calls")
        self.assertIn("tool_calls", out)
        self.assertEqual(out["tool_calls"][0]["function"]["name"], "f1")

    def test_get_module_uses_handler_vendor_for_v2_custom_vendor(self):
        lanying_vendor.vendor_to_module["native_test"] = _NativeModule
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openrouter",
            "handler_vendor": "native_test",
            "config_version": 2,
            "model_config": [],
        }

        out = lanying_vendor.get_module("app", "custom_vendor_1")
        self.assertIs(out, _NativeModule)

    def test_v2_disabled_catalog_model_is_filtered(self):
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openai",
            "handler_vendor": "openai",
            "name": "Test",
            "config_version": 2,
            "model_config": [
                {
                    "service": "chatgpt",
                    "model": "gpt-4o-mini",
                    "enabled": False,
                    "source": "catalog",
                    "extra_params": [],
                }
            ],
        }

        out = lanying_vendor.get_chat_model_config("app", "custom_vendor_2", "gpt-4o-mini")
        self.assertIsNone(out)

    def test_v2_custom_model_inherits_service_template(self):
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openai",
            "handler_vendor": "openai",
            "name": "Test",
            "config_version": 2,
            "model_config": [
                {
                    "service": "chatgpt",
                    "model": "my-chatgpt-compatible-model",
                    "enabled": True,
                    "source": "custom",
                    "extra_params": [
                        {"key": "temperature", "value": "0.2"},
                        {"key": "reasoning", "value": "true"},
                        {"key": "input", "value": "[\"text\"]"},
                        {"key": "token_limit", "value": "128000"},
                        {"key": "max_output_tokens", "value": "8192"},
                    ],
                }
            ],
        }

        out = lanying_vendor.get_chat_model_config("app", "custom_vendor_3", "my-chatgpt-compatible-model")
        self.assertIsNotNone(out)
        self.assertEqual(out["model"], "my-chatgpt-compatible-model")
        self.assertEqual(out["vendor"], "custom_vendor_3")
        self.assertEqual(out["api_key_type"], "self")
        self.assertEqual(out["temperature"], "0.2")
        self.assertEqual(out["reasoning"], True)
        self.assertEqual(out["input"], ["text"])
        self.assertEqual(out["token_limit"], 128000)
        self.assertEqual(out["max_output_tokens"], 8192)

    def test_check_vendor_valid_normalizes_handler_vendor(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openrouter",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="",
            model_config=[],
            config_version=2,
            handler_vendor="azure",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=True):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(vendor_setting.handler_vendor, "openai")

    def test_check_vendor_valid_rejects_non_public_api_endpoint(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="http://127.0.0.1:5000/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=False):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(out["message"], "api_endpoint_not_valid")


if __name__ == "__main__":
    unittest.main()
