import unittest

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

    def tearDown(self):
        lanying_vendor.vendor_to_module = self.old_map
        lanying_vendor.get_chat_model_config = self.old_get_chat_model_config

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


if __name__ == "__main__":
    unittest.main()
