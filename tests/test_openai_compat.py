import unittest

import lanying_openai_compat as compat


class OpenAICompatTests(unittest.TestCase):
    def test_normalize_chat_preset_legacy_to_tools(self):
        preset = {
            "model": "gpt-4o-mini",
            "functions": [
                {
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}
                }
            ],
            "function_call": {"name": "get_weather"},
            "messages": [
                {"role": "user", "content": "hi"},
                {
                    "role": "assistant",
                    "content": "",
                    "function_call": {"name": "get_weather", "arguments": '{"city":"Shanghai"}', "id": "call_1"}
                },
                {"role": "function", "name": "get_weather", "content": '{"temp":25}'}
            ]
        }

        out = compat.normalize_chat_preset(preset)
        self.assertIn("tools", out)
        self.assertEqual(out["tools"][0]["function"]["name"], "get_weather")
        self.assertIn("tool_choice", out)
        self.assertEqual(out["tool_choice"]["function"]["name"], "get_weather")
        self.assertEqual(out["messages"][1]["tool_calls"][0]["id"], "call_1")
        self.assertEqual(out["messages"][2]["role"], "tool")
        self.assertEqual(out["messages"][2]["tool_call_id"], "call_1")

    def test_normalize_keeps_content_list(self):
        preset = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "look"},
                        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}
                    ]
                }
            ]
        }
        out = compat.normalize_chat_preset(preset)
        self.assertIsInstance(out["messages"][0]["content"], list)
        self.assertEqual(out["messages"][0]["content"][0]["text"], "look")

    def test_to_legacy_vendor_preset(self):
        preset = {
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "f1",
                        "description": "d",
                        "parameters": {"type": "object"}
                    }
                }
            ],
            "tool_choice": {"type": "function", "function": {"name": "f1"}},
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {"id": "call_x", "type": "function", "function": {"name": "f1", "arguments": "{}"}}
                    ]
                },
                {"role": "tool", "tool_call_id": "call_x", "content": '{"ok":true}'}
            ]
        }

        out = compat.to_legacy_vendor_preset(preset)
        self.assertIn("functions", out)
        self.assertEqual(out["functions"][0]["name"], "f1")
        self.assertIn("function_call", out)
        self.assertEqual(out["function_call"]["name"], "f1")
        self.assertEqual(out["messages"][0]["function_call"]["id"], "call_x")
        self.assertEqual(out["messages"][1]["role"], "function")
        self.assertEqual(out["messages"][1]["name"], "f1")

    def test_normalize_vendor_response(self):
        response = {
            "result": "ok",
            "reply": "",
            "function_call": {"name": "do_x", "arguments": "{}", "id": "c1"},
            "finish_reason": "function_call"
        }
        out = compat.normalize_vendor_response(response)
        self.assertIn("tool_calls", out)
        self.assertEqual(out["tool_calls"][0]["function"]["name"], "do_x")
        self.assertEqual(out["finish_reason"], "tool_calls")

    def test_normalize_vendor_response_anthropic_finish_reason(self):
        response = {
            "result": "ok",
            "reply": "ok",
            "finish_reason": "end_turn"
        }
        out = compat.normalize_vendor_response(response)
        self.assertEqual(out["finish_reason"], "stop")

    def test_normalize_stream_delta_anthropic_finish_reason(self):
        delta = {"finish_reason": "end_turn"}
        out = compat.normalize_stream_delta(delta)
        self.assertEqual(out["finish_reason"], "stop")

    def test_normalize_vendor_response_unknown_finish_reason_fallback_stop(self):
        response = {
            "result": "ok",
            "reply": "ok",
            "finish_reason": "normal"
        }
        out = compat.normalize_vendor_response(response)
        self.assertEqual(out["finish_reason"], "stop")

    def test_normalize_vendor_response_usage_and_tool_calls_shape(self):
        response = {
            "result": "ok",
            "reply": None,
            "tool_calls": {"id": "call_1", "type": "function", "function": {"name": "x", "arguments": "{}"}},
            "usage": {"input_tokens": 10, "output_tokens": 3}
        }
        out = compat.normalize_vendor_response(response)
        self.assertEqual(out["reply"], "")
        self.assertEqual(len(out["tool_calls"]), 1)
        self.assertEqual(out["usage"]["prompt_tokens"], 10)
        self.assertEqual(out["usage"]["completion_tokens"], 3)
        self.assertEqual(out["usage"]["total_tokens"], 13)

    def test_extract_text_from_content(self):
        content = [
            {"type": "text", "text": "hello "},
            {"type": "image_url", "image_url": {"url": "https://example.com/i.png"}},
            {"type": "text", "text": "world"}
        ]
        self.assertEqual(compat.extract_text_from_content(content), "hello world")

    def test_merge_stream_tool_calls_with_index(self):
        cache = {}
        compat.merge_stream_tool_calls(cache, [
            {"index": 1, "id": "call_b", "type": "function", "function": {"name": "b", "arguments": "{\"x\":"}},
            {"index": 0, "id": "call_a", "type": "function", "function": {"name": "a", "arguments": "{\"k\":"}}
        ])
        compat.merge_stream_tool_calls(cache, [
            {"index": 1, "function": {"arguments": "1}"}},
            {"index": 0, "function": {"arguments": "2}"}}
        ])
        result = compat.sorted_stream_tool_calls(cache)
        self.assertEqual([x["id"] for x in result], ["call_a", "call_b"])
        self.assertEqual(result[0]["function"]["arguments"], "{\"k\":2}")
        self.assertEqual(result[1]["function"]["arguments"], "{\"x\":1}")

    def test_normalize_stream_delta_function_call_arguments_only_chunk(self):
        delta = {
            "function_call": {
                "arguments": "{\"city\":\"Shanghai\"}"
            }
        }
        out = compat.normalize_stream_delta(delta)
        self.assertIn("tool_calls", out)
        self.assertEqual(out["tool_calls"][0]["function"]["name"], "")
        self.assertEqual(out["tool_calls"][0]["function"]["arguments"], "{\"city\":\"Shanghai\"}")

    def test_to_legacy_vendor_preset_keeps_multiple_tool_calls(self):
        preset = {
            "messages": [
                {
                    "role": "assistant",
                    "content": "first",
                    "tool_calls": [
                        {"id": "call_1", "type": "function", "function": {"name": "f1", "arguments": "{}"}},
                        {"id": "call_2", "type": "function", "function": {"name": "f2", "arguments": "{}"}}
                    ]
                }
            ]
        }
        out = compat.to_legacy_vendor_preset(preset)
        self.assertEqual(len(out["messages"]), 2)
        self.assertEqual(out["messages"][0]["function_call"]["name"], "f1")
        self.assertEqual(out["messages"][1]["function_call"]["name"], "f2")


if __name__ == "__main__":
    unittest.main()
