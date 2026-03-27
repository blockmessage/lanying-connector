import copy
import json


def normalize_finish_reason(finish_reason):
    if finish_reason is None:
        return None
    if not isinstance(finish_reason, str):
        return finish_reason
    if finish_reason == "function_call":
        return "tool_calls"
    if finish_reason == "tool_use":
        return "tool_calls"
    if finish_reason == "end_turn":
        return "stop"
    if finish_reason == "max_tokens":
        return "length"
    if finish_reason == "stop_sequence":
        return "stop"
    if finish_reason in ["stop", "length", "tool_calls", "content_filter"]:
        return finish_reason
    if finish_reason == "":
        return ""
    return "stop"


def _safe_json_dumps(value):
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _ensure_content(value):
    if isinstance(value, list):
        return value
    if value is None:
        return ""
    return str(value)


def tools_to_functions(tools):
    functions = []
    if not isinstance(tools, list):
        return functions
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") != "function":
            continue
        function = tool.get("function", {})
        if not isinstance(function, dict):
            continue
        item = {}
        for key in ["name", "description", "parameters"]:
            if key in function:
                item[key] = function[key]
        if item.get("name"):
            functions.append(item)
    return functions


def functions_to_tools(functions):
    tools = []
    if not isinstance(functions, list):
        return tools
    for function in functions:
        if not isinstance(function, dict):
            continue
        item = {"type": "function", "function": {}}
        for key in ["name", "description", "parameters"]:
            if key in function:
                item["function"][key] = function[key]
        if item["function"].get("name"):
            tools.append(item)
    return tools


def function_call_to_tool_calls(function_call, allow_empty_name=False):
    if not isinstance(function_call, dict):
        return []
    name = function_call.get("name", "")
    if name == "" and not allow_empty_name:
        return []
    arguments = function_call.get("arguments", "{}")
    if not isinstance(arguments, str):
        arguments = _safe_json_dumps(arguments)
    tool_call_id = function_call.get("id", "")
    return [{
        "id": tool_call_id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments
        }
    }]


def tool_calls_to_function_call(tool_calls):
    if not isinstance(tool_calls, list) or len(tool_calls) == 0:
        return None
    first = tool_calls[0]
    if not isinstance(first, dict):
        return None
    function = first.get("function", {})
    if not isinstance(function, dict):
        return None
    name = function.get("name", "")
    if name == "":
        return None
    arguments = function.get("arguments", "{}")
    if not isinstance(arguments, str):
        arguments = _safe_json_dumps(arguments)
    ret = {
        "name": name,
        "arguments": arguments
    }
    if "id" in first and first.get("id") is not None:
        ret["id"] = first.get("id")
    return ret


def function_call_to_tool_choice(function_call):
    if function_call is None:
        return None
    if isinstance(function_call, str):
        if function_call in ["none", "auto", "required"]:
            return function_call
        return "auto"
    if isinstance(function_call, dict):
        name = function_call.get("name", "")
        if name:
            return {
                "type": "function",
                "function": {
                    "name": name
                }
            }
    return None


def tool_choice_to_function_call(tool_choice):
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        if tool_choice in ["none", "auto"]:
            return tool_choice
        if tool_choice == "required":
            return "auto"
        return "auto"
    if isinstance(tool_choice, dict):
        function = tool_choice.get("function", {})
        if isinstance(function, dict) and function.get("name"):
            return {"name": function.get("name")}
    return None


def normalize_chat_message(message, last_tool_call_id=""):
    if not isinstance(message, dict):
        return None, last_tool_call_id
    role = message.get("role", "")
    if role == "":
        return None, last_tool_call_id
    new_message = copy.deepcopy(message)
    new_message["role"] = role
    new_message["content"] = _ensure_content(new_message.get("content", ""))

    if role == "assistant":
        if "tool_calls" not in new_message and "function_call" in new_message:
            tool_calls = function_call_to_tool_calls(new_message.get("function_call"))
            if len(tool_calls) > 0:
                new_message["tool_calls"] = tool_calls
        if "tool_calls" in new_message and isinstance(new_message["tool_calls"], list):
            for tool_call in new_message["tool_calls"]:
                if isinstance(tool_call, dict) and tool_call.get("id"):
                    last_tool_call_id = tool_call.get("id")
        if "function_call" in new_message:
            del new_message["function_call"]
    elif role == "function":
        new_message["role"] = "tool"
        if not new_message.get("tool_call_id"):
            new_message["tool_call_id"] = last_tool_call_id
    elif role == "tool":
        if not new_message.get("tool_call_id"):
            new_message["tool_call_id"] = last_tool_call_id

    return new_message, last_tool_call_id


def normalize_chat_preset(preset):
    if not isinstance(preset, dict):
        return preset
    new_preset = copy.deepcopy(preset)

    if "tools" not in new_preset and "functions" in new_preset:
        new_preset["tools"] = functions_to_tools(new_preset.get("functions"))
    if "tool_choice" not in new_preset and "function_call" in new_preset:
        tc = function_call_to_tool_choice(new_preset.get("function_call"))
        if tc is not None:
            new_preset["tool_choice"] = tc

    last_tool_call_id = ""
    messages = []
    for message in new_preset.get("messages", []):
        item, last_tool_call_id = normalize_chat_message(message, last_tool_call_id)
        if item is not None:
            messages.append(item)
    if len(messages) > 0 or "messages" in new_preset:
        new_preset["messages"] = messages

    return new_preset


def to_legacy_vendor_preset(preset):
    if not isinstance(preset, dict):
        return preset
    new_preset = copy.deepcopy(preset)

    if "functions" not in new_preset and "tools" in new_preset:
        functions = tools_to_functions(new_preset.get("tools"))
        if len(functions) > 0:
            new_preset["functions"] = functions
    if "function_call" not in new_preset and "tool_choice" in new_preset:
        fc = tool_choice_to_function_call(new_preset.get("tool_choice"))
        if fc is not None:
            new_preset["function_call"] = fc

    id_to_name = {}
    messages = []
    for message in new_preset.get("messages", []):
        if not isinstance(message, dict):
            continue
        role = message.get("role", "")
        content = _ensure_content(message.get("content", ""))
        if role == "assistant" and isinstance(message.get("tool_calls"), list) and len(message.get("tool_calls")) > 0:
            appended = False
            tool_calls = message.get("tool_calls")
            for idx, tool_call in enumerate(tool_calls):
                function_call = tool_calls_to_function_call([tool_call])
                legacy_msg = {
                    "role": "assistant",
                    "content": content if idx == 0 else ""
                }
                if function_call is not None:
                    legacy_msg["function_call"] = function_call
                    if tool_call.get("id"):
                        id_to_name[tool_call.get("id")] = function_call.get("name", "")
                messages.append(legacy_msg)
                appended = True
            if not appended:
                messages.append({
                    "role": "assistant",
                    "content": content
                })
        elif role == "tool":
            tool_call_id = message.get("tool_call_id", "")
            legacy_msg = {
                "role": "function",
                "content": content
            }
            if tool_call_id and id_to_name.get(tool_call_id):
                legacy_msg["name"] = id_to_name.get(tool_call_id)
            elif message.get("name"):
                legacy_msg["name"] = message.get("name")
            messages.append(legacy_msg)
        else:
            legacy_msg = copy.deepcopy(message)
            legacy_msg["content"] = content
            messages.append(legacy_msg)

    if len(messages) > 0 or "messages" in new_preset:
        new_preset["messages"] = messages

    return new_preset


def normalize_vendor_response(response):
    if not isinstance(response, dict):
        return response
    new_resp = response
    if "tool_calls" not in new_resp and "function_call" in new_resp and new_resp.get("function_call") is not None:
        new_resp["tool_calls"] = function_call_to_tool_calls(new_resp.get("function_call"))
    new_resp["finish_reason"] = normalize_finish_reason(new_resp.get("finish_reason"))
    return new_resp


def normalize_stream_delta(delta):
    if not isinstance(delta, dict):
        return delta
    new_delta = delta
    if "tool_calls" not in new_delta and "function_call" in new_delta:
        tool_calls = function_call_to_tool_calls(new_delta.get("function_call"), allow_empty_name=True)
        if len(tool_calls) > 0:
            new_delta["tool_calls"] = tool_calls
    new_delta["finish_reason"] = normalize_finish_reason(new_delta.get("finish_reason"))
    return new_delta


def merge_stream_tool_calls(cache, delta_tool_calls, arguments_merge_type="append"):
    if not isinstance(delta_tool_calls, list):
        return
    for idx, tool_call in enumerate(delta_tool_calls):
        if not isinstance(tool_call, dict):
            continue
        tool_index = tool_call.get("index", idx)
        if tool_index not in cache:
            cache[tool_index] = {
                "id": "",
                "type": "function",
                "function": {
                    "name": "",
                    "arguments": ""
                }
            }
        cache_item = cache[tool_index]
        if tool_call.get("id"):
            cache_item["id"] = tool_call.get("id")
        if tool_call.get("type"):
            cache_item["type"] = tool_call.get("type")
        function = tool_call.get("function", {})
        if isinstance(function, dict):
            if function.get("name"):
                cache_item["function"]["name"] = function.get("name")
            if "arguments" in function and function.get("arguments") is not None:
                if arguments_merge_type == "replace":
                    cache_item["function"]["arguments"] = str(function.get("arguments"))
                else:
                    cache_item["function"]["arguments"] += str(function.get("arguments"))


def sorted_stream_tool_calls(cache):
    if not isinstance(cache, dict):
        return []
    result = []
    for idx in sorted(cache.keys()):
        item = copy.deepcopy(cache[idx])
        if not item.get("id"):
            item["id"] = f"call_{idx}"
        result.append(item)
    return result


def extract_text_from_content(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                texts.append(str(item.get("text", "")))
        return "".join(texts)
    if content is None:
        return ""
    return str(content)


def get_tools_as_functions(preset):
    if not isinstance(preset, dict):
        return []
    if isinstance(preset.get("functions"), list):
        return preset.get("functions")
    return tools_to_functions(preset.get("tools", []))
