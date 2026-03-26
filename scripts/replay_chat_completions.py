#!/usr/bin/env python3
import argparse
import copy
import json
import os
import sys
import urllib.request


def load_cases(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fill_placeholders(obj, model):
    if isinstance(obj, str):
        return obj.replace("${MODEL}", model)
    if isinstance(obj, list):
        return [fill_placeholders(x, model) for x in obj]
    if isinstance(obj, dict):
        return {k: fill_placeholders(v, model) for k, v in obj.items()}
    return obj


def post_json(url, token, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def post_stream(url, token, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {token}")

    events = []
    with urllib.request.urlopen(req, timeout=120) as resp:
        status = resp.status
        for raw in resp:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                events.append({"done": True})
                break
            try:
                events.append(json.loads(data))
            except Exception:
                events.append({"raw": data})
    return status, events


def has_tool_call_non_stream(resp_json):
    try:
        msg = resp_json["choices"][0]["message"]
        tool_calls = msg.get("tool_calls", [])
        return isinstance(tool_calls, list) and len(tool_calls) > 0
    except Exception:
        return False


def count_tool_calls_non_stream(resp_json):
    try:
        msg = resp_json["choices"][0]["message"]
        tool_calls = msg.get("tool_calls", [])
        if isinstance(tool_calls, list):
            return len(tool_calls)
        return 0
    except Exception:
        return 0


def has_tool_call_stream(events):
    for item in events:
        if not isinstance(item, dict):
            continue
        choices = item.get("choices", [])
        if not choices:
            continue
        delta = choices[0].get("delta", {})
        if isinstance(delta, dict):
            tc = delta.get("tool_calls", [])
            if isinstance(tc, list) and len(tc) > 0:
                return True
        fr = choices[0].get("finish_reason")
        if fr == "tool_calls":
            return True
    return False


def count_tool_calls_stream(events):
    seen = set()
    fallback_index = 0
    for item in events:
        if not isinstance(item, dict):
            continue
        choices = item.get("choices", [])
        if not choices:
            continue
        delta = choices[0].get("delta", {})
        if not isinstance(delta, dict):
            continue
        tool_calls = delta.get("tool_calls", [])
        if not isinstance(tool_calls, list):
            continue
        for tc in tool_calls:
            if not isinstance(tc, dict):
                continue
            tc_id = tc.get("id")
            tc_index = tc.get("index")
            if tc_id:
                seen.add(f"id:{tc_id}")
            elif tc_index is not None:
                seen.add(f"index:{tc_index}")
            else:
                seen.add(f"fallback:{fallback_index}")
                fallback_index += 1
    return len(seen)


def stream_finish_reason(events):
    for item in reversed(events):
        if not isinstance(item, dict):
            continue
        choices = item.get("choices", [])
        if not choices:
            continue
        finish_reason = choices[0].get("finish_reason")
        if finish_reason is not None:
            return finish_reason
    return None


def run_case(base_url, token, case, model):
    case = fill_placeholders(copy.deepcopy(case), model)
    name = case["name"]
    payload = case["request"]
    expect = case.get("expect", {})
    stream = bool(expect.get("stream", False))
    expect_tool_call = bool(expect.get("tool_call", False))
    min_tool_calls = int(expect.get("min_tool_calls", 0))
    expect_finish_reason = expect.get("finish_reason")

    url = base_url.rstrip("/") + "/v1/chat/completions"
    print(f"\\n=== CASE: {name} ===")

    if stream:
        status, events = post_stream(url, token, payload)
        print(f"status={status}, events={len(events)}")
        if status != 200:
            return False, f"status={status}"
        if not any(isinstance(x, dict) and x.get("done") for x in events):
            return False, "stream missing [DONE]"
        if expect_tool_call and not has_tool_call_stream(events):
            return False, "expected tool_calls in stream but not found"
        if min_tool_calls > 0:
            stream_tc_count = count_tool_calls_stream(events)
            if stream_tc_count < min_tool_calls:
                return False, f"expected >= {min_tool_calls} tool_calls in stream, got {stream_tc_count}"
        if expect_finish_reason is not None:
            fr = stream_finish_reason(events)
            if fr != expect_finish_reason:
                return False, f"expected finish_reason={expect_finish_reason}, got {fr}"
        return True, "ok"

    status, text = post_json(url, token, payload)
    print(f"status={status}")
    if status != 200:
        return False, f"status={status}, body={text[:400]}"
    try:
        resp = json.loads(text)
    except Exception:
        return False, f"invalid json response: {text[:400]}"

    if expect_tool_call and not has_tool_call_non_stream(resp):
        return False, f"expected tool_calls in response: {text[:500]}"
    if min_tool_calls > 0:
        non_stream_tc_count = count_tool_calls_non_stream(resp)
        if non_stream_tc_count < min_tool_calls:
            return False, f"expected >= {min_tool_calls} tool_calls in response, got {non_stream_tc_count}"
    if expect_finish_reason is not None:
        fr = None
        try:
            fr = resp["choices"][0].get("finish_reason")
        except Exception:
            pass
        if fr != expect_finish_reason:
            return False, f"expected finish_reason={expect_finish_reason}, got {fr}"
    return True, "ok"


def main():
    parser = argparse.ArgumentParser(description="Replay /v1/chat/completions cases (legacy + new protocol)")
    parser.add_argument("--base-url", default=os.getenv("LANYING_CONNECTOR_BASE_URL", "http://127.0.0.1:5000"))
    parser.add_argument("--api-key", default=os.getenv("LANYING_CONNECTOR_API_KEY", ""))
    parser.add_argument("--model", default=os.getenv("LANYING_CONNECTOR_MODEL", "gpt-4o-mini"))
    parser.add_argument("--cases", default="scripts/chat_replay_cases.json")
    parser.add_argument("--only", default="", help="run only one case by name")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    cases = load_cases(args.cases)
    if args.list:
        for c in cases:
            print(c.get("name", ""))
        return 0

    if not args.api_key:
        print("error: missing --api-key or env LANYING_CONNECTOR_API_KEY", file=sys.stderr)
        return 2

    selected = [c for c in cases if (args.only == "" or c.get("name") == args.only)]
    if len(selected) == 0:
        print("error: no cases selected", file=sys.stderr)
        return 2

    ok_count = 0
    fail_count = 0
    for case in selected:
        try:
            ok, msg = run_case(args.base_url, args.api_key, case, args.model)
        except Exception as e:
            ok, msg = False, f"exception: {e}"
        if ok:
            print(f"PASS: {case['name']} ({msg})")
            ok_count += 1
        else:
            print(f"FAIL: {case['name']} ({msg})")
            fail_count += 1

    print(f"\\nSUMMARY: pass={ok_count}, fail={fail_count}, total={len(selected)}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
