#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import lanying_vendor_openai


def is_modern_reasoning_model(model):
    model = str(model or "")
    if model.startswith("o1"):
        return True
    if model.startswith("o3"):
        return True
    if model.startswith("o4-mini"):
        return True
    return model.startswith("gpt-5")


def list_openai_chat_models(include_hidden):
    models = []
    for config in lanying_vendor_openai.model_configs():
        if config.get("type") != "chat":
            continue
        if not include_hidden and config.get("hidden"):
            continue
        models.append(config)
    return models


def build_payload(model, prompt, stream):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
    }
    if is_modern_reasoning_model(model):
        payload["max_completion_tokens"] = 64
    else:
        payload["max_tokens"] = 64
    return payload


def post_json(base_url, token, vendor, payload, timeout):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + "/v1/chat/completions",
        data=body,
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("vendor", vendor)
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed_ms = round((time.time() - started) * 1000)
            text = resp.read().decode("utf-8", errors="replace")
            return {
                "http_status": resp.status,
                "elapsed_ms": elapsed_ms,
                "body_text": text,
                "body_json": safe_json_loads(text),
                "ok": resp.status == 200,
            }
    except urllib.error.HTTPError as exc:
        elapsed_ms = round((time.time() - started) * 1000)
        text = exc.read().decode("utf-8", errors="replace")
        return {
            "http_status": exc.code,
            "elapsed_ms": elapsed_ms,
            "body_text": text,
            "body_json": safe_json_loads(text),
            "ok": False,
        }
    except Exception as exc:
        elapsed_ms = round((time.time() - started) * 1000)
        return {
            "http_status": None,
            "elapsed_ms": elapsed_ms,
            "body_text": str(exc),
            "body_json": None,
            "ok": False,
        }


def post_stream(base_url, token, vendor, payload, timeout):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + "/v1/chat/completions",
        data=body,
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("vendor", vendor)
    started = time.time()
    try:
        usage = None
        finish_reason = None
        text_parts = []
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            for raw in resp:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                data_text = line[5:].strip()
                if data_text == "[DONE]":
                    break
                data = safe_json_loads(data_text)
                if not isinstance(data, dict):
                    continue
                choices = data.get("choices", [])
                if choices:
                    choice = choices[0]
                    delta = choice.get("delta", {})
                    if isinstance(delta, dict):
                        content = delta.get("content")
                        if isinstance(content, str):
                            text_parts.append(content)
                    if choice.get("finish_reason") is not None:
                        finish_reason = choice.get("finish_reason")
                if isinstance(data.get("usage"), dict):
                    usage = data.get("usage")
            elapsed_ms = round((time.time() - started) * 1000)
            return {
                "http_status": resp.status,
                "elapsed_ms": elapsed_ms,
                "body_text": "".join(text_parts),
                "body_json": {
                    "choices": [{"finish_reason": finish_reason}],
                    "usage": usage or {},
                },
                "ok": resp.status == 200,
            }
    except urllib.error.HTTPError as exc:
        elapsed_ms = round((time.time() - started) * 1000)
        text = exc.read().decode("utf-8", errors="replace")
        return {
            "http_status": exc.code,
            "elapsed_ms": elapsed_ms,
            "body_text": text,
            "body_json": safe_json_loads(text),
            "ok": False,
        }
    except Exception as exc:
        elapsed_ms = round((time.time() - started) * 1000)
        return {
            "http_status": None,
            "elapsed_ms": elapsed_ms,
            "body_text": str(exc),
            "body_json": None,
            "ok": False,
        }


def safe_json_loads(text):
    try:
        return json.loads(text)
    except Exception:
        return None


def probe_model(base_url, token, vendor, model_config, prompt, timeout, stream):
    model = model_config.get("model", "")
    payload = build_payload(model, prompt, stream)
    if stream:
        result = post_stream(base_url, token, vendor, payload, timeout)
    else:
        result = post_json(base_url, token, vendor, payload, timeout)

    body_json = result.get("body_json")
    usage = {}
    finish_reason = None
    reply_preview = ""
    error_code = ""
    error_message = ""

    if isinstance(body_json, dict):
        usage = body_json.get("usage", {}) or {}
        try:
            finish_reason = body_json.get("choices", [{}])[0].get("finish_reason")
        except Exception:
            finish_reason = None
        try:
            reply_preview = str(body_json.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        except Exception:
            reply_preview = ""
        error = body_json.get("error", {})
        if isinstance(error, dict):
            error_code = str(error.get("code", "") or "")
            error_message = str(error.get("message", "") or "")
    if stream and not reply_preview:
        reply_preview = str(result.get("body_text", "") or "")

    ok = bool(result.get("ok"))
    return {
        "model": model,
        "is_default": bool(model_config.get("is_default", False)),
        "http_status": result.get("http_status"),
        "ok": ok,
        "elapsed_ms": result.get("elapsed_ms"),
        "finish_reason": finish_reason,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "error_code": error_code,
        "error_message": error_message,
        "reply_preview": (reply_preview or "")[:120],
        "payload": payload,
        "raw_body": result.get("body_json") if body_json is not None else result.get("body_text"),
    }


def print_summary_table(results):
    headers = ["MODEL", "DEF", "OK", "HTTP", "FINISH", "TOKENS", "MS", "ERROR"]
    rows = []
    for item in results:
        tokens = ""
        if item.get("total_tokens") is not None:
            tokens = str(item.get("total_tokens"))
        rows.append([
            item["model"],
            "Y" if item["is_default"] else "",
            "Y" if item["ok"] else "N",
            "" if item["http_status"] is None else str(item["http_status"]),
            "" if item["finish_reason"] is None else str(item["finish_reason"]),
            tokens,
            "" if item["elapsed_ms"] is None else str(item["elapsed_ms"]),
            item["error_code"] or item["error_message"][:60],
        ])
    widths = [len(h) for h in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    fmt = "  ".join("{:<" + str(width) + "}" for width in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * width for width in widths]))
    for row in rows:
        print(fmt.format(*row))


def ensure_parent_dir(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Call lanying-connector /v1/chat/completions for all OpenAI chat models and build a report.")
    parser.add_argument("--base-url", default=os.getenv("LANYING_CONNECTOR_BASE_URL", "http://127.0.0.1:5000"))
    parser.add_argument("--api-key", default=os.getenv("LANYING_CONNECTOR_API_KEY", ""))
    parser.add_argument("--vendor", default=os.getenv("LANYING_CONNECTOR_VENDOR", "openai"))
    parser.add_argument("--prompt", default=os.getenv("LANYING_CONNECTOR_TEST_PROMPT", "你好，请回复ok。"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("LANYING_CONNECTOR_TEST_TIMEOUT", "120")))
    parser.add_argument("--stream", action="store_true", help="Use stream mode when probing models.")
    parser.add_argument("--include-hidden", action="store_true", help="Include hidden chat models.")
    parser.add_argument("--only", default="", help="Only test one model.")
    parser.add_argument("--output-prefix", default="", help="Output path prefix, without extension.")
    args = parser.parse_args()

    if not args.api_key:
        print("error: missing --api-key or env LANYING_CONNECTOR_API_KEY", file=sys.stderr)
        return 2

    models = list_openai_chat_models(args.include_hidden)
    if args.only:
        models = [item for item in models if item.get("model") == args.only]
    if not models:
        print("error: no models selected", file=sys.stderr)
        return 2

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output_prefix:
        output_prefix = Path(args.output_prefix)
    else:
        output_prefix = REPO_ROOT / "scripts" / "reports" / f"openai_model_probe_{timestamp}"

    results = []
    for model_config in models:
        model = model_config.get("model", "")
        print(f"[probe] {model}")
        try:
            result = probe_model(
                args.base_url,
                args.api_key,
                args.vendor,
                model_config,
                args.prompt,
                args.timeout,
                args.stream,
            )
        except Exception as exc:
            result = {
                "model": model,
                "is_default": bool(model_config.get("is_default", False)),
                "http_status": None,
                "ok": False,
                "elapsed_ms": None,
                "finish_reason": None,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "error_code": "",
                "error_message": str(exc),
                "reply_preview": "",
                "payload": build_payload(model, args.prompt, args.stream),
                "raw_body": str(exc),
            }
        results.append(result)

    print("")
    print_summary_table(results)

    meta = {
        "timestamp": timestamp,
        "base_url": args.base_url,
        "vendor": args.vendor,
        "stream": args.stream,
        "prompt": args.prompt,
    }
    json_output = {
        "meta": meta,
        "results": results,
    }

    json_path = output_prefix.with_suffix(".json")
    ensure_parent_dir(json_path)
    json_path.write_text(json.dumps(json_output, ensure_ascii=False, indent=2), encoding="utf-8")

    print("")
    print(f"json report: {json_path}")

    fail_count = sum(1 for item in results if not item["ok"])
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
