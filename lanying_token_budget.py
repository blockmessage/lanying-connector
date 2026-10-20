"""Local input estimates, separate from context-only safety headroom.

Tool schemas are serialized in full: vendor framing is not public/stable, so
these remain estimates, not a replacement for the provider's usage response.
"""
import json
import math

from lanying_openai_compat import functions_to_tools


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def count_functions(functions, encoding):
    # Strip executor metadata, but preserve the complete nested JSON Schema.
    tools = functions_to_tools(functions)
    if not tools:
        return 0
    return len(encoding.encode(_json(tools), disallowed_special=()))


def count_message(message, encoding, image_tokens):
    def count(value):
        text = value if isinstance(value, str) else _json(value)
        return len(encoding.encode(text, disallowed_special=()))

    total = 4
    for key, value in message.items():
        if key == 'content' and isinstance(value, list):
            for part in value:
                if isinstance(part, dict) and part.get('type') == 'image_url':
                    image = part.get('image_url', {})
                    total += image_tokens(image.get('url', ''), image.get('detail', 'auto'))
                elif isinstance(part, dict) and part.get('type', 'text') == 'text':
                    total += count(part.get('text', '')) + 4
                else:
                    total += count(part)
        elif value is not None:
            # tool_calls/function_call are structured data, NOT content parts.
            total += count(value)
        if key == 'name':
            total -= 1
    return total


def input_budget(context_limit, completion_tokens, input_token_limit=None):
    """Reserve 64 framing tokens plus 5% for local estimation uncertainty.

    Only for fitting requests; never add this headroom to recorded/billed usage.
    """
    available = context_limit - completion_tokens
    # Some providers have a separate input ceiling below the context window.
    try:
        limit = int(input_token_limit)
    except (TypeError, ValueError, OverflowError):
        limit = 0
    if not isinstance(input_token_limit, bool) and limit > 0:
        available = min(available, limit)
    return max(0, math.floor((available - 64) / 1.05))


def drop_oldest_turn(messages):
    """Remove an old user turn with all its calls/results, never the latest.

    Keep system/developer instructions even if embedded between turns. If no
    complete old turn can be removed, let the caller report a size error.
    """
    starts = [i for i, message in enumerate(messages) if message.get('role') == 'user']
    if len(starts) < 2:
        return False
    start, end = starts[:2]
    messages[start:end] = [
        message for message in messages[start:end]
        if message.get('role') in ('system', 'developer')
    ]
    return True
