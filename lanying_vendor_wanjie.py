import copy
import json
import logging
import requests
import tiktoken

import lanying_vendor_openai

SUPPORT_NATIVE_TOOLS = True
WANJIE_API_BASE = 'https://maas-openapi.wanjiedata.com/api/v1'


def _chat_model_config(model, service, quota, token_limit, max_output_tokens, **kwargs):
    config = {
        "model": model,
        "is_origin_vendor": True,
        "service": service,
        "type": "chat",
        "is_prefix": False,
        "quota": quota,
        "token_limit": token_limit,
        "max_output_tokens": max_output_tokens,
        "function_call": True,
    }
    config.update(kwargs)
    return config


def _priced_chat_model_config(model, service, quota, input_price, output_price, token_limit, max_output_tokens, currency='CNY', **kwargs):
    config = _chat_model_config(model, service, quota, token_limit, max_output_tokens, **kwargs)
    config['input_price'] = float(input_price)
    config['output_price'] = float(output_price)
    config['currency'] = str(currency or 'CNY').upper()
    return config


def model_configs():
    return copy.deepcopy([
        _priced_chat_model_config('claude-3.7-sonnet-20250219', 'claude', 3.52, 0.011123, 0.055614, 200000, 8192, order=1),
        _priced_chat_model_config('claude-haiku-4-5-20251001', 'claude', 1.32, 0.003708, 0.018538, 200000, 8192, order=2),
        _priced_chat_model_config('claude-opus-4-1-20250805', 'claude', 16.70, 0.055614, 0.278070, 200000, 32000, order=3),
        _priced_chat_model_config('claude-opus-4-20250514', 'claude', 16.70, 0.055614, 0.278070, 200000, 32000, order=4),
        _priced_chat_model_config('claude-opus-4-5-20251101', 'claude', 5.71, 0.018538, 0.092690, 200000, 8192, order=5),
        _priced_chat_model_config('claude-opus-4-6', 'claude', 5.71, 0.018538, 0.092690, 200000, 8192, order=6),
        _priced_chat_model_config('claude-sonnet-4-20250514', 'claude', 6.56, 0.021390, 0.106950, 200000, 64000, order=7),
        _priced_chat_model_config('claude-sonnet-4-5-20250929', 'claude', 6.56, 0.021390, 0.106950, 200000, 64000, order=8),
        _priced_chat_model_config('claude-sonnet-4-6', 'claude', 3.52, 0.011123, 0.055614, 200000, 64000, order=9),
        _priced_chat_model_config('GPT-4.1', 'chatgpt', 2.00, 0.006845, 0.027379, 1047576, 32768, order=10),
        _priced_chat_model_config('GPT-4o', 'chatgpt', 2.44, 0.008556, 0.034224, 128000, 16384, order=11),
        _priced_chat_model_config('gpt-4o-2024-08-06', 'chatgpt', 2.44, 0.008556, 0.034224, 128000, 16384, order=12),
        _priced_chat_model_config('gpt-4o-2024-11-20', 'chatgpt', 2.44, 0.008556, 0.034224, 128000, 16384, order=13),
        _priced_chat_model_config('gpt-4o-mini-2024-07-18', 'chatgpt', 0.36, 0.000513, 0.002053, 128000, 16384, order=14),
        _priced_chat_model_config('gpt-5', 'chatgpt', 1.97, 0.004278, 0.034224, 400000, 128000, order=15),
        _priced_chat_model_config('gpt-5-codex', 'chatgpt', 1.97, 0.004278, 0.034224, 400000, 128000, order=16),
        _priced_chat_model_config('gpt-5.1', 'chatgpt', 1.97, 0.004278, 0.034224, 400000, 128000, order=17),
        _priced_chat_model_config('gpt-5.1-codex', 'chatgpt', 1.97, 0.004278, 0.034224, 400000, 128000, order=18),
        _priced_chat_model_config('gpt-5.2', 'chatgpt', 2.66, 0.005989, 0.047914, 400000, 128000, order=19),
        _priced_chat_model_config('gpt-5.2-codex', 'chatgpt', 2.66, 0.005989, 0.047914, 400000, 128000, order=20),
        _priced_chat_model_config('gpt-5.3-codex', 'chatgpt', 2.66, 0.005989, 0.047914, 400000, 128000, order=21),
        _priced_chat_model_config('gpt-5.4', 'chatgpt', 3.07, 0.008556, 0.051336, 400000, 128000, order=22),
        _priced_chat_model_config('kimi-k2', 'kimi', 1.10, 0.003400, 0.013600, 256000, 8192, order=23),
        _priced_chat_model_config('kimi-k2-5-260127', 'kimi', 1.44, 0.004000, 0.021000, 256000, 8192, order=24),
        _priced_chat_model_config('mimo-v2-flash', 'xiaomi', 0.38, 0.000700, 0.002100, 256000, 8192, order=25),
        _priced_chat_model_config('mimo-v2-pro', 'xiaomi', 1.78, 0.007000, 0.021000, 256000, 8192, order=26),
    ])


def prepare_chat(auth_info, preset):
    prepare_info = lanying_vendor_openai.prepare_chat(auth_info, preset)
    prepare_info['api_endpoint'] = WANJIE_API_BASE
    prepare_info['api_endpoint_server_location'] = 'domestic'
    prepare_info['auth_info'] = dict(auth_info or {})
    prepare_info['auth_info']['vendor_type'] = 'wanjie'
    return prepare_info


def chat(prepare_info, preset, model_config):
    url = WANJIE_API_BASE + '/chat/completions'
    final_preset = lanying_vendor_openai.format_preset(preset, model_config)
    api_key = prepare_info["api_key"]
    headers = {"Content-Type": "application/json", "Authorization": f'Bearer {api_key}'}
    try:
        logging.info(f"wanjie chat_completion start | preset={preset}, url:{url}")
        logging.info(f"wanjie chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        request_timeout = lanying_vendor_openai.get_request_timeout(prepare_info)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True, timeout=request_timeout)
            logging.info(f"wanjie chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    got_stream_usage = False
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                delta = None
                                if 'choices' in data and len(data['choices']) > 0:
                                    choice = data['choices'][0]
                                    delta = choice['delta']
                                    if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                        delta['finish_reason'] = choice['finish_reason']
                                elif 'usage' in data and isinstance(data['usage'], dict):
                                    got_stream_usage = True
                                    delta = {'usage': data['usage']}
                                if delta:
                                    yield delta
                            except Exception:
                                pass
                    if got_stream_usage:
                        logging.info(f"wanjie stream usage chunk received | model:{final_preset.get('model', '')}")
                return {
                    'result': 'ok',
                    'reply': '',
                    'reply_generator': generator(),
                    'usage': {
                        'completion_tokens': 0,
                        'prompt_tokens': 0,
                        'total_tokens': 0
                    }
                }
            response_json = {}
            try:
                response_json = response.json()
            except Exception:
                pass
            return {
                'result': 'error',
                'reason': 'bad_status_code',
                'status_code': response.status_code,
                'response': response_json
            }
        response = requests.request("POST", url, headers=headers, json=final_preset, timeout=request_timeout)
        logging.info(f"wanjie chat_completion finish | code={response.status_code}, response={response.text}")
        if response.status_code != 200:
            response_json = {}
            try:
                response_json = response.json()
            except Exception:
                pass
            return {
                'result': 'error',
                'reason': 'bad_status_code',
                'status_code': response.status_code,
                'response': response_json
            }
        res = response.json()
        usage = res.get('usage', {})
        response_message = res['choices'][0]['message']
        reply = response_message.get('content', "")
        if reply:
            reply = reply.strip()
        else:
            reply = ''
        tool_calls = response_message.get('tool_calls', [])
        finish_reason = ''
        try:
            finish_reason = res['choices'][0]['finish_reason']
        except Exception:
            pass
        return {
            'result': 'ok',
            'reply': reply,
            'finish_reason': finish_reason,
            'tool_calls': tool_calls,
            'usage': {
                'completion_tokens': usage.get('completion_tokens', 0),
                'prompt_tokens': usage.get('prompt_tokens', 0),
                'total_tokens': usage.get('total_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'reason': 'exception'
        }


def format_preset(preset, model_config):
    return lanying_vendor_openai.format_preset(preset, model_config)


def list_remote_models(auth_info):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {str(auth_info.get('api_key', '') or '').strip()}"
    }
    request_timeout = None
    try:
        timeout_seconds = int(auth_info.get('validation_timeout_seconds', 0))
        if timeout_seconds > 0:
            request_timeout = timeout_seconds
    except Exception:
        pass
    response = requests.request("GET", WANJIE_API_BASE + '/models', headers=headers, timeout=request_timeout)
    response_data = response.text
    try:
        response_data = response.json()
    except Exception:
        pass
    return {
        'result': 'ok',
        'status_code': response.status_code,
        'response': response_data
    }


def encoding_for_model(model):
    try:
        return lanying_vendor_openai.encoding_for_model(model)
    except Exception:
        return tiktoken.get_encoding("cl100k_base")
