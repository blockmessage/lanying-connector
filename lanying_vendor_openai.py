import logging
import tiktoken
import os
import types
import time
import json
import requests
from urllib.parse import urlparse
import lanying_openai_compat
import lanying_utils

SUPPORT_NATIVE_TOOLS = True
PROXY_API_ENDPOINT_HEADER = 'X-Lanying-Proxy-Api-Endpoint'


def get_request_timeout(prepare_info):
    auth_info = prepare_info.get('auth_info', {}) or {}
    try:
        timeout_seconds = int(auth_info.get('validation_timeout_seconds', 0))
        if timeout_seconds > 0:
            return timeout_seconds
    except Exception:
        pass
    return None

def model_configs():
    return [
        {
            "model": 'gpt-4o-mini',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.49,
            "token_limit": 128000,
            "support_vision": False,
            'order': 1,
            'function_call': True,
            'max_output_tokens': 16384
        },
        {
            "model": 'gpt-3.5-turbo',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.00,
            "token_limit": 16385,
            'order': 2,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4o',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 8.00,
            "token_limit": 128000,
            "support_vision": False,
            'order': 3,
            'function_call': True,
            'max_output_tokens': 16384
        },
        # {
        #     "model": 'o3-mini',
        #     "is_origin_vendor": True,
        #     "service": 'chatgpt',
        #     "type": "chat",
        #     "is_prefix": False,
        #     "quota": 2.22,
        #     "token_limit": 200000,
        #     "support_vision": False,
        #     'order': 3,
        #     'function_call': False,
        #     'support_stream': False,
        #     'support_system_role': False,
        #     'max_output_tokens': 100000
        # },
        {
            "model": 'o1-mini',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 2.22,
            "token_limit": 128000,
            "support_vision": False,
            'order': 3,
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 65536
        },
        # {
        #     "model": 'o1',
        #     "is_origin_vendor": True,
        #     "service": 'chatgpt',
        #     "type": "chat",
        #     "is_prefix": False,
        #     "quota": 27.44,
        #     "token_limit": 200000,
        #     "support_vision": False,
        #     'order': 4,
        #     'function_call': False,
        #     'support_stream': False,
        #     'support_system_role': False,
        #     'max_output_tokens': 100000
        # },
        {
            "model": 'o1-mini-2024-09-12',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 2.22,
            "token_limit": 128000,
            "support_vision": False,
            'order': 103,
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 65536
        },
        {
            "model": 'o1-preview',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 27.44,
            "token_limit": 128000,
            "support_vision": False,
            'order': 4,
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 32768
        },
        {
            "model": 'o1-preview-2024-09-12',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 27.44,
            "token_limit": 128000,
            "support_vision": False,
            'order': 104,
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 32768
        },
        {
            "model": 'gpt-4o-mini-audio-preview',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 13.19,
            "token_limit": 128000,
            "support_vision": False,
            "support_audio": True,
            'order': 4,
            'function_call': True,
            'max_output_tokens': 16384
        },
        {
            "model": 'gpt-4-turbo',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 15.78,
            "token_limit": 128000,
            "support_vision": True,
            'order': 4,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 39.11,
            "token_limit": 8192,
            'order': 5,
            'function_call': True,
            'max_output_tokens': 8192
        },
        {
            "model": 'gpt-3.5-turbo-0125',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.00,
            "token_limit": 16385,
            'order': 6,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-3.5-turbo-1106',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.52,
            "token_limit": 16385,
            'order': 7,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4o-2024-05-13',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 8.00,
            "token_limit": 128000,
            "support_vision": False,
            'order': 8,
            'function_call': True,
            'max_output_tokens': 16384
        },
        {
            "model": 'gpt-4-turbo-2024-04-09',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 15.78,
            "token_limit": 128000,
            "support_vision": True,
            'order': 9,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4-1106-preview',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 15.78,
            "token_limit": 128000,
            'order': 10,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4-0125-preview',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 15.78,
            "token_limit": 128000,
            'order': 11,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4o-mini-2024-07-18',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.49,
            "token_limit": 128000,
            "support_vision": False,
            'order': 12,
            'function_call': True,
            'max_output_tokens': 16384
        },
        {
            "model": 'gpt-3.5-turbo-16k',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 3.59,
            "token_limit": 16385,
            'order': 2,
            'hidden': True,
            'function_call': True,
            'max_output_tokens': 4096
        },
        {
            "model": 'gpt-4-32k',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 78.00,
            "token_limit": 32768,
            'order': 7,
            'hidden': True,
            'function_call': True,
            'max_output_tokens': 8192
        },
        {
            "model": 'text-embedding-ada-002',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.13,
            "token_limit": 8191,
            'order': 1000,
            'dim': 1536,
            'dim_origin': 1536
        },
        {
            "model": 'text-embedding-3-small',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.03,
            "token_limit": 8191,
            'order': 1001,
            'dim': 1536,
            'dim_origin': 1536
        },
        {
            "model": 'text-embedding-3-large',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.17,
            "token_limit": 8191,
            'order': 1002,
            'dim': 1536,
            'dim_origin': 3072
        },
        {
            "model": 'dall-e-3',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "image",
            "is_prefix": False,
            "quota": 100,
            "image_quota":{
                "standard_1024x1024": 53.33,
                "standard_1024x1792": 106.67,
                "standard_1792x1024": 106.67,
                "hd_1024x1024": 106.67,
                "hd_1024x1792": 160.00,
                "hd_1792x1024": 160.00
            },
            "token_limit": 16000,
            'order': 10,
            'hidden': True
        },
        {
            "model": 'dall-e-2',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "image",
            "is_prefix": False,
            "quota": 100,
            "image_quota":{
                "standard_1024x1024": 15,
                "standard_512x512": 14,
                "standard_256x256": 12
            },
            "token_limit": 16000,
            'order': 10,
            'hidden': True
        },
        {
            "model": 'whisper-1',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "speech_to_text",
            "is_prefix": False,
            "quota": 1,
            "quota_count_type": "audio_duration_second",
            "quota_count_value": 10,
            "token_limit": 16000,
            'order': 10,
            'hidden': True
        },
        {
            "model": 'tts-1',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "text_to_speech",
            "is_prefix": False,
            "quota": 1.5,
            "quota_count_type": "chat_count",
            "quota_count_value": 100,
            "token_limit": 16000,
            'order': 10,
            'hidden': True
        },
        {
            "model": 'tts-1-hd',
            "is_origin_vendor": True,
            "service": 'chatgpt',
            "type": "text_to_speech",
            "is_prefix": False,
            "quota": 3,
            "quota_count_type": "chat_count",
            "quota_count_value": 100,
            "token_limit": 16000,
            'order': 10,
            'hidden': True
        }
    ]

def prepare_chat(auth_info, preset):
    if 'messages' in preset:
        messages = []
        for message in preset['messages']:
            if 'role' in message and 'content' in message:
                msg = {}
                for k,v in message.items():
                    if k in ['role', 'content', 'name', 'function_call', 'tool_calls', 'tool_call_id']:
                        msg[k] = v
                messages.append(msg)
        preset['messages'] = messages
    return {
        'api_key' : auth_info['api_key'],
        'api_endpoint': auth_info.get('api_endpoint', ''),
        'api_endpoint_server_location': auth_info.get('api_endpoint_server_location', 'overseas')
    }

def chat(prepare_info, preset, model_config):
    api_base, headers = get_api_base_and_headers(prepare_info)
    url = api_base + '/chat/completions'
    final_preset = format_preset(preset, model_config)
    logging.info(f"vendor openai chat request: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
    try:
        stream = final_preset.get("stream", False)
        request_timeout = get_request_timeout(prepare_info)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True, timeout=request_timeout)
            logging.info(f"openai chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        # logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                delta = None
                                if 'choices' in data and len(data['choices']) > 0:
                                    choice = data['choices'][0]
                                    delta = choice['delta']
                                    if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                        delta['finish_reason'] = choice['finish_reason']
                                else:
                                    if 'usage' in data and isinstance(data['usage'], dict):
                                        delta = {
                                            'usage' : data['usage']
                                        }
                                if delta:
                                    # logging.info(f"yield delta:{delta}")
                                    yield delta
                            except Exception as e:
                                pass
                return {
                    'result': 'ok',
                    'reply' : '',
                    'reply_generator': generator(),
                    'usage' : {
                        'completion_tokens': 0,
                        'prompt_tokens': 0,
                        'total_tokens': 0
                    }
                }
            else:
                logging.info(f"fail to get stream: response:{response.text}")
                response_json = {}
                try:
                    response_json = response.json()
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'status_code': response.status_code,
                    'response': response_json
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset, timeout=request_timeout)
            logging.info(f"openai chat_completion finish | code={response.status_code}, response={response.text}")
            if response.status_code == 200:
                res = response.json()
                usage = res.get('usage',{})
                response_message = res['choices'][0]['message']
                reply = response_message.get('content', "")
                if reply:
                    reply = reply.strip()
                else:
                    reply = ''
                if 'audio' in response_message and isinstance(response_message['audio'], dict):
                    audio = response_message['audio']
                    if 'transcript' in audio:
                        reply = audio['transcript']
                else:
                    audio = None
                tool_calls = response_message.get('tool_calls', [])
                finish_reason = ''
                try:
                    finish_reason = res['choices'][0]['finish_reason']
                except Exception as e:
                    pass
                return {
                    'result': 'ok',
                    'reply' : reply,
                    'audio': audio,
                    'finish_reason': finish_reason,
                    'tool_calls': tool_calls,
                    'usage' : {
                        'completion_tokens' : usage.get('completion_tokens',0),
                        'prompt_tokens' : usage.get('prompt_tokens', 0),
                        'total_tokens' : usage.get('total_tokens', 0)
                    }
                }
            else:
                response_json = {}
                try:
                    response_json = response.json()
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'status_code': response.status_code,
                    'response': response_json
                }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'reason': 'exception'
        }

def prepare_embedding(auth_info, _):
    return {
        'api_key' : auth_info['api_key'],
        'api_endpoint': auth_info.get('api_endpoint', '')
    }

def embedding(prepare_info, model, text, model_config):
    api_base, headers = get_api_base_and_headers(prepare_info)
    url = api_base + '/embeddings'
    type = prepare_info.get('type', '')
    if model == '':
        model = 'text-embedding-ada-002'
    json_body = {"input":text, "model":model}
    if model == 'text-embedding-3-large':
        json_body['dimensions'] = 1536
    try:
        logging.info(f"openai embedding start | type={type}")
        response = requests.request("POST", url, headers=headers, json=json_body)
        logging.info(f"openai embedding finish: response:{response}")
        res = response.json()
        if 'data' not in res:
            logging.info(f"openai embedding finish with error:{res}")
        #logging.info(f"openai embedding detail | res={res}")
        embedding = res['data'][0]['embedding']
        usage = res.get('usage',{})
        return {
            'result':'ok',
            'embedding': embedding,
            'model': model,
            'usage': {
                'completion_tokens' : usage.get('completion_tokens',0),
                'prompt_tokens' : usage.get('prompt_tokens', 0),
                'total_tokens' : usage.get('total_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'reason': 'unknown',
            'model': model
        }

def encoding_for_model(model):
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        logging.info(f"fallback to cl100k_base encoding for unknown openai-compatible model: {model}")
        return tiktoken.get_encoding("cl100k_base")

def format_preset(preset, model_config):
    model = preset.get('model', '')
    if model.startswith("o1") or model.startswith("o3-"):
        return format_preset_for_o1(preset)
    support_fields = ['model', "messages", "tools", "tool_choice", "temperature", "top_p", "n", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "logit_bias", "user", "stream"]
    if 'tools' not in preset and 'functions' in preset:
        preset = dict(preset)
        preset['tools'] = lanying_openai_compat.functions_to_tools(preset.get('functions', []))
    if 'tool_choice' not in preset and 'function_call' in preset:
        preset = dict(preset)
        preset['tool_choice'] = lanying_openai_compat.function_call_to_tool_choice(preset.get('function_call'))
    ret = dict()
    for key in support_fields:
        if key in preset:
            ret[key] = preset[key]
    if 'stream' in ret and ret['stream'] == True:
        ret['stream_options'] = {
            'include_usage': True
        }
    if model_config.get('support_audio', False) == True:
        if 'modalities' not in ret:
            ret['modalities'] = ["text", "audio"]
        if 'audio' not in ret:
            ret['audio'] = {"voice": "alloy", "format": "mp3"}
        if 'stream' in ret:
            del ret['stream']
        if 'stream_options' in ret:
            del ret['stream_options']
    return ret

def format_preset_for_o1(preset):
    support_fields = ['model', "messages", "max_completion_tokens"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "messages":
                messages = []
                for message in preset['messages']:
                    if 'role' in message:
                        if message['role'] == 'system':
                            message['role'] = 'user'
                            messages.append(message)
                        elif message['role'] == 'user' or message['role'] == 'assistant':
                            messages.append(message)
                        else:
                            logging.info(f"skip message for o1 {message}")
                ret[key] = messages
            else:
                ret[key] = preset[key]
    if 'max_completion_tokens' not in ret:
        ret['max_completion_tokens'] = 25000
    return ret

def is_official_openai_endpoint(api_endpoint):
    endpoint = str(api_endpoint or '').strip()
    if endpoint == '':
        return False
    try:
        parsed = urlparse(endpoint)
        host = (parsed.hostname or '').lower()
        return host == 'api.openai.com'
    except Exception:
        return False


def get_default_api_base(prepare_info):
    auth_info = prepare_info.get('auth_info', {}) or {}
    vendor_type = str(auth_info.get('vendor_type', '') or '').strip().lower()
    if vendor_type == 'openrouter':
        return 'https://openrouter.ai/api/v1'
    return 'https://api.openai.com/v1'


def get_api_endpoint_server_location(prepare_info):
    auth_info = prepare_info.get('auth_info', {}) or {}
    location = prepare_info.get('api_endpoint_server_location', auth_info.get('api_endpoint_server_location', 'overseas'))
    return str(location or 'overseas').strip().lower()


def should_use_proxy_for_custom_endpoint(prepare_info):
    return get_api_endpoint_server_location(prepare_info) != 'domestic'


def validate_custom_api_endpoint(api_endpoint):
    endpoint = str(api_endpoint or '').strip()
    if endpoint == '':
        return
    if not lanying_utils.is_valid_public_url(endpoint):
        raise ValueError('api_endpoint_not_valid')


def get_api_base_and_headers(prepare_info):
    proxy_api_base = os.getenv("LANYING_CONNECTOR_OPENAI_PROXY_API_BASE", '')
    proxy_api_key = os.getenv("LANYING_CONNECTOR_OPENAI_PROXY_API_KEY", '')
    api_key = prepare_info['api_key']
    api_endpoint = str(prepare_info.get('api_endpoint', '') or '').strip()
    default_api_base = get_default_api_base(prepare_info)
    target_api_base = default_api_base
    use_proxy = len(proxy_api_base) > 0
    if api_endpoint != '':
        if not is_official_openai_endpoint(api_endpoint):
            validate_custom_api_endpoint(api_endpoint)
            use_proxy = len(proxy_api_base) > 0 and should_use_proxy_for_custom_endpoint(prepare_info)
        target_api_base = api_endpoint.rstrip('/')
    if use_proxy:
        api_base = proxy_api_base
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {proxy_api_key}",
            "Authorization-Next": f"Bearer {api_key}"
        }
        headers[PROXY_API_ENDPOINT_HEADER] = target_api_base
    else:
        api_base = target_api_base
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
    return (api_base, headers)
