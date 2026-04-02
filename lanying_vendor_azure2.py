import logging
import tiktoken
import requests
import json
import os
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
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.49,
            "token_limit": 128000,
            "support_vision": False,
            'order': 1,
            "url": '',
            'function_call': True,
            'max_output_tokens': 16384,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-4o',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 8.00,
            "token_limit": 128000,
            "support_vision": False,
            'order': 2,
            "url": '',
            'function_call': True,
            'max_output_tokens': 16384,
            'api_type': 'openai'
        },
        # {
        #     "model": 'o3-mini',
        #     "service": 'chatgpt',
        #     "type": "chat",
        #     "is_prefix": False,
        #     "quota": 2.22,
        #     "token_limit": 200000,
        #     "support_vision": False,
        #     'order': 2.5,
        #     "url": '',
        #     'function_call': False,
        #     'support_stream': False,
        #     'support_system_role': False,
        #     'max_output_tokens': 100000,
        #     'api_type': 'openai'
        # },
        {
            "model": 'o1-mini',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 2.22,
            "token_limit": 128000,
            "support_vision": False,
            'order': 3,
            "url": '',
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 65536,
            'api_type': 'openai'
        },
        {
            "model": 'o1',
            'real_model': 'o1-all',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 27.44,
            "token_limit": 200000,
            "support_vision": False,
            'order': 4,
            "url": '',
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 100000,
            'api_type': 'openai'
        },
        {
            "model": 'o1-preview',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 27.44,
            "token_limit": 128000,
            "support_vision": False,
            'order': 4,
            "url": '',
            'function_call': False,
            'support_stream': False,
            'support_system_role': False,
            'max_output_tokens': 32768,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-3.5-turbo',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.00,
            "token_limit": 16385,
            'order': 5,
            "url": '',
            'function_call': True,
            'max_output_tokens': 4096,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-4',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 39.11,
            "token_limit": 8192,
            'order': 6,
            "url": '',
            'function_call': True,
            'max_output_tokens': 8192,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-4-32k',
            "service": 'chatgpt',
            "type": "chat",
            "is_prefix": False,
            "quota": 78.00,
            "token_limit": 32768,
            'order': 7,
            "url": '',
            'function_call': True,
            'max_output_tokens': 8192,
            'api_type': 'openai'
        },
        {
            "model": 'text-embedding-ada-002',
            "service": 'chatgpt',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.13,
            "token_limit": 8191,
            'order': 1000,
            "url": '',
            'dim': 1536,
            'dim_origin': 1536,
            'api_type': 'openai'
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
    prepare_info = {
        'api_key' : auth_info['api_key']
    }
    if 'api_endpoint' in auth_info and auth_info['api_endpoint'] != '':
        prepare_info['api_endpoint'] = auth_info['api_endpoint']
    return prepare_info

def chat(prepare_info, preset, model_config):
    real_model = model_config.get('real_model', None)
    final_preset = format_preset(preset)
    if real_model:
        final_preset['model'] = real_model
    api_type = model_config.get('api_type', 'azure')
    if api_type == 'openai':
        api_endpoint = os.getenv('AZURE2_API_ENDPOINT', '') 
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + prepare_info['api_key']}
        url = maybe_add_proxy_headers(prepare_info, api_endpoint, headers) + '/v1/chat/completions'
    else:
        url = model_config['url']
        headers = {"Content-Type": "application/json", "api-key": prepare_info['api_key']}
    try:
        logging.info(f"azure2 chat_completion start | preset={preset}, url:{url}")
        logging.info(f"azure2 chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        request_timeout = get_request_timeout(prepare_info)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True, timeout=request_timeout)
            logging.info(f"azure2 chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    is_in_reasoning = False
                    is_reasoning_finish = False
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                delta = None
                                if 'choices' in data and len(data['choices']) > 0:
                                    choice = data['choices'][0]
                                    delta = choice['delta']
                                    if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                        delta['finish_reason'] = choice['finish_reason']
                                    if 'content' in delta and isinstance(delta['content'], str):
                                        if not is_reasoning_finish:
                                            if not is_in_reasoning and delta['content'].startswith('> Reasoning'):
                                                is_in_reasoning = True
                                            elif not is_in_reasoning and len(delta['content']) > 0:
                                                is_reasoning_finish = True
                                            elif is_in_reasoning and '\n\n' in delta['content']:
                                                is_reasoning_finish = True
                                    if is_in_reasoning:
                                        if 'content' in delta:
                                            delta['reasoning_content'] = delta['content']
                                            del delta['content']
                                    if is_reasoning_finish:
                                        is_in_reasoning = False
                                else:
                                    if 'usage' in data and isinstance(data['usage'], dict):
                                        delta = {
                                            'usage' : data['usage']
                                        }
                                if delta:
                                    logging.info(f"yield delta:{delta}")
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
                    'response': response_json
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset, timeout=request_timeout)
            logging.info(f"azure2 chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            usage = res.get('usage',{})
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
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'finish_reason': finish_reason,
                'tool_calls': tool_calls,
                'usage' : {
                    'completion_tokens' : usage.get('completion_tokens',0),
                    'prompt_tokens' : usage.get('prompt_tokens', 0),
                    'total_tokens' : usage.get('total_tokens', 0)
                }
            }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'reason': 'exception'
        }

def prepare_embedding(auth_info, _):
    prepare_info = {
        'api_key' : auth_info['api_key']
    }
    if 'api_endpoint' in auth_info and auth_info['api_endpoint'] != '':
        prepare_info['api_endpoint'] = auth_info['api_endpoint']
    return prepare_info

def embedding(prepare_info, model, text, model_config):
    model = 'text-embedding-ada-002'
    api_type = model_config.get('api_type', 'azure')
    if api_type == 'openai':
        api_endpoint = os.getenv('AZURE2_API_ENDPOINT', '')
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + prepare_info['api_key']}
        url = maybe_add_proxy_headers(prepare_info, api_endpoint, headers) + '/v1/embeddings'
    else:
        url = model_config['url']
        headers = {"Content-Type": "application/json", "api-key": prepare_info['api_key']}
    json_body = {"input":text, "model":model}
    try:
        logging.info(f"azure2 embedding start")
        response = requests.request("POST", url, headers=headers, json=json_body)
        logging.info(f"azure2 embedding finish: response:{response}")
        res = response.json()
        if 'data' not in res:
            logging.info(f"azure2 embedding finish with error:{res}")
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
    if model.startswith("gpt-35-turbo"):
        return tiktoken.encoding_for_model("gpt-3.5-turbo")
    return tiktoken.encoding_for_model(model)

def format_preset(preset):
    model = preset.get('model', '')
    if model.startswith("o1"):
        return format_preset_for_o1(preset)
    support_fields = ['model', "messages", "tool_choice", "temperature", "top_p", "n", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "logit_bias", "user", "stream", "tools"]
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
    return ret

def format_preset_for_o1(preset):
    support_fields = ['model', "messages", "max_completion_tokens", "stream"]
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
    if 'stream' in ret and ret['stream'] == True:
        ret['stream_options'] = {
            'include_usage': True
        }
    return ret

def maybe_add_proxy_headers(prepare_info, api_endpoint, headers):
    domain = os.getenv("LANYING_CONNECTOR_AZURE2_PROXY_DOMAIN", '')
    proxy_api_key = os.getenv("LANYING_CONNECTOR_AZURE2_PROXY_API_KEY", '')
    custom_api_endpoint = prepare_info.get('api_endpoint')
    if custom_api_endpoint:
        if not lanying_utils.is_valid_public_url(custom_api_endpoint):
            raise ValueError('api_endpoint_not_valid')
    if len(domain) > 0:
        api_key = prepare_info['api_key']
        headers['Authorization'] = f"Basic {proxy_api_key}"
        headers['Authorization-Next'] = f"Bearer {api_key}"
        if custom_api_endpoint:
            headers[PROXY_API_ENDPOINT_HEADER] = custom_api_endpoint
        return domain
    if custom_api_endpoint:
        return custom_api_endpoint
    else:
        return api_endpoint
