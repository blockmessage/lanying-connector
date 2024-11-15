import logging
import tiktoken
import requests
import json
import os

def model_configs():
    return [
        {
            "model": 'gpt-4-32k',
            "type": "chat",
            "is_prefix": False,
            "quota": 40,
            "token_limit": 32000,
            'order': 4,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-4-32k/chat/completions?api-version=2023-07-01-preview',
            'function_call': True,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-4',
            "type": "chat",
            "is_prefix": False,
            "quota": 20,
            "token_limit": 8000,
            'order': 3,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-4/chat/completions?api-version=2023-12-01-preview',
            'function_call': True,
            'api_type': 'openai'
        },
        {
            "model": 'gpt-35-turbo-16k',
            "type": "chat",
            "is_prefix": False,
            "quota": 2,
            "token_limit": 16000,
            'order': 2,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo-16k/chat/completions?api-version=2023-12-01-preview',
            'function_call': True,
            'real_model': 'gpt-3.5-turbo-16k',
            'api_type': 'openai'
        },
        {
            "model": 'gpt-35-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 4000,
            'order': 1,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-12-01-preview',
            'function_call': True,
            'real_model': 'gpt-3.5-turbo',
            'api_type': 'openai'
        },
        {
            "model": 'text-embedding-ada-002',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.05,
            "token_limit": 8000,
            'order': 1000,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-12-01-preview',
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
                    if k in ['role', 'content', 'name', 'function_call']:
                        msg[k] = v
                messages.append(msg)
        preset['messages'] = messages
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset):
    model_config = get_chat_model_config(preset['model'])
    real_model = model_config.get('real_model', None)
    final_preset = format_preset(preset)
    if real_model:
        final_preset['model'] = real_model
    api_type = model_config.get('api_type', 'azure')
    if api_type == 'openai':
        api_endpoint = os.getenv('AZURE_API_ENDPOINT', '') 
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + prepare_info['api_key']}
        url = maybe_add_proxy_headers(prepare_info, api_endpoint, headers) + '/v1/chat/completions'
    else:
        url = model_config['url']
        headers = {"Content-Type": "application/json", "api-key": prepare_info['api_key']}
    try:
        logging.info(f"azure chat_completion start | preset={preset}, url:{url}")
        logging.info(f"azure chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"azure chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        # logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                choice = data['choices'][0]
                                delta = choice['delta']
                                if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                    delta['finish_reason'] = choice['finish_reason']
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
            response = requests.request("POST", url, headers=headers, json=final_preset)
            logging.info(f"azure chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            usage = res.get('usage',{})
            response_message = res['choices'][0]['message']
            reply = response_message.get('content', "")
            if reply:
                reply = reply.strip()
            else:
                reply = ''
            function_call = response_message.get('function_call')
            finish_reason = ''
            try:
                finish_reason = res['choices'][0]['finish_reason']
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'finish_reason': finish_reason,
                'function_call': function_call,
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
    return {
        'api_key' : auth_info['api_key']
    }

def embedding(prepare_info, model, text):
    model = 'text-embedding-ada-002'
    model_config = get_chat_model_config(model)
    api_type = model_config.get('api_type', 'azure')
    if api_type == 'openai':
        api_endpoint = os.getenv('AZURE_API_ENDPOINT', '')
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + prepare_info['api_key']}
        url = maybe_add_proxy_headers(prepare_info, api_endpoint, headers) + '/v1/embeddings'
    else:
        url = model_config['url']
        headers = {"Content-Type": "application/json", "api-key": prepare_info['api_key']}
    json_body = {"input":text, "model":model}
    try:
        logging.info(f"azure embedding start")
        response = requests.request("POST", url, headers=headers, json=json_body)
        logging.info(f"azure embedding finish: response:{response}")
        res = response.json()
        if 'data' not in res:
            logging.info(f"azure embedding finish with error:{res}")
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
    if model.startswith("o1-"):
        return tiktoken.encoding_for_model("gpt-4o")
    return tiktoken.encoding_for_model(model)

def format_preset(preset):
    model = preset.get('model', '')
    if model.startswith("o1-"):
        return format_preset_for_o1(preset)
    support_fields = ['model', "messages", "function_call", "temperature", "top_p", "n", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "logit_bias", "user", "stream", "functions"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "functions":
                functions = []
                for function in preset['functions']:
                    function_obj = {}
                    for k,v in function.items():
                        if k in ["name", "description", "parameters"]:
                            function_obj[k] = v
                    functions.append(function_obj)
                ret[key] = functions
            else:
                ret[key] = preset[key]
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

def get_chat_model_url(model):
    for config in model_configs():
        if model == config['model']:
            return config['url']
    return None

def get_chat_model_real_model(model):
    for config in model_configs():
        if model == config['model']:
            if 'real_model' in config:
                return config['real_model']
            else:
                return model
    return None

def get_chat_model_config(model):
    for config in model_configs():
        if model == config['model']:
            return config
    return None

def maybe_add_proxy_headers(prepare_info, api_endpoint, headers):
    domain = os.getenv("LANYING_CONNECTOR_AZURE_PROXY_DOMAIN", '')
    proxy_api_key = os.getenv("LANYING_CONNECTOR_AZURE_PROXY_API_KEY", '')
    if len(domain) > 0:
        api_key = prepare_info['api_key']
        headers['Authorization'] = f"Basic {proxy_api_key}"
        headers['Authorization-Next'] = f"Bearer {api_key}"
        return domain
    else:
        return api_endpoint
