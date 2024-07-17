import logging
import tiktoken
import requests
import json

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
            'function_call': True
        },
        {
            "model": 'gpt-4',
            "type": "chat",
            "is_prefix": False,
            "quota": 20,
            "token_limit": 8000,
            'order': 3,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-4/chat/completions?api-version=2023-12-01-preview',
            'function_call': True
        },
        {
            "model": 'gpt-35-turbo-16k',
            "type": "chat",
            "is_prefix": False,
            "quota": 2,
            "token_limit": 16000,
            'order': 2,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo-16k/chat/completions?api-version=2023-12-01-preview',
            'function_call': True
        },
        {
            "model": 'gpt-35-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 4000,
            'order': 1,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-12-01-preview',
            'function_call': True
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
            'dim_origin': 1536
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
    url = get_chat_model_url(preset['model'])
    final_preset = format_preset(preset)
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
    url = get_chat_model_url(model)
    headers = {"Content-Type": "application/json", "api-key": prepare_info['api_key']}
    json_body = {"input":text, "model":model}
    try:
        logging.info(f"azure embedding start")
        response = requests.request("POST", url, headers=headers, json=json_body)
        logging.info(f"azure embedding finish: response:{response}")
        res = response.json()
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

def get_chat_model_url(model):
    for config in model_configs():
        if model == config['model']:
            return config['url']
    return None
