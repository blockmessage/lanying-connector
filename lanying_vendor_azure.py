import logging
import tiktoken
import requests

def model_configs():
    return [
        {
            "model": 'gpt-35-turbo-16k',
            "type": "chat",
            "is_prefix": True,
            "quota": 2,
            "token_limit": 16000,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo-16k/chat/completions?api-version=2023-03-15-preview'
        },
        {
            "model": 'gpt-35-turbo',
            "type": "chat",
            "is_prefix": True,
            "quota": 1,
            "token_limit": 4000,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-03-15-preview'
        },
        {
            "model": 'text-embedding-ada-002',
            "type": "embedding",
            "is_prefix": True,
            "quota": 0.05,
            "token_limit": 8000,
            "url": 'https://xiaolanai-eastus.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-03-15-preview'
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
        logging.info(f"azure chat_completion start | preset={preset}, final_preset={final_preset}, url:{url}")
        response = requests.request("POST", url, headers=headers, json=final_preset)
        logging.info(f"azure chat_completion finish | code={response.status_code}, response={response.text}")
        res = response.json()
        usage = res.get('usage',{})
        return {
            'result': 'ok',
            'reply' : res['choices'][0]['message']['content'].strip(),
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

def embedding(prepare_info, text):
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
    support_fields = ["model", "messages", "functions", "function_call", "temperature", "top_p", "n", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "logit_bias", "user"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            ret[key] = preset[key]
    return ret

def get_chat_model_url(model):
    for config in model_configs():
        if model.startswith(config['model']):
            return config['url']
    return None
