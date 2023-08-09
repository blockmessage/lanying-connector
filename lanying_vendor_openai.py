import logging
import openai
import tiktoken

def model_configs():
    return [
        {
            "model": 'gpt-4-32k',
            "type": "chat",
            "is_prefix": True,
            "quota": 40,
            "token_limit": 32000,
            'order': 4,
            'hidden': True
        },
        {
            "model": 'gpt-4',
            "type": "chat",
            "is_prefix": True,
            "quota": 20,
            "token_limit": 8000,
            'order': 3
        },
        {
            "model": 'gpt-3.5-turbo-16k',
            "type": "chat",
            "is_prefix": True,
            "quota": 2,
            "token_limit": 16000,
            'order': 2
        },
        {
            "model": 'gpt-3.5-turbo',
            "type": "chat",
            "is_prefix": True,
            "quota": 1,
            "token_limit": 4000,
            'order': 1
        },
        {
            "model": 'text-embedding-ada-002',
            "type": "embedding",
            "is_prefix": True,
            "quota": 0.05,
            "token_limit": 8000,
            'order': 1000
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
    openai.api_key = prepare_info['api_key']
    final_preset = format_preset(preset)
    response = openai.ChatCompletion.create(**final_preset)
    logging.info(f"vendor openai response: {response}")
    try:
        usage = response.get('usage',{})
        return {
            'result': 'ok',
            'reply' : response['choices'][0]['message']['content'].strip(),
            'usage' : {
                'completion_tokens' : usage.get('completion_tokens',0),
                'prompt_tokens' : usage.get('prompt_tokens', 0),
                'total_tokens' : usage.get('total_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'response': response 
        }

def prepare_embedding(auth_info, _):
    return {
        'api_key' : auth_info['api_key']
    }

def embedding(prepare_info, text):
    api_key = prepare_info['api_key']
    openai.api_key = api_key
    type = prepare_info.get('type', '')
    model = 'text-embedding-ada-002'
    logging.info(f"openai embedding start | type={type}, api_key:{api_key[:4]}...{api_key[-4:]}")
    response = openai.Embedding.create(input=text, engine=model)
    try:
        embedding = response['data'][0]['embedding']
        usage = response.get('usage',{})
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
        logging.info(f"fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'model': model,
            'response': response
        }

def encoding_for_model(model): 
    return tiktoken.encoding_for_model(model)

def format_preset(preset):
    support_fields = ['model', "messages", "functions", "function_call", "temperature", "top_p", "n", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "logit_bias", "user"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            ret[key] = preset[key]
    return ret
