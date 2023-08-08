import logging
import tiktoken
import zhipuai

def model_configs():
    return [
        {
            "model": 'chatglm_pro',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 4000
        },
        {
            "model": 'chatglm_std',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 4000
        },
        {
            "model": 'chatglm_lite',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.2,
            "token_limit": 4000
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset):
    zhipuai.api_key = prepare_info['api_key']
    final_preset = format_preset(preset)
    try:
        logging.info(f"zhipuai chat_completion start | preset={preset}, final_preset={final_preset}")
        response = zhipuai.model_api.invoke(**final_preset)
        logging.info(f"zhipuai chat_completion finish | response={response}")
        usage = response.get('usage',{})
        return {
            'result': 'ok',
            'reply' : response['data']['choices'][0]['content'].strip(),
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
            'reason': 'unknown'
        }

def prepare_embedding(auth_info, _):
    return {
        'api_key' : auth_info['api_key']
    }


def encoding_for_model(model): 
    return tiktoken.encoding_for_model(model)

def format_preset(preset):
    support_fields = ['model', "prompt", "temperature", "top_p"]
    ret = dict()
    for key in support_fields:
        if key == 'prompt':
            messages = []
            for message in preset.get('messages', []) + preset.get('prompt', []):
                if 'role' in message and 'content' in message:
                    if message['role'] == "system":
                        messages.append({'role':'user', 'content': message['content']})
                    else:
                        messages.append({'role':message['role'], 'content': message['content']})
            ret[key] = messages
        elif key == 'top_p':
            if key in preset:
                ret[key] = min(1.0, max(0.01, preset[key]))
        elif key == 'temperature':
            if key in preset:
                ret[key] = min(0.99, max(0.01, preset[key]))
        elif key in preset:
            ret[key] = preset[key]
    return ret
