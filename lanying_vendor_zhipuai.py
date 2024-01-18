import logging
import tiktoken
from zhipuai import ZhipuAI
import json

ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'

def model_configs():
    return [
        {
            "model": 'glm-3-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 128000,
            'order': 1
        },
        {
            "model": 'glm-4',
            "type": "chat",
            "is_prefix": False,
            "quota": 20,
            "token_limit": 128000,
            'order': 2
        },
        {
            "model": 'chatglm_pro',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 4000,
            'order': 3
        },
        {
            "model": 'chatglm_std',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 4000,
            'order': 4
        },
        {
            "model": 'chatglm_lite',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.2,
            "token_limit": 4000,
            'order': 5
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset):
    client = ZhipuAI(api_key=prepare_info['api_key'])
    final_preset = format_preset(preset)
    try:
        logging.info(f"zhipuai chat_completion start | preset={preset}, final_preset={final_preset}")
        stream = final_preset.get("stream", False)
        if stream:
            response = client.chat.completions.create(**final_preset)
            logging.info(f"zhipuai chat_completion finish | stream={stream}")
            def generator():
                for chunk in response:
                    logging.info(f"chunk.choices[0].delta:{chunk.choices[0].delta}")
                    content = chunk.choices[0].delta.content
                    if 'usage' in chunk:
                        yield {'content':content, 'usage': chunk.get('usage')}
                    else:
                        yield {'content':content}
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
            response = client.chat.completions.create(**final_preset)
            logging.info(f"zhipuai chat_completion finish | response={response}")
            usage = response.usage
            reply = response.choices[0].message.content
            return {
                'result': 'ok',
                'reply' : reply,
                'usage' : {
                    'completion_tokens' : usage.completion_tokens,
                    'prompt_tokens' : usage.prompt_tokens,
                    'total_tokens' : usage.total_tokens
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


def encoding_for_model(model): # for temp
    return tiktoken.encoding_for_model("gpt-3.5-turbo")

def format_preset(preset):
    support_fields = ['model', "messages", "temperature", "top_p", "max_tokens", "stop", "stream"]
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
            elif key == 'stop':
                if len(key) == 1:
                    ret[key] = preset[key]
            else:
                ret[key] = preset[key]
    return ret
