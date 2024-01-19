import logging
import tiktoken
from zhipuai import ZhipuAI
import json
import copy

ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'

def model_configs():
    return [
        {
            "model": 'glm-3-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 128000,
            'order': 1
        },
        {
            "model": 'glm-4',
            "type": "chat",
            "is_prefix": False,
            "quota": 10,
            "token_limit": 128000,
            'order': 2
        },
        {
            "model": 'chatglm_pro',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 4000,
            'order': 3,
            'hidden': True
        },
        {
            "model": 'chatglm_std',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 4000,
            'order': 4,
            'hidden': True
        },
        {
            "model": 'chatglm_lite',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.2,
            "token_limit": 4000,
            'order': 5,
            'hidden': True
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
            #logging.info(f"zhipuai chat_completion finish | stream={stream}")
            def generator():
                for chunk in response:
                    logging.info(f"chunk:{chunk}")
                    content = chunk.choices[0].delta.content
                    chunk_info = {}
                    if content:
                        chunk_info['content'] = content
                    else:
                        chunk_info['content'] = ''
                    if chunk.choices[0].delta.tool_calls:
                        tool_calls = chunk.choices[0].delta.tool_calls
                        chunk_info['function_call'] = {
                            'name': tool_calls[0].function.name,
                            'arguments': tool_calls[0].function.arguments,
                            'id': tool_calls[0].id
                        }
                    if chunk.usage:
                        chunk_info['usage'] = chunk.usage
                    yield chunk_info
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
            message = response.choices[0].message
            if message.content:
                reply = message.content
            else:
                reply = ''
            function_call = None
            if hasattr(message, 'tool_calls'):
                if message.tool_calls is not None and len(message.tool_calls) > 0:
                    function_call = {
                        'name': message.tool_calls[0].function.name,
                        'arguments': message.tool_calls[0].function.arguments,
                        'id': message.tool_calls[0].id
                    }
            return {
                'result': 'ok',
                'reply' : reply,
                'function_call': function_call,
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
    support_fields = ['model', "messages", "temperature", "top_p", "max_tokens", "stop", "stream", "functions"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "functions":
                tools = []
                for function in preset['functions']:
                    function_obj = {}
                    for k,v in function.items():
                        if k in ["name", "description", "parameters"]:
                            function_obj[k] = v
                    tools.append({'type':'function', 'function':function_obj})
                ret['tools'] = tools
            elif key == "messages":
                last_tool_call_id = ''
                messages = []
                for message in preset['messages']:
                    logging.info(f"message:{message}")
                    if 'function_call' in message:
                        message = copy.deepcopy(message)
                        function_call = message['function_call']
                        last_tool_call_id = function_call.get('id', ''),
                        tool_calls = [{
                            'tool_call_id': function_call.get('id', ''),
                            'type':'function',
                            'function':{
                                'name': function_call['name'],
                                'arguments': function_call['arguments']
                            }}]
                        message['tool_calls'] = tool_calls
                        del message['function_call']
                    elif message['role'] == 'function':
                        message = copy.deepcopy(message)
                        message['role'] = 'tool'
                        message['tool_call_id'] = last_tool_call_id
                    messages.append(message)
                ret['messages'] = messages
            elif key == 'stop':
                if len(key) == 1:
                    ret[key] = preset[key]
            else:
                ret[key] = preset[key]
    return ret
