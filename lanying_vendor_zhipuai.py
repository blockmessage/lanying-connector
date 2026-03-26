import logging
import tiktoken
from zhipuai import ZhipuAI
import zhipuai.core._errors
import json
import copy
import lanying_openai_compat

ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'
SUPPORT_NATIVE_TOOLS = True

def model_configs():
    return [
        {
            "model": 'glm-3-turbo',
            "is_origin_vendor": True,
            "service": 'zhipuai',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.96,
            "token_limit": 128000,
            'order': 1,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'glm-4',
            "is_origin_vendor": True,
            "service": 'zhipuai',
            "type": "chat",
            "is_prefix": False,
            "quota": 15.04,
            "token_limit": 128000,
            'order': 2,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'chatglm_pro',
            "is_origin_vendor": True,
            "service": 'zhipuai',
            "type": "chat",
            "is_prefix": False,
            "quota": 10,
            "token_limit": 128000,
            'order': 3,
            'hidden': True,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'chatglm_std',
            "is_origin_vendor": True,
            "service": 'zhipuai',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 128000,
            'order': 4,
            'hidden': True,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'chatglm_lite',
            "is_origin_vendor": True,
            "service": 'zhipuai',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 128000,
            'order': 5,
            'hidden': True,
            'max_output_tokens': 4096,
            'function_call': True
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset, model_config):
    client = ZhipuAI(api_key=prepare_info['api_key'])
    final_preset = format_preset(preset)
    response = None
    try:
        logging.info(f"zhipuai chat_completion start | preset={preset}")
        logging.info(f"zhipuai chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = client.chat.completions.create(**final_preset)
            #logging.info(f"zhipuai chat_completion finish | stream={stream}")
            def generator():
                for chunk in response:
                    # logging.info(f"chunk:{chunk}")
                    content = chunk.choices[0].delta.content
                    chunk_info = {}
                    if content:
                        chunk_info['content'] = content
                    else:
                        chunk_info['content'] = ''
                    if chunk.choices[0].delta.tool_calls:
                        tool_calls = []
                        for tool_call in chunk.choices[0].delta.tool_calls:
                            tool_calls.append({
                                'id': tool_call.id,
                                'type': 'function',
                                'function': {
                                    'name': tool_call.function.name,
                                    'arguments': tool_call.function.arguments
                                }
                            })
                        chunk_info['tool_calls'] = tool_calls
                    if chunk.usage:
                        chunk_info['usage'] = {
                            'completion_tokens' : chunk.usage.completion_tokens,
                            'prompt_tokens' : chunk.usage.prompt_tokens,
                            'total_tokens' : chunk.usage.total_tokens
                        }
                    finish_reason = ''
                    try:
                        finish_reason = chunk.choices[0].finish_reason
                        chunk_info['finish_reason'] = finish_reason
                    except Exception as e:
                        pass
                    # logging.info(f"yield delta: {chunk_info}")
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
            tool_calls = []
            if hasattr(message, 'tool_calls'):
                if message.tool_calls is not None and len(message.tool_calls) > 0:
                    for tool_call in message.tool_calls:
                        tool_calls.append({
                            'id': tool_call.id,
                            'type': 'function',
                            'function': {
                                'name': tool_call.function.name,
                                'arguments': tool_call.function.arguments
                            }
                        })
            finish_reason = ''
            try:
                finish_reason = response.choices[0].finish_reason
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'finish_reason': finish_reason,
                'tool_calls': tool_calls,
                'usage' : {
                    'completion_tokens' : usage.completion_tokens,
                    'prompt_tokens' : usage.prompt_tokens,
                    'total_tokens' : usage.total_tokens
                }
            }
    except APIRequestFailedError as e:
        pass
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
    support_fields = ['model', "messages", "temperature", "top_p", "max_tokens", "stop", "stream", "tools", "tool_choice"]
    if 'tools' not in preset and 'functions' in preset:
        preset = dict(preset)
        preset['tools'] = lanying_openai_compat.functions_to_tools(preset.get('functions', []))
    if 'tool_choice' not in preset and 'function_call' in preset:
        preset = dict(preset)
        preset['tool_choice'] = lanying_openai_compat.function_call_to_tool_choice(preset.get('function_call'))
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "messages":
                messages = []
                for message in preset['messages']:
                    item, _ = lanying_openai_compat.normalize_chat_message(message)
                    if item is not None:
                        messages.append(item)
                ret['messages'] = messages
            elif key == 'stop':
                if len(key) == 1:
                    ret[key] = preset[key]
            elif key == 'top_p':
                value = preset[key]
                if value >= 1:
                    value = 0.9
                elif value <=0:
                    value = 0.1
                ret[key] = value
            else:
                ret[key] = preset[key]
    return ret
