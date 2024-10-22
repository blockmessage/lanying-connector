import logging
import anthropic
from anthropic import AnthropicBedrock
import os
import time
import tiktoken
from anthropic.types import (
    RawMessageStartEvent,
    RawContentBlockDeltaEvent,
    RawContentBlockStartEvent,
    RawMessageDeltaEvent,
    RawContentBlockStopEvent,
    Message,
    TextBlock,
    ToolUseBlock
)
import json
ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'

def model_configs():
    return [
        {
            "model": 'anthropic.claude-3-sonnet-20240229-v1:0',
            "type": "chat",
            "is_prefix": False,
            "quota": 5.61,
            "token_limit": 200000,
            'order': 1,
            'function_call': True
        },
        {
            "model": 'anthropic.claude-3-haiku-20240307-v1:0',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.54,
            "token_limit": 200000,
            'order': 2,
            'function_call': True
        },
        {
            "model": 'anthropic.claude-3-opus-20240229-v1:0',
            "type": "chat",
            "is_prefix": False,
            "quota": 30.78,
            "token_limit": 200000,
            'order': 3,
            'function_call': True
        },
        {
            "model": 'anthropic.claude-3-5-sonnet-20240620-v1:0',
            "type": "chat",
            "is_prefix": False,
            "quota": 5.61,
            "token_limit": 200000,
            'order': 4,
            'function_call': True
        },
        {
            "model": 'anthropic.claude-v2:1',
            "type": "chat",
            "is_prefix": False,
            "quota": 9.90,
            "token_limit": 200000,
            'order': 5,
            'function_call': False
        },
        {
            "model": 'anthropic.claude-v2',
            "type": "chat",
            "is_prefix": False,
            "quota": 9.90,
            "token_limit": 200000,
            'order': 6,
            'function_call': False
        },
        {
            "model": 'anthropic.claude-instant-v1',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.06,
            "token_limit": 200000,
            'order': 7,
            'function_call': False
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'aws_access_key' : auth_info['api_key'],
        'aws_secret_key' : auth_info['secret_key'],
        'region': 'us-west-2'
    }

def chat(prepare_info, preset):
    from lanying_vendor import get_chat_model_config
    model_config = get_chat_model_config('aws', preset['model'])
    client = AnthropicBedrock(
        aws_access_key=prepare_info['aws_access_key'],
        aws_secret_key=prepare_info['aws_secret_key'],
        aws_region=prepare_info['region'])
    final_preset = format_preset(preset, model_config)
    headers = maybe_add_proxy_headers(prepare_info, client)
    logging.info(f"vendor aws chat request: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
    retry_times = 1
    response = None
    task_id = time.time()
    for i in range(retry_times):
        logging.info(f"vendor aws start try task_id:{task_id}, {i}/{retry_times}")
        try:
            response = client.messages.create(**final_preset, extra_headers = headers)
            break
        except Exception as e:
            if i == retry_times - 1:
                logging.info(f"vendor aws chat complete stop retry: task_id:{task_id}, {i}/{retry_times}")
                raise e
            else:
                logging.info(f"vendor aws chat complete got exception: task_id:{task_id}, {i}/{retry_times}")
                logging.exception(e)
                try:
                    logging.info(dir(e))
                except Exception as ee:
                    pass
                time.sleep(2)
    logging.info(f"vendor aws chat response: task_id:{task_id}, {response}")
    if isinstance(response, anthropic.Stream):
        def generator():
            usage = {
                'completion_tokens': 0,
                'prompt_tokens': 0,
                'total_tokens': 0
            }
            function_call = None
            function_content = ''
            for chunk in response:
                # logging.info(f"vendor aws chunk: {chunk}")
                chunk_reply = {}
                if isinstance(chunk, RawContentBlockStartEvent):
                    if chunk.content_block.type == 'text':
                        content = chunk.content_block.text
                        chunk_reply['content'] = content
                    elif chunk.content_block.type == 'tool_use':
                        function_call = {
                            'id': chunk.content_block.id,
                            'name': chunk.content_block.name,
                            'arguments': chunk.content_block.input
                        }
                elif isinstance(chunk, RawContentBlockDeltaEvent):
                    if chunk.delta.type == 'text_delta':
                        content = chunk.delta.text
                        chunk_reply['content'] = content
                    elif chunk.delta.type == 'input_json_delta':
                        function_content += chunk.delta.partial_json
                elif isinstance(chunk, RawMessageDeltaEvent):
                    if chunk.usage:
                        usage['completion_tokens'] = chunk.usage.output_tokens
                        usage['total_tokens'] = usage['prompt_tokens'] + usage['completion_tokens']
                        chunk_reply['usage'] = usage
                        finish_reason = ''
                        try:
                            finish_reason = str(chunk.delta.stop_reason)
                            chunk_reply['finish_reason'] = finish_reason
                        except Exception as e:
                            pass
                elif isinstance(chunk, RawMessageStartEvent):
                    if chunk.message.usage:
                        usage['prompt_tokens'] = chunk.message.usage.input_tokens
                        usage['completion_tokens'] = chunk.message.usage.output_tokens
                        usage['total_tokens'] = usage['prompt_tokens'] + usage['completion_tokens']
                        chunk_reply['usage'] = usage
                elif isinstance(chunk, RawContentBlockStopEvent):
                    if function_call is not None:
                        function_call['arguments'] = function_content
                        chunk_reply['function_call'] = function_call
                        function_call = None
                if len(chunk_reply) > 0:
                    # logging.info(f"vendor aws yield:{chunk_reply}")
                    yield chunk_reply
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
    try:
        if isinstance(response, Message):
            usage = response.usage
            reply = ''
            function_call = None
            try:
                for content in response.content:
                    if isinstance(content, TextBlock):
                        reply += content.text
                    elif isinstance(content, ToolUseBlock):
                        function_call = {
                            'name': content.name,
                            'arguments': json.dumps(content.input, ensure_ascii=False),
                            'id': content.id,
                        }
            except Exception as ee:
                logging.exception(ee)
                pass
            if reply:
                reply = reply.strip()
            else:
                reply = ''
            finish_reason = ''
            try:
                finish_reason = str(response.stop_reason)
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'function_call' : function_call,
                'finish_reason': finish_reason,
                'usage' : {
                    'completion_tokens' : usage.output_tokens,
                    'prompt_tokens' : usage.input_tokens,
                    'total_tokens' : usage.input_tokens + usage.output_tokens
                }
            }
        else:
            return {
                'result': 'error',
                'reason': 'unknown',
                'response': response 
            }
    except Exception as e:
        logging.exception(e)
        logging.info(f"vendor aws fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'response': response 
        }

def format_preset(preset, model_config):
    support_fields = ['system', 'model', "messages", "temperature", "top_p", "top_k", "stop_sequences", "max_tokens","stream", "functions"]
    ret = dict()
    support_function_call = (model_config.get('function_call', True) == True)
    for key in support_fields:
        if key in preset:
            if key == "messages":
                last_tool_call_id = ''
                messages = []
                system_message = ret.get('system', '')
                for message in preset['messages']:
                    if 'role' in message and 'content' in message:
                        role = message['role']
                        content = message['content']
                        if role == 'system':
                            if len(content) > 0:
                                if system_message == '':
                                    system_message = content
                                else:
                                    system_message += "\n\n\n" + content
                        elif role == "user":
                            if len(messages) > 0 and messages[-1]['role'] == 'user':
                                messages.append({'role':'assistant', 'content':ASSISTANT_MESSAGE_DEFAULT})
                            messages.append({'role': role, 'content':content})
                        elif role == 'assistant':
                            if support_function_call and 'function_call' in message:
                                last_tool_call_id = message['function_call'].get('id', '')
                                new_message = {
                                    'role': role,
                                    'content': [
                                        {
                                            "type": "tool_use",
                                            "id": message['function_call'].get('id', ''),
                                            "name": message['function_call'].get('name', ''),
                                            "input": json.loads(message['function_call'].get('arguments', '{}'))
                                        }
                                    ]
                                }
                            else:
                                new_message = {
                                    'role': role,
                                    'content': content
                                }
                            if len(new_message['content']) > 0:
                                if len(messages) > 0 and messages[-1]['role'] == 'assistant':
                                    messages.append({'role':'user', 'content':USER_MESSAGE_DEFAULT})
                                messages.append(new_message)
                        elif role == 'function':
                            if support_function_call:
                                function_message = {
                                    'role': 'user',
                                    'content': [
                                        {
                                            "type": "tool_result",
                                            "tool_use_id": last_tool_call_id,
                                            "content": message['content']
                                        }
                                    ]
                                }
                                messages.append(function_message)
                    else:
                        logging.info(f"vendor aws ingore message in preset: {message}")
                if len(system_message) > 0:
                    ret['system'] = system_message
                ret[key] = messages
            elif key == 'functions':
                if support_function_call:
                    tools = []
                    for function in preset['functions']:
                        function_obj = {}
                        for k,v in function.items():
                            if k in ["name", "description", "parameters"]:
                                if k == 'parameters':
                                    function_obj['input_schema'] = v
                                else:
                                    function_obj[k] = v
                        tools.append(function_obj)
                    ret['tools'] = tools
            else:
                ret[key] = preset[key]
        else:
            if key == 'max_tokens':
                ret[key] = 1024
    return ret

def maybe_add_proxy_headers(prepare_info, client):
    proxy_api_base = os.getenv("LANYING_CONNECTOR_AWS_PROXY_API_BASE", '')
    proxy_api_key = os.getenv("LANYING_CONNECTOR_AWS_PROXY_API_KEY", '')
    if len(proxy_api_base) > 0:
        client.base_url = proxy_api_base
        return {
            "Authorization": f"Basic {proxy_api_key}"
        }
    else:
        return {}

def encoding_for_model(model): # for temp
    return tiktoken.encoding_for_model("gpt-3.5-turbo")