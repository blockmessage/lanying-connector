import logging
import anthropic
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
import lanying_openai_compat
ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'
SUPPORT_NATIVE_TOOLS = True

def model_configs():
    return [
        {
            "model": 'claude-3-5-haiku-20241022',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.88,
            "token_limit": 200000,
            'order': 1,
            'max_output_tokens': 8192,
            'function_call': True
        },
        {
            "model": 'claude-3-5-sonnet-20241022',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 6.44,
            "token_limit": 200000,
            'order': 2,
            'max_output_tokens': 8192,
            'function_call': True
        },
        {
            "model": 'claude-3-opus-20240229',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 31.33,
            "token_limit": 200000,
            'order': 3,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'claude-3-sonnet-20240229',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 6.44,
            "token_limit": 200000,
            'order': 4,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'claude-3-haiku-20240307',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.74,
            "token_limit": 200000,
            'order': 5,
            'max_output_tokens': 4096,
            'function_call': True
        },
        {
            "model": 'claude-2.1',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 12.67,
            "token_limit": 200000,
            'order': 8,
            'max_output_tokens': 4096,
            'function_call': False
        },
        {
            "model": 'claude-2.0',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 12.67,
            "token_limit": 100000,
            'order': 9,
            'max_output_tokens': 4096,
            'function_call': False
        },
        {
            "model": 'claude-instant-1.2',
            "is_origin_vendor": True,
            "service": 'claude',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.47,
            "token_limit": 100000,
            'order': 10,
            'max_output_tokens': 4096,
            'function_call': False
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset, model_config):
    client = anthropic.Anthropic(
        api_key=prepare_info['api_key']
    )
    final_preset = format_preset(preset, model_config)
    headers = maybe_add_proxy_headers(prepare_info, client)
    logging.info(f"vendor claude chat request: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
    retry_times = 1
    response = None
    task_id = time.time()
    for i in range(retry_times):
        logging.info(f"vendor claude start try task_id:{task_id}, {i}/{retry_times}")
        try:
            response = client.messages.create(**final_preset, extra_headers = headers)
            break
        except Exception as e:
            if i == retry_times - 1:
                logging.info(f"vendor claude chat complete stop retry: task_id:{task_id}, {i}/{retry_times}")
                raise e
            else:
                logging.info(f"vendor claude chat complete got exception: task_id:{task_id}, {i}/{retry_times}")
                logging.exception(e)
                try:
                    logging.info(dir(e))
                except Exception as ee:
                    pass
                time.sleep(2)
    logging.info(f"vendor claude chat response: task_id:{task_id}, {response}")
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
                # logging.info(f"vendor claude chunk: {chunk}")
                chunk_reply = {}
                if isinstance(chunk, RawContentBlockStartEvent):
                    if chunk.content_block.type == 'text':
                        content = chunk.content_block.text
                        chunk_reply['content'] = content
                    elif chunk.content_block.type == 'tool_use':
                        function_content = ''
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
                        chunk_reply['tool_calls'] = lanying_openai_compat.function_call_to_tool_calls(function_call)
                        function_call = None
                        function_content = ''
                if len(chunk_reply) > 0:
                    # logging.info(f"vendor claude yield:{chunk_reply}")
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
            tool_calls = []
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
                        tool_calls.append(lanying_openai_compat.function_call_to_tool_calls(function_call)[0])
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
                'tool_calls' : tool_calls,
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
        logging.info(f"vendor claude fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'response': response 
        }

def format_preset(preset, model_config):
    support_fields = ['system', 'model', "messages", "temperature", "top_p", "top_k", "stop_sequences", "max_tokens", "stream", "functions", "tools", "tool_choice"]
    ret = dict()
    support_function_call = (model_config.get('function_call', True) == True)
    preset_for_tools = preset
    if 'functions' not in preset_for_tools and 'tools' in preset_for_tools:
        preset_for_tools = dict(preset_for_tools)
        preset_for_tools['functions'] = lanying_openai_compat.tools_to_functions(preset_for_tools.get('tools', []))
    for key in support_fields:
        if key in preset_for_tools:
            if key == "messages":
                last_tool_call_id = ''
                messages = []
                system_message = ret.get('system', '')
                for message in preset_for_tools['messages']:
                    normalized_message, last_tool_call_id = lanying_openai_compat.normalize_chat_message(message, last_tool_call_id)
                    if normalized_message is not None and 'role' in normalized_message and 'content' in normalized_message:
                        role = normalized_message['role']
                        content = lanying_openai_compat.extract_text_from_content(normalized_message['content'])
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
                            tool_calls = normalized_message.get('tool_calls', [])
                            if support_function_call and isinstance(tool_calls, list) and len(tool_calls) > 0:
                                tool_use_content = []
                                for tool_call in tool_calls:
                                    function_call = lanying_openai_compat.tool_calls_to_function_call([tool_call]) or {}
                                    if function_call.get('id', '') != '':
                                        last_tool_call_id = function_call.get('id', '')
                                    function_input = lanying_openai_compat.extract_text_from_content(function_call.get('arguments', '{}'))
                                    tool_use_content.append(
                                        {
                                            "type": "tool_use",
                                            "id": function_call.get('id', ''),
                                            "name": function_call.get('name', ''),
                                            "input": json.loads(function_input)
                                        }
                                    )
                                new_message = {
                                    'role': role,
                                    'content': tool_use_content
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
                        elif role == 'tool':
                            if support_function_call:
                                function_message = {
                                    'role': 'user',
                                    'content': [
                                        {
                                            "type": "tool_result",
                                            "tool_use_id": normalized_message.get('tool_call_id', last_tool_call_id),
                                            "content": normalized_message['content']
                                        }
                                    ]
                                }
                                messages.append(function_message)
                    else:
                        logging.info(f"vendor claude ingore message in preset: {message}")
                if len(system_message) > 0:
                    ret['system'] = system_message
                ret[key] = messages
            elif key in ['functions', 'tools']:
                if support_function_call:
                    tools = []
                    for function in preset_for_tools.get('functions', []):
                        function_obj = {}
                        for k,v in function.items():
                            if k in ["name", "description", "parameters"]:
                                if k == 'parameters':
                                    function_obj['input_schema'] = v
                                else:
                                    function_obj[k] = v
                        tools.append(function_obj)
                    ret['tools'] = tools
            elif key == 'tool_choice':
                if support_function_call:
                    tool_choice = preset_for_tools.get('tool_choice')
                    if isinstance(tool_choice, dict):
                        function_name = tool_choice.get('function', {}).get('name', '')
                        if function_name != '':
                            ret['tool_choice'] = {'type': 'tool', 'name': function_name}
                    elif isinstance(tool_choice, str):
                        if tool_choice == 'required':
                            ret['tool_choice'] = {'type': 'any'}
                        elif tool_choice == 'auto':
                            ret['tool_choice'] = {'type': 'auto'}
            else:
                ret[key] = preset_for_tools[key]
        else:
            if key == 'max_tokens':
                ret[key] = 1024
    return ret

def maybe_add_proxy_headers(prepare_info, client):
    proxy_api_base = os.getenv("LANYING_CONNECTOR_CLAUDE_PROXY_API_BASE", '')
    proxy_api_key = os.getenv("LANYING_CONNECTOR_CLAUDE_PROXY_API_KEY", '')
    if len(proxy_api_base) > 0:
        client.base_url = proxy_api_base
        return {
            "Authorization": f"Basic {proxy_api_key}"
        }
    else:
        return {}

def encoding_for_model(model): # for temp
    return tiktoken.encoding_for_model("gpt-3.5-turbo")
