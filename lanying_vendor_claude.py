import logging
import anthropic
import os
import time
import tiktoken
from anthropic.types.beta import (
    ContentBlockDeltaEvent,
    ContentBlockStartEvent,
    MessageDeltaEvent,
    MessageStartEvent,
)
ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'

def model_configs():
    return [
        {
            "model": 'claude-2.1',
            "type": "chat",
            "is_prefix": False,
            "quota": 10,
            "token_limit": 200000,
            'order': 1
        },
        {
            "model": 'claude-2.0',
            "type": "chat",
            "is_prefix": False,
            "quota": 10,
            "token_limit": 100000,
            'order': 2
        },
        {
            "model": 'claude-instant-1.2',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 100000,
            'order': 3
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset):
    client = anthropic.Anthropic(
        api_key=prepare_info['api_key']
    )
    final_preset = format_preset(preset)
    headers = maybe_add_proxy_headers(prepare_info, client)
    logging.info(f"vendor claude chat request: {final_preset}")
    retry_times = 1
    response = None
    task_id = time.time()
    for i in range(retry_times):
        logging.info(f"vendor claude start try task_id:{task_id}, {i}/{retry_times}")
        try:
            response = client.beta.messages.create(**final_preset, extra_headers = headers)
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
            for chunk in response:
                # logging.info(f"vendor claude chunk: {chunk}")
                chunk_reply = {}
                if isinstance(chunk, ContentBlockStartEvent):
                    if chunk.content_block.type == 'text':
                        content = chunk.content_block.text
                        chunk_reply['content'] = content
                elif isinstance(chunk, ContentBlockDeltaEvent):
                    if chunk.delta.type == 'text_delta':
                        content = chunk.delta.text
                        chunk_reply['content'] = content
                elif isinstance(chunk, MessageDeltaEvent):
                    if chunk.usage:
                        if 'output_tokens' in chunk.usage:
                            usage['completion_tokens'] = chunk.usage['output_tokens']
                            usage['total_tokens'] = usage['prompt_tokens'] + usage['completion_tokens']
                            chunk_reply['usage'] = usage
                elif isinstance(chunk, MessageStartEvent):
                    if chunk.message.usage:
                        if 'input_tokens' in chunk.message.usage:
                            usage['prompt_tokens'] = chunk.message.usage['input_tokens']
                        if 'output_tokens' in chunk.message.usage:
                            usage['completion_tokens'] = chunk.message.usage['output_tokens']
                        usage['total_tokens'] = usage['prompt_tokens'] + usage['completion_tokens']
                        chunk_reply['usage'] = usage
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
        usage = response.usage
        reply = ''
        try:
            reply = response.content[0].text
        except Exception as ee:
            logging.exception(ee)
            pass
        if reply:
            reply = reply.strip()
        else:
            reply = ''
        return {
            'result': 'ok',
            'reply' : reply,
            'usage' : {
                'completion_tokens' : usage.get('output_tokens', 0),
                'prompt_tokens' : usage.get('input_tokens', 0),
                'total_tokens' : usage.get('input_tokens', 0) + usage.get('output_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        logging.info(f"vendor claude fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'response': response 
        }

def format_preset(preset):
    support_fields = ['system', 'model', "messages", "temperature", "top_p", "top_k", "stop_sequences", "max_tokens","stream"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "messages":
                messages = []
                system_message = ret.get('system', '')
                for message in preset['messages']:
                    if 'role' in message and 'content' in message:
                        role = message['role']
                        content = message['content']
                        if role == 'system':
                            if len(system_message) > 0:
                                system_message += "\n\n\n" + message['content']
                        elif role == "user":
                            if len(messages) > 0 and messages[-1]['role'] == 'user':
                                messages.append({'role':'assistant', 'content':ASSISTANT_MESSAGE_DEFAULT})
                            messages.append({'role': role, 'content':content})
                        elif role == 'assistant':
                            if len(messages) > 0 and messages[-1]['role'] == 'assistant':
                                messages.append({'role':'user', 'content':USER_MESSAGE_DEFAULT})
                            messages.append({'role': role, 'content':content})
                    else:
                        logging.info(f"vendor claude ingore message in preset: {message}")
                if len(system_message) > 0:
                    ret['system'] = system_message
                ret[key] = messages
            else:
                ret[key] = preset[key]
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