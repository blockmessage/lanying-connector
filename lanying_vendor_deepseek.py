import logging
import tiktoken
import requests
import json
import lanying_openai_compat

ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'
SUPPORT_NATIVE_TOOLS = True

def model_configs():
    return [
        {
            "model": 'deepseek-reasoner',
            'model_show_name': 'deepseek-reasoner (R1)',
            "is_origin_vendor": True,
            "service": 'deepseek',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.26,
            "token_limit": 128000,
            'order': 1,
            'max_output_tokens': 64000,
            'function_call': False
        },
        {
            "model": 'deepseek-chat',
            'model_show_name': 'deepseek-reasoner (V3)',
            "is_origin_vendor": True,
            "service": 'deepseek',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.74,
            "token_limit": 128000,
            'order': 2,
            'max_output_tokens': 8000,
            'function_call': False
        },
        {
            "model": 'deepseek-coder',
            "is_origin_vendor": True,
            "service": 'deepseek',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.52,
            "token_limit": 64000,
            'order': 10,
            'hidden': True,
            'max_output_tokens': 8000,
            'function_call': False
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset, model_config):
    url = 'https://api.deepseek.com/chat/completions'
    final_preset = format_preset(preset, model_config)
    api_key = prepare_info["api_key"]
    headers = {"Content-Type": "application/json", "Authorization": f'Bearer {api_key}'}
    try:
        logging.info(f"deepseek chat_completion start | preset={preset}, url:{url}")
        logging.info(f"deepseek chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"deepseek chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                choice = data['choices'][0]
                                delta = choice['delta']
                                if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                    delta['finish_reason'] = choice['finish_reason']
                                if 'usage' in data:
                                    delta['usage'] = data['usage']
                                logging.info(f"yield delta:{delta}")
                                yield delta
                            except Exception as e:
                                pass
                first_data = None
                old_generator = generator()
                for delta in old_generator:
                    first_data = delta  # 获取第一个数据
                    break  # 只取第一个数据

                if first_data is None:
                    return {
                        'result': 'error',
                        'reason': 'empty_reply'
                    }

                # 如果有数据，则创建一个新的 generator，包含第一个数据并继续后续的数据
                def new_generator():
                    yield first_data  # 先返回第一个数据
                    yield from old_generator  # 然后返回剩余的 generator 数据

                return {
                    'result': 'ok',
                    'reply' : '',
                    'reply_generator': new_generator(),
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
            logging.info(f"deepseek chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            usage = res.get('usage',{})
            response_message = res['choices'][0]['message']
            reply = response_message.get('content', "")
            if reply:
                reply = reply.strip()
            else:
                reply = ''
            tool_calls = response_message.get('tool_calls', [])
            finish_reason = ''
            try:
                finish_reason = res['choices'][0]['finish_reason']
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'finish_reason': finish_reason,
                'tool_calls': tool_calls,
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

def format_preset(preset, model_config):
    support_fields = ['model', 'messages', 'frequency_penalty', 'max_tokens', 'presence_penalty', 'stop', 'stream', 'temperature', 'top_p', 'logprobs', 'top_logprobs', 'tools', 'tool_choice']
    function_call_support = model_config.get('function_call', False)
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
                    if 'role' in message and 'content' in message:
                        role = message['role']
                        content = message['content']
                        if role == 'system':
                            new_message = {
                                'role': role,
                                'content': content
                            }
                            messages.append(new_message)
                        elif role == "user":
                            if len(messages) > 0 and messages[-1]['role'] == 'user':
                                messages.append({'role':'assistant', 'content':ASSISTANT_MESSAGE_DEFAULT})
                            messages.append({'role': role, 'content':content})
                        elif role == 'assistant':
                            new_message = {
                                'role': role,
                                'content': content
                            }
                            if len(new_message['content']) > 0:
                                if len(messages) > 0 and messages[-1]['role'] == 'assistant':
                                    messages.append({'role':'user', 'content':USER_MESSAGE_DEFAULT})
                                messages.append(new_message)
                ret['messages'] = messages
            else:
                if key in ['tools', 'tool_choice'] and not function_call_support:
                    continue
                ret[key] = preset[key]
    return ret

def encoding_for_model(model):
    return tiktoken.encoding_for_model("gpt-3.5-turbo")
