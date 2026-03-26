import logging
import tiktoken
import requests
import json
import copy
import lanying_openai_compat

SUPPORT_NATIVE_TOOLS = True

def model_configs():
    return [
        {
            "model": 'DeepSeek-R1',
            "service": 'deepseek',
            'real_model': 'deepseek-ai/DeepSeek-R1',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.26,
            "token_limit": 128000,
            'order': 1,
            'max_output_tokens': 64000,
            'function_call': False
        },
        {
            "model": 'DeepSeek-V3',
            "service": 'deepseek',
            'real_model': 'deepseek-ai/DeepSeek-V3',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.74,
            "token_limit": 128000,
            'order': 2,
            'max_output_tokens': 8000,
            'function_call': False
        },
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset, model_config):
    real_model = model_config.get('real_model', None)
    url = 'https://api.siliconflow.cn/v1/chat/completions'
    final_preset = format_preset(preset, model_config)
    if real_model:
        final_preset['model'] = real_model
    api_key = prepare_info["api_key"]
    headers = {"Content-Type": "application/json", "Authorization": f'Bearer {api_key}'}
    try:
        logging.info(f"siliconflow chat_completion start | preset={preset}, url:{url}")
        logging.info(f"siliconflow chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"siliconflow chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        # logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                if 'choices' in data and len(data['choices']) > 0:
                                    choice = data['choices'][0]
                                    delta = choice['delta']
                                    if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                        delta['finish_reason'] = choice['finish_reason']
                                else:
                                    delta = {'content': ''}
                                if 'usage' in data:
                                    delta['usage'] = data['usage']
                                logging.info(f"yield delta:{delta}")
                                yield delta
                            except Exception as e:
                                pass
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
                logging.info(f"fail to get stream: response:{response.text}")
                response_json = {}
                try:
                    response_json = response.json()
                    return {
                        'result': 'error',
                        'reason': response_json['message'],
                        'code': response_json['code'],
                        'response': response_json
                    }
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'response': response_json,
                    'status_code': response.status_code
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset)
            logging.info(f"siliconflow chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            if response.status_code != 200:
                return {
                    'result': 'error',
                    'reason': res['message'],
                    'code': res['code']
                }
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
    support_fields = ['model', 'messages', 'frequency_penalty', 'max_tokens', 'presence_penalty', 'stop', 'stream', 'temperature', 'top_p', 'logprobs', 'top_logprobs', 'logit_bias', 'tools', 'tool_choice']
    function_call_support = model_config.get('function_call', False)
    logging.info(f"function_call_support: {function_call_support}")
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
            else:
                if key in ['tools', 'tool_choice'] and not function_call_support:
                    continue
                ret[key] = preset[key]
    return ret

def encoding_for_model(model):
    return tiktoken.encoding_for_model("gpt-3.5-turbo")
