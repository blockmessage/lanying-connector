import logging
import tiktoken
import requests
import json
import copy
import lanying_openai_compat

SUPPORT_NATIVE_TOOLS = True


def _chat_model_config(model, quota, token_limit, max_output_tokens, **kwargs):
    config = {
        "model": model,
        "is_origin_vendor": True,
        "service": 'qwen',
        "type": "chat",
        "is_prefix": False,
        "quota": quota,
        "token_limit": token_limit,
        'max_output_tokens': max_output_tokens
    }
    config.update(kwargs)
    return config


def _priced_chat_model_config(model, quota, input_price, output_price, token_limit, max_output_tokens, currency='CNY', **kwargs):
    config = _chat_model_config(model, quota, token_limit, max_output_tokens, **kwargs)
    config['input_price'] = float(input_price)
    config['output_price'] = float(output_price)
    config['currency'] = str(currency or 'CNY').upper()
    return config

def show_models():
    for model in model_configs():
        print(f'{model["model"]}\t{model["input_price"]}\t{model["output_price"]}')

def model_configs():
    return [
        _priced_chat_model_config(
            'qwen3.5-flash',
            0.8,
            0.0012,
            0.012,
            1000000,
            32768,
            is_default=True,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen3.6-plus',
            2.89,
            0.008,
            0.048,
            1000000,
            65536,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen3-max',
            2.04,
            0.007,
            0.028,
            262144,
            65536,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen-flash',
            0.8,
            0.0012,
            0.012,
            1000000,
            32768,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen-plus',
            3.13,
            0.0048,
            0.064,
            1000000,
            32768,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen-max',
            0.84,
            0.0024,
            0.0096,
            32768,
            8192,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen-plus-latest',
            3.13,
            0.0048,
            0.064,
            1000000,
            32768,
            reasoning=True,
            function_call=True
        ),
        _priced_chat_model_config(
            'qwen-max-latest',
            0.84,
            0.0024,
            0.0096,
            131072,
            8192,
            function_call=True
        )
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset, model_config):
    url = 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions'
    final_preset = format_preset(preset)
    api_key = prepare_info["api_key"]
    headers = {"Content-Type": "application/json", "Authorization": f'Bearer {api_key}'}
    try:
        logging.info(f"aliyun chat_completion start | preset={preset}, url:{url}")
        logging.info(f"aliyun chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"aliyun chat_completion finish | code={response.status_code}, stream:{stream}")
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
                                if 'usage' in data and isinstance(data['usage'], dict):
                                    delta['usage'] = data['usage']
                                # logging.info(f"yield delta:{delta}")
                                if 'usage' in delta:
                                    logging.info(f"yield usage delta:{delta}")
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
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'response': response_json
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset)
            logging.info(f"aliyun chat_completion finish | code={response.status_code}, response={response.text}")
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

def format_preset(preset):
    support_fields = ['model', 'messages', 'frequency_penalty', 'max_tokens', 'presence_penalty', 'stop', 'stream', 'temperature', 'top_p', 'tools', 'tool_choice']
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
            elif key == 'top_p':
                if preset[key] >= 1:
                    ret[key] = 0.9
                elif preset[key] <= 0:
                    ret[key] = 0.1
                else:
                    ret[key] = preset[key]
            elif key == 'stream' and preset[key] == True:
                ret['stream_options'] = {'include_usage':True}
                ret[key] = preset[key]
            else:
                ret[key] = preset[key]
    return ret

def encoding_for_model(model):
    return tiktoken.encoding_for_model("gpt-3.5-turbo")
