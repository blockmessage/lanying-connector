import logging
import tiktoken
import requests
import json
import copy

def model_configs():
    return [
        {
            "model": 'moonshot-v1-8k',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.8,
            "token_limit": 8000,
            'order': 1,
            'function_call': True
        },
        {
            "model": 'moonshot-v1-32k',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.6,
            "token_limit": 32000,
            'order': 2,
            'function_call': True
        },
        {
            "model": 'moonshot-v1-128k',
            "type": "chat",
            "is_prefix": False,
            "quota": 4,
            "token_limit": 128000,
            'order': 3,
            'function_call': True
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key' : auth_info['api_key']
    }

def chat(prepare_info, preset):
    url = 'https://api.moonshot.cn/v1/chat/completions'
    final_preset = format_preset(preset)
    api_key = prepare_info["api_key"]
    headers = {"Content-Type": "application/json", "Authorization": f'Bearer {api_key}'}
    try:
        logging.info(f"moonshot chat_completion start | preset={preset}, url:{url}")
        logging.info(f"moonshot chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"moonshot chat_completion finish | code={response.status_code}, stream:{stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                if 'choices' in data and len(data['choices']) > 0:
                                    choice = data['choices'][0]
                                    delta = choice['delta']
                                    if 'finish_reason' in choice and choice['finish_reason'] is not None:
                                        delta['finish_reason'] = choice['finish_reason']
                                    if 'tool_calls'in delta and isinstance(delta['tool_calls'], list) and len(delta['tool_calls']) > 0:
                                        tool_calls = delta['tool_calls']
                                        function_call = {}
                                        if 'name' in tool_calls[0]['function']:
                                            function_call['name'] = tool_calls[0]['function']['name']
                                        if 'arguments' in tool_calls[0]['function']:
                                            function_call['arguments'] = tool_calls[0]['function']['arguments']
                                        if 'id' in tool_calls[0]:
                                            function_call['id'] = tool_calls[0]['id']
                                        delta['function_call'] = function_call
                                    if 'usage' in choice and isinstance(choice['usage'], dict):
                                        delta['usage'] = choice['usage']
                                else:
                                    delta = {'content': ''}
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
                        'reason': response_json['error']['message'],
                        'code': response_json['error']['type']
                    }
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'response': response_json
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset)
            logging.info(f"moonshot chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            if response.status_code != 200:
                return {
                    'result': 'error',
                    'reason': res['error']['message'],
                    'code': res['error']['type']
                }
            usage = res.get('usage',{})
            response_message = res['choices'][0]['message']
            reply = response_message.get('content', "")
            if reply:
                reply = reply.strip()
            else:
                reply = ''
            function_call = None
            if 'tool_calls' in response_message and isinstance(response_message['tool_calls'], list) and len(response_message['tool_calls']) > 0:
                try:
                    function_call = {
                        'name': response_message['tool_calls'][0]['function']['name'],
                        'arguments': response_message['tool_calls'][0]['function']['arguments'],
                        'id': response_message['tool_calls'][0]['id']
                    }
                except Exception as e:
                    logging.error(f"fail to parse moonshot function call {response_message}")
                    logging.exception(e)
            finish_reason = ''
            try:
                finish_reason = res['choices'][0]['finish_reason']
            except Exception as e:
                pass
            return {
                'result': 'ok',
                'reply' : reply,
                'finish_reason': finish_reason,
                'function_call': function_call,
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
    support_fields = ['model', 'messages', 'frequency_penalty', 'max_tokens', 'presence_penalty', 'stop', 'stream', 'temperature', 'top_p', 'logprobs', 'top_logprobs', 'logit_bias', 'functions']
    function_call_support = get_chat_model_function_call(preset['model'])
    logging.info(f"function_call_support: {function_call_support}")
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "functions":
                if function_call_support:
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
                    isFunction = False
                    if 'function_call' in message:
                        message = copy.deepcopy(message)
                        function_call = message['function_call']
                        last_tool_call_id = function_call.get('id', '')
                        tool_calls = [{
                            'id': function_call.get('id', ''),
                            'type':'function',
                            'function':{
                                'name': function_call['name'],
                                'arguments': function_call['arguments']
                            }}]
                        message['tool_calls'] = tool_calls
                        del message['function_call']
                        isFunction = True
                    elif message['role'] == 'function':
                        message = copy.deepcopy(message)
                        message['role'] = 'tool'
                        message['tool_call_id'] = last_tool_call_id
                        isFunction = True
                    if isFunction:
                        if function_call_support:
                            messages.append(message)
                    else:
                        messages.append(message)
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

def get_chat_model_function_call(model):
    for config in model_configs():
        if model == config['model']:
            return config['function_call']
    return False
