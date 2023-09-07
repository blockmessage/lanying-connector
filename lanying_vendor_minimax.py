import requests
import logging
import tiktoken
import json

def model_configs():
    return [
        {
            "model": 'abab5.5-chat',
            "type": "chat",
            "is_prefix": True,
            "quota": 1,
            'order': 1,
            "token_limit": 11000
        },
        {
            "model": 'embo-01',
            "type": "embedding",
            "is_prefix": True,
            "quota": 0.0005,
            'order': 1000,
            "token_limit": 11000
        }
    ]

def prepare_chat(auth_info, preset):
    bot_name = 'AI助手'
    user_name = "用户"
    if 'bot_setting' in preset:
        for setting in preset['bot_setting']:
            if 'bot_name' in setting:
                bot_name = setting['bot_name']
    if 'messages' in preset:
        messages = []
        for message in preset['messages']:
            if 'sender_type' in message and 'sender_name' in message and 'text' in message:
                if message['sender_type'] == 'USER':
                    messages.append({'role':'user', 'content': message['text']})
                    user_name = message['sender_name']
                elif message['sender_type'] == 'BOT':
                    messages.append({'role':'assistant', 'content': message['text']})
                    bot_name = message['sender_name']
                elif message['sender_type'] == 'FUNCTION':
                    messages.append({'role':'function', 'content': message['text']})
            elif 'role' in message and 'content' in message:
                messages.append(message)
        preset['messages'] = messages
    return {
        'bot_name':bot_name,
        'user_name':user_name,
        'api_key': auth_info['api_key'],
        'api_group_id': auth_info['api_group_id']
    }

# {"created":0,"model":"","reply":"","choices":null,"base_resp":{"status_code":1008,"status_msg":"insufficient balance"}}
def chat(prepare_info, preset):
    api_key = prepare_info['api_key']
    api_group_id = prepare_info['api_group_id']
    url = f"https://api.minimax.chat/v1/text/chatcompletion_pro?GroupId={api_group_id}"
    final_preset = format_preset(prepare_info, preset)
    headers = {"Content-Type": "application/json", "Authorization": api_key}
    logging.info(f"minimax chat_completion start | preset={preset}, final_preset={final_preset}, url:{url}, api_key:{api_key[:10]}...{api_key[-6:]}")
    try:
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=stream)
            logging.info(f"minimax chat_completion finish | code={response.status_code}, stream={stream}")
            if response.status_code == 200:
                def generator():
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                if 'usage' in data:
                                    yield {'usage': data['usage']}
                                else:
                                    text = data['choices'][0]['messages'][0]['text']
                                    yield {'content': text}
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
            logging.info(f"minimax chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            base_resp = res.get('base_resp',{})
            status_code = base_resp.get('status_code', 0)
            if status_code == 0:
                usage = res.get('usage',{})
                return {
                    'result': 'ok',
                    'reply': res['reply'],
                    'function_call': res.get('function_call'),
                    'usage': {
                        'completion_tokens' : usage.get('completion_tokens',0),
                        'prompt_tokens' : usage.get('prompt_tokens', 0),
                        'total_tokens' : usage.get('total_tokens', 0)
                    }
                }
            else:
                return {
                    'result': 'error',
                    'reason': base_resp.get('status_msg', ''),
                    'response': res 
                }
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to transform response:{response}")
        return {'result':'error',
                'reason':str(response.status_code),
                'response':response}

def prepare_embedding(auth_info, type):
    return {
        'api_key': auth_info['api_key'],
        'api_group_id': auth_info['api_group_id'],
        'type': type
    }

def embedding(prepare_info, text):
    api_key = prepare_info['api_key']
    api_group_id = prepare_info['api_group_id']
    type = prepare_info.get('type', 'db')
    model = "embo-01"
    url = f"https://api.minimax.chat/v1/embeddings?GroupId={api_group_id}"
    headers = {"Content-Type": "application/json", "Authorization": api_key}
    body = {
        "texts": [
            text
        ],
        "model": model,
        "type": type
    }
    logging.info(f"minimax embedding start | type={type}, url:{url}, api_key:{api_key[:10]}...{api_key[-6:]}")
    response = requests.post(url, headers=headers, json = body)
    logging.info(f"minimax embedding finish | code={response.status_code}, response={response.text}")
    try:
        res = response.json()
        embedding = res['vectors'][0]
        usage = res.get('usage',{})
        return {
            'result':'ok',
            'model': model,
            'embedding': embedding,
            'usage': {
                'completion_tokens' : usage.get('completion_tokens',0),
                'prompt_tokens' : usage.get('prompt_tokens', 0),
                'total_tokens' : usage.get('total_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'model': model,
            'response': response
        }

def encoding_for_model(model): # for temp
    if model == "embo-01":
        return tiktoken.get_encoding("cl100k_base")
    else:
        return tiktoken.encoding_for_model("gpt-3.5-turbo")

def format_preset(prepare_info, preset):
    bot_name = prepare_info['bot_name']
    user_name = prepare_info['user_name']
    support_fields = ['model', "tokens_to_generate", "temperature", "top_p", "mask_sensitive_info", "messages", "bot_setting", "reply_constraints", "functions", "stream"]
    payload = dict()
    for key in support_fields:
        if key == 'tokens_to_generate':
            if key in preset:
                payload[key] = preset[key]
            elif 'max_tokens' in preset:
                payload[key] = preset['max_tokens']
        elif key == 'messages':
            messages = []
            bot_setting = []
            for message in preset.get('messages',[]):
                if 'role' in message and 'content' in message:
                    if message['content'] != '' or 'function_call' in message:
                        if message['role'] == 'system':
                            bot_setting.append({'bot_name':bot_name, 'content': message['content']})
                        elif message['role'] == 'user':
                            messages.append({'sender_type': 'USER', 'sender_name': user_name, 'text': message['content']})
                        elif message['role'] == 'assistant':
                            if 'function_call' in message:
                                messages.append({'sender_type': 'BOT', 'sender_name' : bot_name, 'text': message['content'], 'function_call': message['function_call']})
                            else:
                                messages.append({'sender_type': 'BOT', 'sender_name' : bot_name,'text': message['content']})
                        elif message['role'] == 'function':
                            messages.append({'sender_type': 'FUNCTION', 'sender_name': message.get('name', 'FUNCTION'), 'text': message['content']})
                elif 'text' in message:
                    if message['text'] != '':
                        messages.append(message)
            payload[key] = messages
            if len(bot_setting) > 0:
                payload['bot_setting'] = maybe_merge_bot_settings(bot_setting)
        elif key == 'bot_setting':
            if key not in preset:
                if key not in payload:
                    payload[key] = [{
                        'bot_name': bot_name,
                        'content': f'你是一个{bot_name}'
                    }]
            else:
                if key not in payload:
                    payload[key] = preset[key]
                else:
                    payload[key].extend(preset[key])
                    payload[key] = maybe_merge_bot_settings(payload[key])
        elif key == 'reply_constraints':
            if key not in preset:
                payload[key] = { 
                    "sender_type": "BOT",
                    "sender_name": bot_name
                }
            else:
                payload[key] = preset[key]
        elif key in preset:
            if key == "functions":
                functions = []
                for function in preset['functions']:
                    function_obj = {}
                    for k,v in function.items():
                        if k in ["name", "description", "parameters"]:
                            function_obj[k] = v
                    functions.append(function_obj)
                payload[key] = functions
            else:
                payload[key] = preset[key]
    return payload

def maybe_merge_bot_settings(bot_setting):
    cache = {}
    for item in bot_setting:
        bot_name = item['bot_name']
        content = item['content']
        if bot_name in cache:
            cache[bot_name] = cache[bot_name] + "\n\n" + content
        else:
            cache[bot_name] = content
    new_bot_setting = []
    for k,v in cache.items():
        new_bot_setting.append({'bot_name':k, 'content':v})
    return new_bot_setting
