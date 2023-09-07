import requests
import logging
import tiktoken
import json
import lanying_redis
import hashlib

ACCESS_TOKEN_REFRESH_TIME = 86400
SYSTEM_MESSAGE_DEFAULT = '好的'

def model_configs():
    return [
        {
            "model": 'ERNIE-Bot-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 0.5,
            "token_limit": 10000,
            'order': 1,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/eb-instant'
        },
        {
            "model": 'ERNIE-Bot',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 1900,
            "token_limit_type": "prompt",
            'order': 2,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions'
        },
        {
            "model": 'Embedding-V1',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.15,
            'order': 1000,
            "token_limit": 10000
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key': auth_info['api_key'],
        'secret_key': auth_info['secret_key']
    }

def get_access_token(api_key, secret_key):
    key = access_token_key(api_key, secret_key)
    redis = lanying_redis.get_redis_connection()
    access_token = lanying_redis.redis_get(redis, key)
    if access_token:
        return access_token
    url = f"https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={api_key}&client_secret={secret_key}"
    payload = json.dumps("")
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        access_token = response.json().get("access_token")
        if access_token:
            redis.setex(key, ACCESS_TOKEN_REFRESH_TIME, access_token)
            logging.info("refresh baidu access_token: success")
        else:
            logging.info("refresh baidu access_token: fail")
        return access_token
    except Exception as e:
        logging.exception(e)
        logging.info("refresh baidu access_token: fail")
        return None

def chat(prepare_info, preset):
    api_key = prepare_info['api_key']
    secret_key = prepare_info['secret_key']
    access_token = get_access_token(api_key, secret_key)
    if access_token is None:
        logging.info("baidu chat_completion | failed to get access_token")
        return {
            'result': 'error',
            'reason': 'fail_to_get_access_token'
        }
    model_url = get_chat_model_url(preset['model'])
    url = f"{model_url}?access_token={access_token}"
    final_preset = format_preset(prepare_info, preset)
    headers = {"Content-Type": "application/json"}
    try:
        logging.info(f"baidu chat_completion start | preset={preset}, final_preset={final_preset}, access_token_len={len(access_token)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"baidu chat_completion finish | code={response.status_code}, stream={stream}")
            if response.status_code == 200:
                def generator():
                    completion_tokens = 0
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        # logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                text = data.get('result','')
                                if 'usage' in data:
                                    usage = data['usage']
                                    completion_tokens += usage.get('completion_tokens', 0)
                                    usage['completion_tokens'] = completion_tokens
                                    yield {'usage': usage, 'content': text}
                                else:
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
            logging.info(f"baidu chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            error_code = res.get('error_code')
            error_msg = res.get('error_msg')
            if error_code:
                return {
                    'result': 'error',
                    'reason': error_msg,
                    'code': error_code
                }
            usage = res.get('usage',{})
            return {
                'result': 'ok',
                'reply': res['result'],
                'usage': {
                    'completion_tokens' : usage.get('completion_tokens',0),
                    'prompt_tokens' : usage.get('prompt_tokens', 0),
                    'total_tokens' : usage.get('total_tokens', 0)
                }
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
        'secret_key': auth_info['secret_key']
    }

def embedding(prepare_info, text):
    api_key = prepare_info['api_key']
    secret_key = prepare_info['secret_key']
    access_token = get_access_token(api_key, secret_key)
    model = "Embedding-V1"
    if access_token is None:
        logging.info("baidu embedding | failed to get access_token")
        return {
            'result': 'error',
            'reason': 'fail_to_get_access_token'
        }
    url = f"https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/embeddings/embedding-v1?access_token={access_token}"
    headers = {"Content-Type": "application/json"}
    body = {
        "input": [
            text
        ]
    }
    logging.info(f"baidu embedding start | access_token_len={len(access_token)}")
    response = requests.post(url, headers=headers, json = body)
    logging.info(f"baidu embedding finish | code={response.status_code}, response={response.text}")
    try:
        res = response.json()
        error_code = res.get('error_code')
        error_msg = res.get('error_msg')
        if error_code:
            return {
                'result': 'error',
                'reason': error_msg,
                'code': error_code
            }
        embedding = res['data'][0]['embedding']
        if len(embedding) < 1536:
            embedding = embedding + [0.0 for i in range(1536-len(embedding))]
        usage = res.get('usage',{})
        return {
            'result': 'ok',
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
    if model == "Embedding-V1":
        return tiktoken.get_encoding("cl100k_base")
    else:
        return tiktoken.encoding_for_model("gpt-3.5-turbo")

def format_preset(prepare_info, preset):
    support_fields = ["messages", "temperature", "top_p", "penalty_score", "user_id", "stream"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "messages":
                messages = []
                for message in preset['messages']:
                    if 'role' in message and 'content' in message:
                        role = message['role']
                        content = message['content']
                        if len(content) > 0:
                            if role == "system":
                                messages.append({'role':'user', 'content':content})
                                messages.append({'role':'assistant', 'content':SYSTEM_MESSAGE_DEFAULT})
                            elif role == "user":
                                if len(messages) > 0 and messages[-1]['role'] == 'user':
                                    messages.append({'role':'assistant', 'content':SYSTEM_MESSAGE_DEFAULT})
                                messages.append({'role':'user', 'content':content})
                            elif role == 'assistant':
                                if len(messages) > 0 and messages[-1]['role'] == 'user':
                                    messages.append({'role':'assistant', 'content':content})
                                else:
                                    logging.info("dropping a assistant message: {message}")
                ret[key] = messages
            else:
                ret[key] = preset[key]
    return ret

def access_token_key(api_key, secret_key):
    sha_value = hashlib.sha256((api_key+secret_key).encode('utf-8')).hexdigest()
    return f"lanying-connector:baidu:access-token:{api_key}:{sha_value}"


def get_chat_model_url(model):
    for config in model_configs():
        if config['model'] == model:
            return config['url']
    return None
