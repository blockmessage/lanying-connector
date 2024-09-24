import xml.etree.ElementTree as ET
import logging
from flask import Blueprint, request, make_response
import lanying_config
import hashlib
import lanying_redis
import requests
import random
import string
import json
import lanying_message
import os
import time
import lanying_im_api
import lanying_utils
import lanying_user_router
import uuid
import lanying_audio
from lanying_async import executor

official_account_max_message_size = 600
service = 'wechat_official_account'
bp = Blueprint(service, __name__)

@bp.route("/service/wechat_official_account/app/<string:app_id>/messages", methods=["GET"])
@bp.route("/wechat/official/messages/<string:app_id>", methods=["GET"])
def service_get_messages(app_id):
    logging.info(f"app_id:{app_id}, args:{request.args.to_dict()}")
    if check_token(app_id):
        echostr = request.args.get('echostr')
        resp = make_response(echostr)
        return resp
    return 'bad_signature'

@bp.route("/service/wechat_official_account/app/<string:app_id>/messages", methods=["POST"])
@bp.route("/wechat/official/messages/<string:app_id>", methods=["POST"])
def service_post_messages(app_id):
    start_time = time.time()
    xml_data = request.data
    reply = 'failed'
    logging.info(f"app_id:{app_id}, xml_data:{xml_data}, request.args:{request.args.to_dict()}, headers:{request.headers.to_wsgi_list()}")
    if check_token(app_id):
        config = lanying_config.get_service_config(app_id, service)
        if config:
            xml = ET.fromstring(xml_data)
            msg_type = xml.find('MsgType').text
            if msg_type == 'text':
                reply = handle_wechat_msg_text(xml, config, app_id, start_time)
            elif msg_type == 'image':
                reply = handle_wechat_msg_image(xml, config, app_id, start_time)
            elif msg_type == 'voice':
                reply = handle_wechat_msg_voice(xml, config, app_id, start_time)
            elif msg_type == 'event':
                reply = handle_wechat_msg_event(xml, config, app_id, start_time)
            else:
                reply = 'success'
        else:
            logging.info(f"config not found:{app_id}, {service}")
    logging.info(f"handle_wechat_message: reply to wechat:{reply}")
    resp = make_response(reply)
    return resp

def handle_wechat_msg_text(xml, config, app_id, start_time):
    reply = 'failed'
    to_user_name = xml.find('ToUserName').text
    from_user_name = xml.find('FromUserName').text
    create_time = xml.find('CreateTime').text
    content = xml.find('Content').text
    msg_id = int(xml.find('MsgId').text)
    verify_type = config.get('type', 'verified')
    logging.info(f"got wechat text message | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time},content:{content},msg_id:{msg_id}, verify_type:{verify_type}")
    user_id = get_or_register_user(config, app_id, from_user_name)
    if user_id:
        if verify_type == 'unverified':
            reply_expire_time = start_time + int(os.getenv("WECHAT_OFFICIAL_ACCOUNT_REPLY_EXPIRE_TIME", "3"))
            last_msg_id_key = f"wechat_official_account:last_msg_id:{user_id}"
            key = subscribe_key(user_id, msg_id)
            redis = lanying_redis.get_redis_connection()
            keys = [key]
            if redis.exists(*keys) == 0:
                redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                last_msg_id_str = lanying_redis.redis_get(redis, last_msg_id_key)
                if content == "1" and last_msg_id_str:
                    redis.hset(key, 'watch_msg_id', last_msg_id_str)
                    key = subscribe_key(user_id, int(last_msg_id_str))
                    lock_value = redis.hincrby(key, 'lock', 1)
                    reply = wait_reply_msg(app_id, key, reply_expire_time, False, lock_value)
                else:
                    redis.set(last_msg_id_key, msg_id)
                    ext = {
                        'ai':{
                            'role':'user',
                            'channel':'wechat_official_account',
                            'feedback':{
                                'wechat_msg_id':msg_id
                            },
                            'force_stream': True
                        }
                    }
                    lanying_message.send_message_async(config, app_id, user_id, config['lanying_user_id'],content, ext)
                    lock_value = redis.hincrby(key, 'lock', 1)
                    reply = wait_reply_msg(app_id, key, reply_expire_time, False, lock_value)
            else:
                retry_count = redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                watch_msg_id_str = lanying_redis.redis_hget(redis, key, 'watch_msg_id')
                if watch_msg_id_str:
                    key = subscribe_key(user_id, int(watch_msg_id_str))
                lock_value = redis.hincrby(key, 'lock', 1)
                reply = wait_reply_msg(app_id, key, reply_expire_time, retry_count >=3, lock_value)
            reply_xml = transform_reply_to_xml(reply, from_user_name, to_user_name)
            if len(reply_xml) > 0:
                return reply_xml
        else:
            from_user_id = user_id
            to_user_id = config['lanying_user_id']
            router_sub_user_ids = config.get('router_sub_user_ids', [])
            router_res = lanying_user_router.handle_msg_route_to_im(app_id, service, from_user_id, to_user_id, router_sub_user_ids)
            if router_res['result'] == 'ok':
                msg_ext = {'ai':{'role':'user', 'channel':'wechat_official_account'}}
                if router_res['msg_type'] == 'CHAT':
                    lanying_message.send_message_async(config, app_id, router_res['from'], router_res['to'], content, msg_ext)
                else:
                    logging.info(f"handle_wechat_msg_text receive groupchat | router_res:{router_res}")
            reply = 'success'
    else:
        logging.info(f"failed to get user_id | app_id:{app_id}, username:{from_user_name}")
    return reply

def handle_wechat_msg_image(xml, config, app_id, start_time):
    reply = 'failed'
    to_user_name = xml.find('ToUserName').text
    from_user_name = xml.find('FromUserName').text
    create_time = xml.find('CreateTime').text
    pic_url = xml.find('PicUrl').text
    msg_id = int(xml.find('MsgId').text)
    verify_type = config.get('type', 'verified')
    logging.info(f"got wechat image message | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time},pic_url:{pic_url},msg_id:{msg_id}, verify_type:{verify_type}")
    user_id = get_or_register_user(config, app_id, from_user_name)
    if user_id:
        if verify_type == 'unverified':
            reply_expire_time = start_time + int(os.getenv("WECHAT_OFFICIAL_ACCOUNT_REPLY_EXPIRE_TIME", "3"))
            last_msg_id_key = f"wechat_official_account:last_msg_id:{user_id}"
            key = subscribe_key(user_id, msg_id)
            redis = lanying_redis.get_redis_connection()
            keys = [key]
            if redis.exists(*keys) == 0:
                redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                redis.set(last_msg_id_key, msg_id)
                ext = {
                    'ai':{
                        'role':'user',
                        'channel':'wechat_official_account',
                        'feedback':{
                            'wechat_msg_id':msg_id
                        },
                        'force_stream': False
                    }
                }
                attachment = {
                    'url': pic_url,
                    'fLen': 0
                }
                to_user_id = config['lanying_user_id']
                file_type = 102
                extra = {
                    'ext': ext,
                    'attachment': attachment,
                    'download_args': [app_id, user_id, pic_url, 'png', file_type, 1, to_user_id]
                }
                lanying_im_api.send_message_async(config, app_id, user_id, to_user_id, 1, 1, '', extra)
                lock_value = redis.hincrby(key, 'lock', 1)
                reply = wait_reply_msg(app_id, key, reply_expire_time, False, lock_value)
            else:
                retry_count = redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                watch_msg_id_str = lanying_redis.redis_hget(redis, key, 'watch_msg_id')
                if watch_msg_id_str:
                    key = subscribe_key(user_id, int(watch_msg_id_str))
                lock_value = redis.hincrby(key, 'lock', 1)
                reply = wait_reply_msg(app_id, key, reply_expire_time, retry_count >=3, lock_value)
            reply_xml = transform_reply_to_xml(reply, from_user_name, to_user_name)
            if len(reply_xml) > 0:
                return reply_xml
        else:
            from_user_id = user_id
            to_user_id = config['lanying_user_id']
            router_sub_user_ids = config.get('router_sub_user_ids', [])
            router_res = lanying_user_router.handle_msg_route_to_im(app_id, service, from_user_id, to_user_id, router_sub_user_ids)
            if router_res['result'] == 'ok':
                msg_ext = {'ai':{'role':'user', 'channel':'wechat_official_account'}}
                if router_res['msg_type'] == 'CHAT':
                    attachment = {
                    'url': pic_url,
                    'fLen': 0
                    }
                    file_type = 102
                    extra = {
                        'ext': msg_ext,
                        'attachment': attachment,
                        'download_args': [app_id, router_res['from'], pic_url, 'png', file_type, 1, router_res['to']]
                    }
                    lanying_im_api.send_message_async(config, app_id, router_res['from'], router_res['to'], 1, 1, '', extra)
                else:
                    logging.info(f"handle_wechat_msg_text receive groupchat | router_res:{router_res}")
            reply = 'success'
    else:
        logging.info(f"failed to get user_id | app_id:{app_id}, username:{from_user_name}")
    return reply

def handle_wechat_msg_voice(xml, config, app_id, start_time):
    reply = 'failed'
    to_user_name = xml.find('ToUserName').text
    from_user_name = xml.find('FromUserName').text
    create_time = xml.find('CreateTime').text
    media_id = xml.find('MediaId').text
    media_id_16k = xml.find('MediaId16K').text
    format = xml.find('Format').text
    msg_id = int(xml.find('MsgId').text)
    verify_type = config.get('type', 'verified')
    logging.info(f"got wechat image message | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time},media_id:{media_id}, media_id_16k:{media_id_16k}, format:{format},msg_id:{msg_id}, verify_type:{verify_type}")
    user_id = get_or_register_user(config, app_id, from_user_name)
    amr_filename = lanying_utils.get_temp_filename(app_id, ".amr")
    filename = lanying_utils.get_temp_filename(app_id, ".mp3")
    get_wechat_media(config, app_id, media_id_16k, amr_filename)
    lanying_audio.amr_to_mp3(amr_filename, filename)
    if user_id:
        if verify_type == 'unverified':
            reply_expire_time = start_time + int(os.getenv("WECHAT_OFFICIAL_ACCOUNT_REPLY_EXPIRE_TIME", "3"))
            last_msg_id_key = f"wechat_official_account:last_msg_id:{user_id}"
            key = subscribe_key(user_id, msg_id)
            redis = lanying_redis.get_redis_connection()
            keys = [key]
            if redis.exists(*keys) == 0:
                redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                redis.set(last_msg_id_key, msg_id)
                ext = {
                    'ai':{
                        'role':'user',
                        'channel':'wechat_official_account',
                        'feedback':{
                            'wechat_msg_id':msg_id
                        },
                        'force_stream': False
                    }
                }
                file_size = os.path.getsize(filename)
                duration = lanying_audio.get_duration(filename)
                attachment = {
                    'dName': 'voice',
                    "fLen": file_size,
                    'duration': duration
                }
                to_user_id = config['lanying_user_id']
                file_type = 104
                upload_res = lanying_im_api.upload_chat_file(app_id, user_id, 'mp3', 'audio/mp3', file_type, 1, to_user_id, filename)
                if upload_res['result'] == 'ok':
                    download_url = upload_res['url']
                    attachment['url'] = download_url
                    extra = {
                        'ext': ext,
                        'attachment': attachment
                    }
                    lanying_im_api.send_message_async(config, app_id, user_id, to_user_id, 1, 2, '', extra)
                    lock_value = redis.hincrby(key, 'lock', 1)
                    reply = wait_reply_msg(app_id, key, reply_expire_time, False, lock_value)
            else:
                retry_count = redis.hincrby(key, "retry_count", 1)
                redis.expire(key, 600)
                watch_msg_id_str = lanying_redis.redis_hget(redis, key, 'watch_msg_id')
                if watch_msg_id_str:
                    key = subscribe_key(user_id, int(watch_msg_id_str))
                lock_value = redis.hincrby(key, 'lock', 1)
                reply = wait_reply_msg(app_id, key, reply_expire_time, retry_count >=3, lock_value)
            reply_xml = transform_reply_to_xml(reply, from_user_name, to_user_name)
            if len(reply_xml) > 0:
                return reply_xml
        else:
            from_user_id = user_id
            to_user_id = config['lanying_user_id']
            router_sub_user_ids = config.get('router_sub_user_ids', [])
            router_res = lanying_user_router.handle_msg_route_to_im(app_id, service, from_user_id, to_user_id, router_sub_user_ids)
            if router_res['result'] == 'ok':
                msg_ext = {'ai':{'role':'user', 'channel':'wechat_official_account'}}
                if router_res['msg_type'] == 'CHAT':
                    file_size = os.path.getsize(filename)
                    duration = lanying_audio.get_duration(filename)
                    attachment = {
                        'dName': 'voice',
                        "fLen": file_size,
                        'duration': duration
                    }
                    file_type = 104
                    upload_res = lanying_im_api.upload_chat_file(app_id, router_res['from'], 'mp3', 'audio/mp3', file_type, 1, router_res['to'], filename)
                    if upload_res['result'] == 'ok':
                        download_url = upload_res['url']
                        attachment['url'] = download_url
                        extra = {
                            'ext': msg_ext,
                            'attachment': attachment
                        }
                        lanying_im_api.send_message_async(config, app_id, router_res['from'], router_res['to'], 1, 2, '', extra)
                else:
                    logging.info(f"handle_wechat_msg_text receive groupchat | router_res:{router_res}")
            reply = 'success'
    else:
        logging.info(f"failed to get user_id | app_id:{app_id}, username:{from_user_name}")
    return reply

def transform_reply_to_xml(reply, from_user_name, to_user_name):
    if isinstance(reply, dict):
        if reply['result'] == 'ok':
            content_type = reply.get('content_type', 'TEXT')
            if content_type == 'TEXT':
                content = reply.get('content', '')
                return f"""<xml>
                <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                <CreateTime>{int(time.time())}</CreateTime>
                <MsgType><![CDATA[text]]></MsgType>
                <Content><![CDATA[{content}]]></Content>
                </xml>
                """
            elif content_type == 'AUDIO':
                media_id = reply.get('media_id')
                return f"""<xml>
                <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                <CreateTime>{int(time.time())}</CreateTime>
                <MsgType><![CDATA[voice]]></MsgType>
                <Voice>
                    <MediaId><![CDATA[{media_id}]]></MediaId>
                </Voice>
                </xml>
                """
            elif content_type == 'IMAGE':
                media_id = reply.get('media_id')
                return f"""<xml>
                <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                <CreateTime>{int(time.time())}</CreateTime>
                <MsgType><![CDATA[image]]></MsgType>
                <Image>
                    <MediaId><![CDATA[{media_id}]]></MediaId>
                </Image>
                </xml>
                """
    elif len(reply) > 0:
        return f"""<xml>
                <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                <CreateTime>{int(time.time())}</CreateTime>
                <MsgType><![CDATA[text]]></MsgType>
                <Content><![CDATA[{reply}]]></Content>
                </xml>
                """
    return ""

def handle_wechat_msg_event(xml, config, app_id, start_time):
    to_user_name = xml.find('ToUserName').text
    from_user_name = xml.find('FromUserName').text
    create_time = xml.find('CreateTime').text
    event = xml.find('Event').text
    logging.info(f"got wechat event message | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time}, event:{event}")
    if event == 'subscribe':
        welcome_message = ''
        try:
            profile = lanying_im_api.get_user_profile_with_token(app_id, config['lanying_user_id'], config['lanying_admin_token'])
            private_info = lanying_utils.safe_json_loads(profile['data'].get('private_info', '{}'))
            welcome_message = private_info.get('welcome_message', '')
        except Exception as e:
            logging.exception(e)
        if len(welcome_message) > 0:
            logging.info(f"reply welcome msg | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name}, msg:{welcome_message}")
            return f"""<xml>
                    <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                    <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                    <CreateTime>{int(time.time())}</CreateTime>
                    <MsgType><![CDATA[text]]></MsgType>
                    <Content><![CDATA[{welcome_message}]]></Content>
                    </xml>
                    """
    return 'success'

def subscribe_key(user_id, msg_id):
    return f"wechat_official_account:subscribe:{user_id}:{msg_id}"

def wait_reply_msg(app_id, key, expire_time, is_last, lock_value):
    redis = lanying_redis.get_redis_connection()
    now = time.time()
    info = {}
    tip = '...（消息超长，回复1继续接收）'
    while now < expire_time:
        info = lanying_redis.redis_hgetall(redis, key)
        if int(info.get('lock', '0')) != lock_value:
            break
        now = time.time()
        if now > expire_time + 0.2:
            break
        message = info.get('message', '')
        start = int(info.get('start', '0'))
        if int(info.get('finish', '0')) > 0:
            content_type = info.get('content_type', 'TEXT')
            if content_type == 'TEXT':
                message_len = len(message)
                if message_len > start + official_account_max_message_size:
                    send_len = official_account_max_message_size - len(tip)
                    redis.hset(key, 'start', start+send_len)
                    reply = message[start:start+send_len] + tip
                    logging.info(f"reply wechat finish part message | {reply}")
                    return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
                elif message_len > start:
                    redis.hset(key, 'start', message_len)
                    reply = message[start:message_len]
                    logging.info(f"reply wechat finish last part message | {reply}")
                    return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
                else:
                    reply = '没有更多消息'
                    logging.info(f"reply wechat nomore message | {reply}")
                    return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
            elif content_type == 'AUDIO':
                if info.get('media_finish', '0') == '0':
                    redis.hset(key, 'media_finish', 1)
                    return {'result': 'ok', 'content_type': 'AUDIO', 'media_id':info.get('media_id')}
                else:
                    reply = '没有更多消息'
                    logging.info(f"reply wechat nomore message | {reply}")
                    return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
            elif content_type == 'IMAGE':
                if info.get('media_finish', '0') == '0':
                    redis.hset(key, 'media_finish', 1)
                    return {'result': 'ok', 'content_type': 'IMAGE', 'media_id':info.get('media_id')}
                else:
                    reply = '没有更多消息'
                    logging.info(f"reply wechat nomore message | {reply}")
                    return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
        else:
            message_len = len(message)
            if message_len > start + official_account_max_message_size:
                send_len = official_account_max_message_size - len(tip)
                redis.hset(key, 'start', start+send_len)
                reply = message[start:start+send_len] + tip
                logging.info(f"reply wechat unfinish part message | {reply}")
                return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
        now = time.time()
        time.sleep(min(max(0,expire_time-now), 200))
        now = time.time()
    if is_last:
        message = info.get('message', '')
        message_len = len(message)
        start = int(info.get('start', '0'))
        send_len = min(official_account_max_message_size - len(tip), max(0, message_len - start))
        if send_len > 0:
            redis.hset(key, 'start', start+send_len)
            reply = message[start:start+send_len] + tip
            logging.info(f"reply wechat getmore message | {reply}")
        else:
            no_content_count = int(info.get('no_content_count', '0'))
            if no_content_count < 3:
                redis.hincrby(key, 'no_content_count', 1)
                reply = '服务处理中，回复1继续接收'
            else:
                reply = lanying_config.get_message_404(app_id)
            logging.info(f"reply wechat 404 message | {reply}")
            #redis.hset(key, 'start', start+len(reply))
            #redis.hset(key, 'finish', 1)
        return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}
    else:
        time.sleep(6)
        reply = 'wait'
        return {'result': 'ok', 'content_type': 'TEXT', 'content':reply}

def handle_chat_message(config, message):
    from_user_id = message['from']['uid']
    to_user_id = message['to']['uid']
    app_id = message['appId']
    msg_type = message['type']
    router_res = lanying_user_router.handle_msg_route_from_im(app_id, service, from_user_id, to_user_id, msg_type)
    if router_res['result'] == 'error':
        logging.info(f"handle_chat_message skip with message: {router_res['message']}")
        return
    message['from']['uid'] = router_res['from']
    message['to']['uid'] = router_res['to']
    message['type'] = router_res['msg_type']
    checkres = check_message_need_send(config, message)
    if checkres['result'] == 'error':
        return
    msg_type = checkres['msg_type']
    json_ext = checkres['json_ext']
    verify_type = checkres['verify_type']
    is_stream = checkres['is_stream']
    app_id = message['appId']
    to_user_id = message['to']['uid']
    logging.info(f"{service} | handle_chat_message do for user_id, app_id={app_id}, to_user_id:{to_user_id}, verify_type:{verify_type}")
    if verify_type == 'unverified':
        redis = lanying_redis.get_redis_connection()
        wechat_msg_id = 0
        try:
            wechat_msg_id = int(json_ext['ai']['feedback']['wechat_msg_id'])
        except Exception as e:
            pass
        if wechat_msg_id > 0:
            key = subscribe_key(to_user_id, wechat_msg_id)
            sub_info = lanying_redis.redis_hgetall(redis, key)
            ai_info = json_ext.get('ai',{})
            is_image_description = bool(ai_info.get("is_image_description", False))
            if is_stream:
                old_seq = int(sub_info.get('seq', '-1'))
                seq = int(ai_info.get('seq', '0'))
                if seq > old_seq and not is_image_description:
                    message_content = sub_info.get('message','')
                    if msg_type == 'CHAT' or msg_type == 'APPEND':
                        message_content += message['content']
                    elif msg_type == 'REPLACE':
                        message_content = message['content']
                        message_antispam = lanying_config.get_message_antispam(app_id)
                        if message_content == message_antispam:
                            redis.hset(key, "start", 0)
                    redis.hset(key, "message", message_content)
                    if ai_info.get('finish', False) == True:
                        redis.hset(key, 'finish', 1)
            else:
                message_content = sub_info.get('message','')
                if msg_type == 'CHAT':
                    content_type = message.get('ctype', 'TEXT')
                    if content_type == 'TEXT':
                        if not is_image_description:
                            message_content = message['content']
                            redis.hmset(key, {
                                "content_type": content_type,
                                "message": message_content,
                                "finish": 1
                            })
                    elif content_type == 'AUDIO':
                        if not is_image_description:
                            message_content = message['content']
                            attachment = lanying_utils.safe_json_loads(message.get('attachment', ''))
                            attachment_url = attachment['url']
                            user_id = message['from']['uid']
                            extra = {'format': 'mp3'}
                            filename = lanying_utils.get_temp_filename(app_id, ".mp3")
                            result = lanying_im_api.download_url(config, app_id, user_id, attachment_url, filename, extra)
                            if result['result'] == 'ok':
                                result = upload_wechat_media(config, app_id, 'voice', filename)
                                if result['result'] == 'ok':
                                    media_id = result['media_id']
                                    redis.hmset(key, {
                                        "content_type": content_type,
                                        "media_id": media_id,
                                        "message": message_content,
                                        "media_finish": 0,
                                        "finish": 1
                                    })
                    elif content_type == 'IMAGE':
                        message_content = message['content']
                        attachment = lanying_utils.safe_json_loads(message.get('attachment', ''))
                        attachment_url = attachment['url']
                        user_id = message['from']['uid']
                        extra = {'image_type': '1'}
                        filename = lanying_utils.get_temp_filename(app_id, ".png")
                        result = lanying_im_api.download_url(config, app_id, user_id, attachment_url, filename, extra)
                        if result['result'] == 'ok':
                            result = upload_wechat_media(config, app_id, 'image', filename)
                            if result['result'] == 'ok':
                                media_id = result['media_id']
                                redis.hmset(key, {
                                    "content_type": content_type,
                                    "media_id": media_id,
                                    "message": message_content,
                                    "media_finish": 0,
                                    "finish": 1
                                })
    else:
        wechat_username = get_wechat_username(app_id, to_user_id)
        if wechat_username:
            send_wechat_message(config, app_id, message, wechat_username)

def check_message_need_send(config, message):
    from_user_id = int(message['from']['uid'])
    to_user_id = int(message['to']['uid'])
    type = message['type']
    my_user_id = config['lanying_user_id']
    verify_type = config.get('type', 'verified')
    if my_user_id != None and from_user_id == my_user_id and to_user_id != my_user_id and (type == 'CHAT' or type == 'REPLACE' or type == 'APPEND'):
        ext = message.get('ext', '')
        try:
            json_ext = json.loads(ext)
        except Exception as e:
            json_ext = {}
        try:
            is_stream = (json_ext['ai']['stream'] == True)
            is_finish = (json_ext['ai']['finish'] == True)
        except Exception as e:
            is_stream = False
            is_finish = False
        if is_stream:
            if verify_type == 'unverified':
                pass
            else:
                if type == 'REPLACE':
                    pass
                elif type == 'CHAT' and is_finish:
                    pass
                else:
                    logging.info(f"skip chat and stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                    return {'result':'error', 'msg':''}
        else:
            if type == 'REPLACE' or type == 'APPEND':
                logging.info(f"skip EDIT and not stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'msg':''}
        logging.info(f'check_message_need_send: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},result:ok')
        return {'result':'ok', "is_stream": is_stream, "verify_type":verify_type, "json_ext":json_ext, "msg_type": type}
    logging.info(f'skip other user msg: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id}, type:{type}')
    return {'result':'error', 'msg':''}

def send_wechat_message(config, app_id, message, to_username):
    content_type = message.get('ctype', 'TEXT')
    if content_type == 'TEXT':
        send_wechat_message_text(config, app_id, message, to_username)
    elif content_type == 'IMAGE':
        send_wechat_message_image(config, app_id, message, to_username)
    elif content_type == 'AUDIO':
        send_wechat_message_audio(config, app_id, message, to_username)

def send_wechat_message_text(config, app_id, message, to_username):
    access_token = get_wechat_access_token(config, app_id)
    content = message['content']
    content_list = split_string_by_size(content, official_account_max_message_size)
    for now_content in content_list:
        data = {
            "touser": to_username,
            "msgtype": "text",
            "text": {
                "content": now_content
            }
        }
        logging.info(f"send_wechat_message_text start | app_id:{app_id}, to_username:{to_username}, content:{now_content}")
        server, headers = get_proxy_info()
        url = f"{server}/cgi-bin/message/custom/send?access_token={access_token}"
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
        result = response.json()
        logging.info(f"send_wechat_message_text finish| app_id:{app_id}, to_username:{to_username}, content:{now_content}, result:{result}")

def send_wechat_message_image(config, app_id, message, to_username):
    attachment = lanying_utils.safe_json_loads(message.get('attachment', ''))
    if 'url' in attachment:
        attachment_url = attachment['url']
        user_id = message['from']['uid']
        extra = {'image_type': '1'}
        filename = lanying_utils.get_temp_filename(app_id, ".png")
        result = lanying_im_api.download_url(config, app_id, user_id, attachment_url, filename, extra)
        if result['result'] == 'ok':
            result = upload_wechat_media(config, app_id, 'image', filename)
            if result['result'] == 'ok':
                media_id = result['media_id']
                access_token = get_wechat_access_token(config, app_id)
                data = {
                    "touser": to_username,
                    "msgtype": "image",
                    "image": {
                        "media_id": media_id
                    }
                }
                logging.info(f"send_wechat_message_image start | app_id:{app_id}, to_username:{to_username}, filename:{filename}, media_id:{media_id}")
                server, headers = get_proxy_info()
                url = f"{server}/cgi-bin/message/custom/send?access_token={access_token}"
                response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
                result = response.json()
                logging.info(f"send_wechat_message_image finish| app_id:{app_id}, to_username:{to_username}, filename:{filename}, media_id:{media_id}, result:{result}")

def send_wechat_message_audio(config, app_id, message, to_username):
    attachment = lanying_utils.safe_json_loads(message.get('attachment', ''))
    if 'url' in attachment:
        attachment_url = attachment['url']
        user_id = message['from']['uid']
        extra = {'format': 'mp3'}
        filename = lanying_utils.get_temp_filename(app_id, ".mp3")
        result = lanying_im_api.download_url(config, app_id, user_id, attachment_url, filename, extra)
        if result['result'] == 'ok':
            result = upload_wechat_media(config, app_id, 'voice', filename)
            if result['result'] == 'ok':
                media_id = result['media_id']
                access_token = get_wechat_access_token(config, app_id)
                data = {
                    "touser": to_username,
                    "msgtype": "voice",
                    "voice": {
                        "media_id": media_id
                    }
                }
                logging.info(f"send_wechat_message_audio start | app_id:{app_id}, to_username:{to_username}, filename:{filename}, media_id:{media_id}")
                server, headers = get_proxy_info()
                url = f"{server}/cgi-bin/message/custom/send?access_token={access_token}"
                response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
                result = response.json()
                logging.info(f"send_wechat_message_audio finish| app_id:{app_id}, to_username:{to_username}, filename:{filename}, media_id:{media_id}, result:{result}")

def get_wechat_media(config, app_id, media_id, filename):
    access_token = get_wechat_access_token(config, app_id)
    logging.info(f"get_wechat_media start | app_id:{app_id}, media_id:{media_id}")
    server, headers = get_proxy_info()
    params = {
        'access_token': access_token,
        'media_id': str(media_id)
    }
    url = f"{server}/cgi-bin/media/get"
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        logging.info(f"get_wechat_media got response: app_id:{app_id}, media_id:{media_id}, response_headers:{response.headers}")
        content_type = response.headers.get('Content-Type')
        if content_type == 'application/json':
            logging.info(f"get_wechat_media got error: app_id:{app_id}, media_id:{media_id}, response:{response.text}")
            return {'result': 'error', 'message': response.text}
        else:
            with open(filename, 'wb') as f:
                f.write(response.content)
            logging.info(f"get_wechat_media finish| app_id:{app_id}, media_id:{media_id}, filename:{filename}, result: success")
            return {'result': 'ok'}

def upload_wechat_media(config, app_id, media_type, filename):
    access_token = get_wechat_access_token(config, app_id)
    logging.info(f"upload_wechat_media start | app_id:{app_id}, media_type:{media_type}, filename:{filename}")
    server, headers = get_proxy_info()
    url = f"{server}/cgi-bin/media/upload"
    params = {
        "access_token": access_token,
        "type": media_type
    }
    files = {'media': open(filename, 'rb')}
    response = requests.post(url, params=params, files=files, headers=headers)
    response_json = response.json()
    if 'media_id' in response_json:
        media_id = response_json['media_id']
        logging.info(f"upload_wechat_media success | app_id:{app_id}, media_type:{media_type}, filename:{filename}, media_id:{media_id}")
        return {'result': 'ok', 'media_id': media_id}
    else:
        logging.info(f"upload_wechat_media failed | app_id:{app_id}, media_type:{media_type}, filename:{filename}, reason:{response.text}")
        return {'result': 'error', 'message': response.text}

def check_token(app_id):
    config = lanying_config.get_service_config(app_id, service)
    if config:
        wechat_message_token = config.get('wechat_message_token')
        signature = request.args.get('signature','')
        timestamp = request.args.get('timestamp','')
        nonce = request.args.get('nonce','')
        sign_list = sorted([wechat_message_token,timestamp,nonce])
        mysignature = hashlib.sha1("".join(sign_list).encode("utf-8")).hexdigest()
        logging.info(f"my:{mysignature},got:{signature}")
        return mysignature == signature
    return True

def get_or_register_user(config, app_id, username):
    verify_type = config.get('type', 'verified')
    redis = lanying_redis.get_redis_connection()
    key = wechat_user_key(app_id, username)
    if verify_type == 'unverified':
        logging.info(f"get_or_register_user no need bind | app_id:{app_id}, username:{username}")
        result = redis.get(key)
        if result:
            user_id = int(result)
            return user_id
        else:
            user_id = register_anonymous_user(app_id, username, "wechat_")
            if user_id:
                im_key = im_user_key(app_id, user_id)
                redis.set(key, user_id)
                redis.set(im_key, username)
            return user_id
    login_info = lanying_im_api.official_account_login_with_open_id(config, app_id, username)
    user_id = login_info.get('data', {}).get('user_id', 0)
    bind_token = login_info.get('data', {}).get('openid', '')
    if user_id > 0:
        im_key = im_user_key(app_id, user_id)
        redis.set(key, user_id)
        redis.set(im_key, username)
        logging.info(f"get_or_register_user found binding | app_id:{app_id}, username:{username}, user_id:{user_id}")
        return user_id
    elif len(bind_token) > 0:
        logging.info(f"get_or_register_user found bind_token | app_id:{app_id}, username:{username}, bind_token:{bind_token}")
        user_id = register_anonymous_user(app_id, username, "wechat_")
        if user_id:
            logging.info(f"get_or_register_user bind token to user | app_id:{app_id}, username:{username}, bind_token:{bind_token}, user_id:{user_id}")
            executor.submit(lanying_im_api.official_account_bind, config, app_id, user_id, bind_token)
            im_key = im_user_key(app_id, user_id)
            redis.set(key, user_id)
            redis.set(im_key, username)
        return user_id
    else:
        logging.info(f"get_or_register_user cannot bind | app_id:{app_id}, username:{username}")
        result = redis.get(key)
        if result:
            user_id = int(result)
            return user_id
        else:
            user_id = register_anonymous_user(app_id, username, "wechat_")
            if user_id:
                im_key = im_user_key(app_id, user_id)
                redis.set(key, user_id)
                redis.set(im_key, username)
            return user_id

def get_wechat_username(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    im_key = im_user_key(app_id, user_id)
    result = redis.get(im_key)
    if result:
        username = str(result,'utf-8')
        wechat_key = wechat_user_key(app_id, username)
        wechat_result = redis.get(wechat_key)
        if wechat_result:
            if int(wechat_result) == int(user_id):
                return username
            else:
                logging.info(f"get_wechat_username | not match, app_id:{app_id}, user_id:{user_id}, username:{username}, username_to_user_id:{wechat_result}")
                return None
        else:
            logging.info(f"get_wechat_username | not match, app_id:{app_id}, user_id:{user_id}, username:{username}")
            return None
    logging.info(f"get_wechat_username | not found, app_id:{app_id}, user_id:{user_id}")
    return None

def get_wechat_access_token(config, app_id):
    result = lanying_im_api.get_wechat_official_account_access_token(config, app_id)
    if 'data' in result and 'access_token' in result['data']:
        access_token = result['data']['access_token']
        return access_token
    return None

def wechat_user_key(app_id, username):
    return f"{service}:wechat_user:{app_id}:{username}"

def im_user_key(app_id, user_id):
    return f"{service}:im_user:{app_id}:{user_id}"

def wechat_access_token_key(app_id):
    return f"{service}:wechat_access_token:{app_id}"

def register_anonymous_user(app_id, username, prefix):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    password = get_random_string(32)
    response = requests.post(apiEndpoint + '/user/register/anonymous',
                                headers={'app_id': app_id},
                                json={'username':prefix,
                                        'password': password})
    logging.info(f"register user, app_id={app_id}, username={username}, response={response.content}")
    logging.info(password)
    response_json = json.loads(response.content)
    if response_json['code'] == 200:
        user_id = response_json['data']['user_id']
        logging.info(f"register user, app_id={app_id}, username={username}, user_id={user_id}")
        return user_id
    return None

def get_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def split_string_by_size(input_string, chunk_size):
    return [input_string[i:i+chunk_size] for i in range(0, len(input_string), chunk_size)]

def get_proxy_info():
    proxy_server = os.getenv("LANYING_CONNECTOR_WECHAT_PROXY_SERVER", '')
    if len(proxy_server) > 0:
        proxy_key = os.getenv("LANYING_CONNECTOR_WECHAT_PROXY_KEY", '')
        headers = {
            "Authorization": f"Basic {proxy_key}",
        }
        return (proxy_server, headers)
    else:
        server = "https://api.weixin.qq.com"
        headers = {}
        return (server, headers)

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

@bp.route("/service/wechat_official_account/get_wechat_menu", methods=["POST"])
def get_wechat_menu():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = get_menu(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':remove_menu_unsupport_types(result["data"])})
    return resp

@bp.route("/service/wechat_official_account/set_wechat_menu", methods=["POST"])
def set_wechat_menu():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    menu = dict(data['menu'])
    result = set_menu(app_id, menu)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat_official_account/list_wechat_menu_history", methods=["POST"])
def list_wechat_menu_history():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = list_menu_history(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

def get_menu(app_id):
    config = lanying_config.get_service_config(app_id, service)
    access_token = get_wechat_access_token(config, app_id)
    server, headers = get_proxy_info()
    url = f'{server}/cgi-bin/get_current_selfmenu_info?access_token={access_token}'
    result = json.loads(requests.get(url, headers=headers).content)
    logging.info(f"get_menu:{app_id}, result:{result}")
    if 'errmsg' in result and result['errmsg'] != "ok":
        return {'result': 'error', 'message':result['errmsg']}
    return {'result': 'ok', 'data': format_menu(result)}

def set_menu(app_id, menu):
    old_menu = get_menu(app_id)
    menu = remove_menu_unsupport_types(menu)
    config = lanying_config.get_service_config(app_id, service)
    access_token = get_wechat_access_token(config, app_id)
    server, headers = get_proxy_info()
    if isinstance(menu, dict) and 'button' in menu and len(menu['button']) == 0:
        url = f'{server}/cgi-bin/menu/delete?access_token={access_token}'
    else:
        url = f'{server}/cgi-bin/menu/create?access_token={access_token}'
    response = requests.post(url, data=json.dumps(menu, ensure_ascii=False).encode('utf-8'), headers=headers)
    result = json.loads(response.content)
    logging.info(f"set_menu:{app_id}, menu:{menu}, result:{result}")
    if 'errmsg' in result and result['errmsg'] != "ok":
        return {'result': 'error', 'message':result['errmsg']}
    save_menu_history(app_id, old_menu, menu)
    return {'result': 'ok', 'data': result} 


def format_menu(obj):
    if isinstance(obj, dict):
        ret = {}
        for k,v in obj.items():
            if k == 'sub_button' and isinstance(v, dict) and 'list' in v:
                ret[k] = format_menu(v['list'])
            elif k == 'selfmenu_info':
                if 'is_menu_open' in obj and obj['is_menu_open'] == 0:
                    ret[k] = {'button':[]}
                else:
                    ret[k] = format_menu(v)
            else:
                ret[k] = format_menu(v)
        return ret
    elif isinstance(obj, list):
        ret = []
        for item in obj:
            ret.append(format_menu(item))
        return ret
    else:
        return obj

def remove_menu_unsupport_types(obj):
    if isinstance(obj, dict):
        if 'type' in obj and not is_menu_type_allowed(obj['type']):
            return None
        ret = {}
        for k,v in obj.items():
            ret[k] = remove_menu_unsupport_types(v)
        return ret
    elif isinstance(obj, list):
        ret = []
        for item in obj:
            new_item = remove_menu_unsupport_types(item)
            if new_item is not None:
                ret.append(new_item)
        return ret
    else:
        return obj

def is_menu_type_allowed(type):
    return type in ["miniprogram", "click", "view", "scancode_push", "scancode_waitmsg", "pic_sysphoto", "pic_photo_or_album", "pic_weixin", "location_select", "media_id","article_id", "article_view_limited"]

def save_menu_history(app_id, old_menu, menu):
    try:
        redis = lanying_redis.get_redis_connection()
        now = int(time.time())
        info = {'selfmenu_info': menu, 'create_time': now}
        info_json = json.dumps(info, ensure_ascii=False)
        list_key = menu_history_list_key(app_id)
        list_count = redis.rpush(list_key, info_json)
        if list_count == 1:
            logging.info(f"old wechat menu:{app_id}:{old_menu}")
            if old_menu['result'] == 'ok':
                old_info = old_menu['data']
                old_info['create_time'] = now - 1
                redis.lpush(list_key, json.dumps(old_menu['data'], ensure_ascii=False))
        elif list_count > 20:
            value_to_delete = redis.lindex(list_key, 10)
            redis.lrem(list_key, 1, value_to_delete)
    except Exception as e:
        logging.exception(e)

def list_menu_history(app_id):
    redis = lanying_redis.get_redis_connection()
    list_key = menu_history_list_key(app_id)
    history_list = lanying_redis.redis_lrange(redis, list_key, 0, -1)
    ret = []
    for history in history_list:
        ret.append(json.loads(history))
    return {'result': 'ok', 'data': {'list': ret}}

def menu_history_list_key(app_id):
    return f"lanying-connector:menu_history_list:{app_id}"
