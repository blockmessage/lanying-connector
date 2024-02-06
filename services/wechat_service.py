
from flask import Blueprint, request, make_response
import logging
import time
import json
import lanying_wechat_chatbot
import lanying_chatbot
import os
import lanying_redis
import lanying_config
import requests
import string
import random
import lanying_message
import lanying_im_api
import lanying_file_storage
from lanying_connector import executor
import lanying_utils
import re
import lanying_user_router

wechat_max_message_size = 3900
service = 'wechat'
bp = Blueprint(service, __name__)
wechat_chatbot_message_token = os.getenv('WECHAT_CHATBOT_MESSAGE_TOKEN')

@bp.route("/wechat/<string:token>/messages", methods=["POST"])
def service_post_messages(token):
    text = request.get_data(as_text=True)
    message = json.loads(text)
    if token == wechat_chatbot_message_token:
        logging.info(f"receive wechat message:{message}")
        wc_id = message.get('wcId', '')
        account = message.get('account', '')
        message_type = message.get('messageType', '')
        data = message.get('data', {})
        if message_type == '60001':
            handle_wechat_chat_message(wc_id, account, data)
        elif message_type == '80001':
            handle_wechat_group_message(wc_id, account, data)
        elif message_type == '85001':
            handle_wechat_group_notify(wc_id, account, data)
        elif message_type == '30000':
            handle_wechat_offline(wc_id, account, data)
    else:
        logging.info(f"receive invalid wechat message:{message}")
    resp = make_response({'code':200, 'data':{'success': True}})
    return resp

@bp.route("/service/wechat/login", methods=["POST"])
def login():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    type = str(data['type'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    proxy = int(data.get('proxy', 0))
    ttuid = str(data.get('ttuid', ''))
    result = lanying_wechat_chatbot.login(app_id, type, wechat_chatbot_id, proxy, ttuid)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/get_login_info", methods=["POST"])
def get_login_info():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    w_id = str(data['w_id'])
    result = lanying_wechat_chatbot.get_login_info(app_id, w_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/create_wechat_chatbot", methods=["POST"])
def create_wechat_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    w_id = str(data['w_id'])
    chatbot_id = str(data['chatbot_id'])
    msg_types = list(data['msg_types'])
    non_friend_chat_mode = str(data['non_friend_chat_mode'])
    note = str(data.get('note',''))
    router_sub_user_ids = list(data.get('router_sub_user_ids',[]))
    result = lanying_wechat_chatbot.create_wechat_chatbot(app_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note, router_sub_user_ids)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/list_wechat_chatbots", methods=["POST"])
def list_wechat_chatbots():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_wechat_chatbot.list_wechat_chatbots(app_id)
    resp = make_response({'code':200, 'data': {'list': result}})
    return resp

@bp.route("/service/wechat/change_status", methods=["POST"])
def change_status():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    status = str(data['status'])
    result = lanying_wechat_chatbot.change_status(app_id, wechat_chatbot_id, status)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/deduct_failed", methods=["POST"])
def deduct_failed():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    result = lanying_wechat_chatbot.deduct_failed(app_id, wechat_chatbot_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/delete_wechat_chatbot", methods=["POST"])
def delete_wechat_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    result = lanying_wechat_chatbot.delete_wechat_chatbot(app_id, wechat_chatbot_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/configure_wechat_chatbot", methods=["POST"])
def configure_wechat_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    w_id = str(data.get('w_id',''))
    chatbot_id = str(data['chatbot_id'])
    msg_types = list(data['msg_types'])
    non_friend_chat_mode = str(data['non_friend_chat_mode'])
    note = str(data.get('note',''))
    router_sub_user_ids = list(data.get('router_sub_user_ids',[]))
    result = lanying_wechat_chatbot.configure_wechat_chatbot(app_id, wechat_chatbot_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note, router_sub_user_ids)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

def handle_wechat_chat_message(wc_id, account, data):
    redis = lanying_redis.get_redis_connection()
    content = data['content']
    from_user = data['fromUser']
    msg_id = data['msgId']
    new_msg_id = data['newMsgId']
    self = data.get('self', False)
    timestamp = data['timestamp']
    to_user = data['toUser']
    wid = data['wId']
    if self:
        logging.info(f"handle_chat_message skip self message | self:{self}, wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    if is_wechat_official_account(from_user):
        logging.info(f"handle_chat_message skip wechat official account message: wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    message_deduplication = message_deduplication_key(from_user, to_user, msg_id, new_msg_id)
    if redis.get(message_deduplication):
        logging.info(f"handle_chat_message skip for message_deduplication | wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    check_result = check_wid(wid)
    if check_result['result'] == 'error':
        logging.info(f"handle_wechat_chat_message skip for {check_result['message']} | wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    to_user_id = check_result['user_id']
    app_id = check_result['app_id']
    router_sub_user_ids = check_result['router_sub_user_ids']
    from_user_id = get_or_register_user(app_id, from_user)
    if from_user_id:
        maybe_update_user_profile_from_wechat(app_id, wid, from_user, from_user_id)
        config = lanying_config.get_service_config(app_id, service)
        redis.setex(message_deduplication, 3*86400, "1")
        router_res = lanying_user_router.handle_msg_route_to_im(app_id, service, from_user_id, to_user_id, router_sub_user_ids)
        if router_res['result'] == 'ok':
            msg_ext = {'ai':{'role':'user', 'channel':'wechat'}}
            if router_res['msg_type'] == 'CHAT':
                lanying_message.send_message_async(config, app_id, router_res['from'], router_res['to'],content, msg_ext)
            else:
                logging.info(f"handle_wechat_chat_message receive groupchat | router_res:{router_res}")
    else:
        logging.info(f"handle_wechat_chat_message user_id not found: {from_user_id}")

def handle_wechat_group_message(wc_id, account, data):
    redis = lanying_redis.get_redis_connection()
    content = data['content']
    from_user = data['fromUser']
    from_group = data['fromGroup']
    msg_id = data['msgId']
    new_msg_id = data['newMsgId']
    self = data.get('self', False)
    timestamp = data['timestamp']
    to_user = data['toUser']
    atlist = data.get('atlist', [])
    wid = data['wId']
    if self:
        logging.info(f"handle_wechat_group_message skip self message | self:{self}, wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    message_deduplication = message_deduplication_key(from_user, to_user, msg_id, new_msg_id)
    if redis.get(message_deduplication):
        logging.info(f"handle_wechat_group_message skip for message_deduplication | wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    check_result = check_wid(wid)
    if check_result['result'] == 'error':
        logging.info(f"handle_wechat_group_message skip for {check_result['message']} | wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    to_user_id = check_result['user_id']
    app_id = check_result['app_id']
    router_sub_user_ids = check_result['router_sub_user_ids']
    from_user_id = get_or_register_user(app_id, from_user)
    group_id = get_or_create_group(app_id, wid, from_group, from_user_id, to_user_id)
    ensure_user_in_group(app_id, from_user_id, group_id)
    ensure_user_in_group(app_id, to_user_id, group_id)
    try:
        maybe_sync_wechat_user_group_info_to_im(app_id, wid, from_group, group_id, from_user, from_user_id)
    except Exception as e:
        logging.exception(e)
    if from_user_id:
        #maybe_update_user_profile_from_wechat(app_id, wid, from_user, from_user_id)
        config = lanying_config.get_service_config(app_id, service)
        redis.setex(message_deduplication, 3*86400, "1")
        msg_config = transform_at_list_to_im(app_id, atlist, content, wc_id, to_user_id)
        router_res = lanying_user_router.handle_group_msg_route_to_im(app_id, service, from_user_id, to_user_id, router_sub_user_ids, group_id)
        if router_res['result'] == 'ok':
            msg_ext = {'ai':{'role':'user', 'channel':'wechat'}}
            if router_res['msg_type'] == 'CHAT':
                lanying_message.send_message_async(config, app_id, router_res['from'], router_res['to'], content, msg_ext)
            elif router_res['msg_type'] == 'GROUPCHAT':
                lanying_message.send_group_message_async(config, app_id, router_res['from'], router_res['to'], content, msg_ext, msg_config)
    else:
        logging.info(f"handle_wechat_group_message user_id not found: {from_user_id}")

def transform_at_list_to_im(app_id, atlist, content, wc_id, to_user_id):
    if content.startswith('@所有人\u2005') and len(atlist) > 0:
        return {'mentionAll': True}
    else:
        mention_list = []
        for now_wc_id in atlist:
            if now_wc_id == wc_id:
                mention_list.append(to_user_id)
            else:
                user_id = get_user(app_id, now_wc_id)
                if user_id:
                    mention_list.append(user_id)
                elif len(mention_list) < 2:
                    user_id = get_or_register_user(app_id, now_wc_id)
                    if user_id:
                        mention_list.append(user_id)
        if len(mention_list) > 0:
            return {'mentionList': mention_list}
        else:
            return {}

def check_wid(wid):
    global_wid_info = lanying_wechat_chatbot.get_global_wid_info(wid)
    if global_wid_info is None:
        return {'result': 'error', 'message': 'w_id not found'}
    app_id = global_wid_info['app_id']
    wechat_chatbot_id = global_wid_info['wechat_chatbot_id']
    wechat_chatbot_info = lanying_wechat_chatbot.get_wechat_chatbot(app_id, wechat_chatbot_id)
    if wechat_chatbot_info is None:
        return {'result': 'error', 'message': 'wechat_chatbot_id not found'}
    if wechat_chatbot_info['deduct_failed'] == 'yes':
        return {'result': 'error', 'message': 'wechat_chatbot deduct_failed'}
    if wechat_chatbot_info['soft_status'] != 'enabled':
        return {'result': 'error', 'message': 'wechat_chatbot status not enabled'}
    if wechat_chatbot_info['status'] != 'online':
        return {'result': 'error', 'message': 'wechat_chatbot status not online'}
    chatbot_id = wechat_chatbot_info['chatbot_id']
    router_sub_user_ids = wechat_chatbot_info['router_sub_user_ids']
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {'result': 'error', 'message': 'chatbot_id not found'}
    user_id = chatbot_info['user_id']
    return {'result': 'ok', 'user_id': user_id, 'app_id': app_id, 'router_sub_user_ids':router_sub_user_ids}

def handle_wechat_group_notify(wc_id, account, data):
    wid = data['wId']
    wechat_group_id = data['userName']
    group_nickname = data.get('nickName', '')
    check_result = check_wid(wid)
    if check_result['result'] == 'error':
        logging.info(f"handle_wechat_chat_message skip for {check_result['message']} | wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    to_user_id = check_result['user_id']
    app_id = check_result['app_id']
    info_key = wechat_group_info_key(app_id, wechat_group_id)
    redis = lanying_redis.get_redis_connection()
    redis.delete(info_key)
    logging.info(f"handle_wechat_group_notify remove cache | wechat_group_id:{wechat_group_id}, group_nickname:{group_nickname}")

def handle_wechat_offline(wc_id, account, data):
    wid = data['wId']
    global_wid_info = lanying_wechat_chatbot.get_global_wid_info(wid)
    if global_wid_info is None:
        logging.info(f"handle_wechat_offline w_id not found: wc_id: {wc_id}, account:{account}, wid:{wid}, data:{data}")
        return
    app_id = global_wid_info['app_id']
    lanying_wechat_chatbot.change_wid_status(app_id, wid, "offline", 'kick')

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
    from_user_id = message['from']['uid']
    to_user_id = message['to']['uid']
    checkres = check_message_need_send(config, message)
    if checkres['result'] == 'error':
        logging.info(f"handle_chat_message skip with message: {checkres['message']}")
        return
    wechat_chatbot = checkres['wechat_chatbot']
    msg_type = checkres['type']
    logging.info(f"{service} | handle_chat_message do for user_id, app_id={app_id}, to_user_id:{to_user_id}")
    wechat_username = get_wechat_username(app_id, to_user_id)
    if wechat_username:
        w_id = wechat_chatbot['w_id']
        if len(w_id) > 0:
            w_id_info = lanying_wechat_chatbot.get_wid_info(app_id, w_id)
            if w_id_info:
                if w_id_info["status"] == 'binding':
                    send_wechat_message(config, app_id, message, wechat_username, w_id)
                else:
                    logging.info(f"wechat chatbot skip send message for bad wid status: wid:{w_id}, app_id:{app_id}, status:{w_id_info['status']}")
            else:
                logging.info(f"wechat chatbot skip send message for wid not found: wid:{w_id}, app_id:{app_id}")
    else:
        group_id = message['to']['uid']
        wechat_group_id = get_wechat_group_id(app_id, group_id)
        if wechat_group_id:
            w_id = wechat_chatbot['w_id']
            if len(w_id) > 0:
                w_id_info = lanying_wechat_chatbot.get_wid_info(app_id, w_id)
                if w_id_info:
                    if w_id_info["status"] == 'binding':
                        wechat_at_list = transform_at_list_from_im(app_id, message)
                        send_wechat_group_message(config, app_id, message, wechat_group_id, w_id, wechat_at_list)
                    else:
                        logging.info(f"wechat chatbot skip send group message for bad wid status: wid:{w_id}, app_id:{app_id}, status:{w_id_info['status']}")
                else:
                    logging.info(f"wechat chatbot skip send group message for wid not found: wid:{w_id}, app_id:{app_id}")

def transform_at_list_from_im(app_id, message):
    config = lanying_utils.safe_json_loads(message.get('config', '{}'))
    mention_list = config.get('mentionList', [])
    wechat_at_list = []
    if len(mention_list) > 0:
        for mention_user_id in mention_list:
            username = get_wechat_username(app_id, mention_user_id)
            if username:
                wechat_at_list.append(username)
    return wechat_at_list

def check_message_need_send(config, message):
    from_user_id = int(message['from']['uid'])
    to_user_id = int(message['to']['uid'])
    app_id = str(message['appId'])
    type = message['type']
    chatbot_id = lanying_chatbot.get_user_chatbot_id(app_id, from_user_id)
    if chatbot_id is None:
        return {'result': 'error', 'message': 'chatbot not found'}
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {'result': 'error', 'message': 'chatbot not found'}
    wechat_chatbot_id = chatbot['wechat_chatbot_id']
    wechat_chatbot = lanying_wechat_chatbot.get_wechat_chatbot(app_id,wechat_chatbot_id)
    if wechat_chatbot is None:
        return {'result': 'error', 'message': 'wechat_chatbot not found'}
    if wechat_chatbot['deduct_failed'] == 'yes':
        return {'result': 'error', 'message': 'wechat_chatbot deduct failed'}
    if wechat_chatbot['soft_status'] != 'enabled':
        return {'result': 'error', 'message': 'wechat_chatbot status not enabled'}
    if wechat_chatbot['status'] != 'online':
        return {'result': 'error', 'message': 'wechat_chatbot status not online'}
    my_user_id = chatbot['user_id']
    if my_user_id != None and from_user_id == my_user_id and to_user_id != my_user_id and (type == 'CHAT' or type == 'GROUPCHAT' or type == 'REPLACE' or type == 'APPEND'):
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
            if type == 'REPLACE':
                pass
            elif (type == 'CHAT' or type == 'GROUPCHAT') and is_finish:
                pass
            else:
                logging.info(f"skip chat and stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'message':''}
        else:
            if type == 'REPLACE' or type == 'APPEND':
                logging.info(f"skip EDIT and not stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'message':''}
        logging.info(f'check_message_need_send: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},result:ok')
        return {'result':'ok', 'wechat_chatbot': wechat_chatbot, 'type': type}
    logging.info(f'skip other user msg: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id}, type:{type}')
    return {'result':'error', 'message':''}

def get_or_register_user(app_id, username):
    redis = lanying_redis.get_redis_connection()
    key = wechat_user_key(app_id, username)
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

def get_user(app_id, username):
    redis = lanying_redis.get_redis_connection()
    key = wechat_user_key(app_id, username)
    result = redis.get(key)
    if result:
        user_id = int(result)
        return user_id
    else:
        return None

def get_im_group(app_id, wechat_group_id):
    redis = lanying_redis.get_redis_connection()
    key = wechat_group_key(app_id, wechat_group_id)
    result = redis.get(key)
    if result:
        group_id = int(result)
        return group_id
    return None

def get_or_create_group(app_id, wid, from_group, from_user_id, to_user_id):
    redis = lanying_redis.get_redis_connection()
    key = wechat_group_key(app_id, from_group)
    result = redis.get(key)
    if result:
        group_id = int(result)
        return group_id
    else:
        wechat_group_info = get_wechat_group_info(app_id, wid, from_group)
        wechat_group_name = wechat_group_info.get('nickName', '')
        if wechat_group_name is None or wechat_group_name == '':
            wechat_group_name = from_group
        group_id = create_lanying_group(app_id, wechat_group_name, from_user_id, to_user_id)
        change_group_apply_approval_accept_all(app_id, group_id)
        if group_id:
            im_key = im_group_key(app_id, group_id)
            redis.set(key, group_id)
            redis.set(im_key, from_group)
        return group_id

def register_anonymous_user(app_id, username, prefix):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    password = get_random_string(32)
    response = requests.post(apiEndpoint + '/user/register/anonymous',
                                headers={'app_id': app_id},
                                json={'username':prefix,
                                        'password': password})
    logging.info(f"register user, app_id={app_id}, username={username}, response={response.content}")
    response_json = json.loads(response.content)
    if response_json['code'] == 200:
        user_id = response_json['data']['user_id']
        logging.info(f"register user, app_id={app_id}, username={username}, user_id={user_id}")
        return user_id
    return None

def create_lanying_group(app_id, from_group, from_user_id, to_user_id):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    response = requests.post(apiEndpoint + '/group/create',
                                headers={'app_id': app_id, 'access-token': admin_token, 'user_id': str(to_user_id)},
                                json={'name':from_group,
                                        'type': 0,
                                        'user_list': [from_user_id]})
    logging.info(f"create group, app_id={app_id}, from_group={from_group}, response={response.content}")
    response_json = json.loads(response.content)
    if response_json['code'] == 200:
        group_id = response_json['data']['group_id']
        logging.info(f"create group, app_id={app_id}, from_group={from_group}, group_id={group_id}")
        return group_id
    return None

def get_wechat_group_info_from_cache(app_id, wechat_group_id):
    info_key = wechat_group_info_key(app_id, wechat_group_id)
    redis = lanying_redis.get_redis_connection()
    result = redis.get(info_key)
    if result:
        try:
            return json.loads(result)
        except Exception as e:
            pass
    return {}

def get_wechat_group_info(app_id, wid, wechat_group_id):
    try:
        url =  lanying_wechat_chatbot.get_api_server() + "/getChatRoomInfo"
        headers = lanying_wechat_chatbot.get_headers(app_id)
        data = {
            "wId": wid,
            "chatRoomId": wechat_group_id
        }
        logging.info(f"get_wechat_group_info start | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}")
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
        result = response.json()
        logging.info(f"get_wechat_group_info finish | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, result:{result}")
        if result["code"] == "1000":
            group_info_list = result['data']
            if len(group_info_list) == 1:
                group_info = group_info_list[0]
                info_key = wechat_group_info_key(app_id, wechat_group_id)
                redis = lanying_redis.get_redis_connection()
                redis.setex(info_key, 3600, json.dumps(group_info, ensure_ascii=False))
                return group_info
    except Exception as e:
        logging.exception(e)
    logging.info(f"get_wechat_group_info failed | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}")
    return {}

def change_group_apply_approval_accept_all(app_id, group_id):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    response = requests.post(apiEndpoint + '/group/settings/require_admin_approval',
                                headers={'app_id': app_id, 'access-token': admin_token, 'group_id': str(group_id)},
                                json={'group_id':group_id,
                                        'apply_approval': 0})
    logging.info(f"change_group_apply_approval_accept_all start, app_id={app_id}, group_id={group_id}, response={response.content}")
    response_json = json.loads(response.content)
    if response_json['code'] == 200:
        logging.info(f"change_group_apply_approval_accept_all success, app_id={app_id},group_id={group_id}")
        return True
    return False

def group_apply(app_id, user_id, group_id):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    response = requests.post(apiEndpoint + '/group/apply',
                                headers={'app_id': app_id, 'access-token': admin_token, 'user_id': str(user_id)},
                                json={'group_id':group_id,
                                        'reason': 'apply from lanying connector'})
    logging.info(f"group_apply start, app_id={app_id}, group_id={group_id}, response={response.content}")
    response_json = json.loads(response.content)
    if response_json['code'] in [200, 20017]:
        logging.info(f"group_apply success, app_id={app_id},group_id={group_id}")
        return True
    return False

def wait_user_in_group(app_id, user_id, group_id, try_times):
    for i in range(try_times):
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        admin_token = lanying_config.get_lanying_admin_token(app_id)
        response = requests.get(apiEndpoint + '/group/user_joined',
                                    headers={'app_id': app_id, 'access-token': admin_token, 'user_id': str(user_id)})
        logging.info(f"wait_user_in_group | app_id={app_id}, user_id={user_id}, group_id={group_id}, try_times:{i}/{try_times}, response={response.content}")
        response_json = json.loads(response.content)
        if response_json['code'] == 200:
            if group_id in response_json['data']:
                key = im_group_member_key(app_id, group_id)
                redis = lanying_redis.get_redis_connection()
                redis.hset(key, user_id, int(time.time()))
                return True
            else:
                time.sleep(1)
    return False

def get_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def wechat_user_key(app_id, username):
    return f"lc_service:{service}:wechat_user:{app_id}:{username}"

def wechat_group_key(app_id, group):
    return f"lc_service:{service}:wechat_group:{app_id}:{group}"

def wechat_group_info_key(app_id, group):
    return f"lc_service:{service}:wechat_group_info:{app_id}:{group}"

def wechat_user_info_key(app_id, username):
    return f"lc_service:{service}:wechat_user_info:{app_id}:{username}"

def im_user_key(app_id, user_id):
    return f"lc_service:{service}:im_user:{app_id}:{user_id}"

def im_group_key(app_id, group_id):
    return f"lc_service:{service}:im_group:{app_id}:{group_id}"

def im_group_member_key(app_id, group_id):
    return f"lc_service:{service}:im_group_member:{app_id}:{group_id}"

def im_group_member_info_key(app_id, group_id):
    return f"lc_service:{service}:im_group_member_info:{app_id}:{group_id}"

def im_group_info_key(app_id, group_id):
    return f"lc_service:{service}:im_group_info:{app_id}:{group_id}"

def message_deduplication_key(from_user, to_user, msg_id, new_msg_id):
    return f"lc_service:{service}:message_deduplication:{from_user}:{to_user}:{msg_id}:{new_msg_id}"

def get_wechat_username(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    im_key = im_user_key(app_id, user_id)
    result = redis.get(im_key)
    if result:
        return str(result,'utf-8')
    logging.info(f"get_wechat_username | not found, app_id:{app_id}, user_id:{user_id}")
    return None

def get_wechat_group_id(app_id, group_id):
    redis = lanying_redis.get_redis_connection()
    im_key = im_group_key(app_id, group_id)
    result = redis.get(im_key)
    if result:
        return str(result,'utf-8')
    logging.info(f"get_wechat_group_id | not found, app_id:{app_id}, group_id:{group_id}")
    return None

def send_wechat_message(config, app_id, message, to_username, w_id):
    url =  lanying_wechat_chatbot.get_api_server() + "/sendText"
    headers = lanying_wechat_chatbot.get_headers(app_id)
    content = message['content']
    content_list = split_string_by_size(content, wechat_max_message_size)
    for now_content in content_list:
        data = {
            "wId": w_id,
            "wcId": to_username,
            "content": now_content
        }
        logging.info(f"wechat_chatbot send_wechat_message start | app_id:{app_id}, to_username:{to_username}, content:{now_content}")
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
        result = response.json()
        logging.info(f"wechat_chatbot send_wechat_message finish| app_id:{app_id}, to_username:{to_username}, content:{now_content}, result:{result}")
        if result["code"] == "1001" and result["message"] == 'wId已注销或二维码失效，请重新登录':
            logging.info(f"wechat chatbot wid offline by send message result: wid:{w_id}, result:{result}")
            lanying_wechat_chatbot.change_wid_status(app_id, w_id, "offline", 'offline')

def send_wechat_group_message(config, app_id, message, wechat_group_id, w_id, wechat_at_list):
    url =  lanying_wechat_chatbot.get_api_server() + "/sendText"
    headers = lanying_wechat_chatbot.get_headers(app_id)
    content = message['content']
    content_list = split_string_by_size(content, wechat_max_message_size)
    for now_content in content_list:
        data = {
            "wId": w_id,
            "wcId": wechat_group_id,
            "content": now_content
        }
        if len(wechat_at_list) > 0:
            data['at'] = ','.join(wechat_at_list)
        logging.info(f"wechat_chatbot send_wechat_group_message start | app_id:{app_id}, wechat_group_id:{wechat_group_id}, content:{now_content}, wechat_at_list:{wechat_at_list}")
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
        result = response.json()
        logging.info(f"wechat_chatbot send_wechat_group_message finish| app_id:{app_id}, wechat_group_id:{wechat_group_id}, content:{now_content}, wechat_at_list:{wechat_at_list}, result:{result}")
        if result["code"] == "1001" and result["message"] == 'wId已注销或二维码失效，请重新登录':
            logging.info(f"wechat chatbot wid offline by send message result: wid:{w_id}, result:{result}")
            lanying_wechat_chatbot.change_wid_status(app_id, w_id, "offline", 'offline')

def replace_at_message(text):
    pattern = re.compile(r'(@[^\s,\u2005]+)[\s\u2005,]+')
    result = pattern.sub('\\1\u2005', text)
    return result

def split_string_by_size(input_string, chunk_size):
    return [input_string[i:i+chunk_size] for i in range(0, len(input_string), chunk_size)]

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

def maybe_update_user_profile_from_wechat(app_id, wid, username, user_id):
    redis = lanying_redis.get_redis_connection()
    user_info_key = wechat_user_info_key(app_id, username)
    info = lanying_redis.redis_hgetall(redis, user_info_key)
    update_time = int(info.get('update_time', 0))
    nickname = info.get('nickname', '')
    now = int(time.time())
    if (nickname == '' and now - update_time > 86400) or (now - update_time > 7 * 86400):
        redis.hset(user_info_key, "update_time", now)
        executor.submit(update_user_profile_from_wechat, app_id, wid, username, user_id)

def update_user_profile_from_wechat(app_id, wid, username, user_id):
    logging.info(f"update_user_profile_from_wechat start | app_id:{app_id}, wid:{wid}, username:{username}, user_id:{user_id}")
    url =  lanying_wechat_chatbot.get_api_server() + "/getContact"
    headers = lanying_wechat_chatbot.get_headers(app_id)
    data = {
        "wId": wid,
        "wcId": username
    }
    logging.info(f"update_user_profile_from_wechat fetch from wechat start | app_id:{app_id}, username:{username}")
    response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
    result = response.json()
    logging.info(f"update_user_profile_from_wechat fetch from wechat finish | app_id:{app_id}, username:{username}, result:{result}")
    if result["code"] == "1000" and len(result['data']) == 1:
        info = result['data'][0]
        nickname = info.get('nickName', '')
        avatar = info.get('bigHead', '')
        if len(nickname) > 0 or len(avatar) > 0:
            redis = lanying_redis.get_redis_connection()
            user_info_key = wechat_user_info_key(app_id, username)
            info = lanying_redis.redis_hgetall(redis, user_info_key)
            old_nickname = info.get('nickname', '')
            old_avatar = info.get('avatar', '')
            if old_nickname != nickname:
                success = sync_user_profile_to_lanying_user(app_id, user_id, nickname)
                logging.info(f"sync_user_profile_to_lanying_user | app_id:{app_id}, wid:{wid}, username:{username}, user_id:{user_id}, success:{success}")
                if success:
                    redis.hset(user_info_key, 'nickname', nickname)
            if  old_avatar != avatar:
                success = sync_user_avatar_to_lanying_user(app_id, user_id, avatar)
                logging.info(f"sync_user_avatar_to_lanying_user | app_id:{app_id}, wid:{wid}, username:{username}, user_id:{user_id}, success:{success}")
                if success:
                    redis.hset(user_info_key, 'avatar', avatar)

def sync_user_profile_to_lanying_user(app_id, user_id, nickname):
    profile_result = lanying_im_api.set_user_profile(app_id, user_id, '', nickname, '')
    logging.info(f"set profile result:{profile_result}")
    if profile_result and profile_result["code"] == 200:
        return True
    return False

def sync_user_avatar_to_lanying_user(app_id, user_id, avatar):
    temp_filename = f"{app_id}_{user_id}_{int(time.time())}.jpg"
    download_result = lanying_file_storage.download_file_url(avatar, {}, temp_filename)
    if download_result['result'] == 'ok':
        upload_info = lanying_im_api.get_user_avatar_upload_url(app_id, user_id)
        if upload_info and upload_info['code'] == 200:
            upload_info_data = upload_info.get('data', {})
            upload_url = upload_info_data.get('upload_url', '')
            download_url = upload_info_data.get('download_url', '')
            oss_body_param = upload_info_data.get('oss_body_param', {})
            files = {
                'file': ('avatar.jpg', open(temp_filename, 'rb'), 'image/jpeg'),
            }
            data = {
                'OSSAccessKeyId': oss_body_param.get('OSSAccessKeyId', ''),
                'policy': oss_body_param.get('policy', ''),
                'signature': oss_body_param.get('signature', ''),
                'callback': oss_body_param.get('callback', ''),
                'key': oss_body_param.get('key', ''),
            }
            response = requests.post(upload_url, headers={}, files=files, data=data)
            logging.info(f"upload to oss result | app_id:{app_id}, user_id:{user_id}, response.status_code:{response.status_code}, response_text:{response.text}")
            if response.status_code == 200:
                avatar_set_result = lanying_im_api.set_user_avatar(app_id, user_id, download_url)
                logging.info(f"set avatar result:{avatar_set_result}")
                if avatar_set_result and avatar_set_result["code"] == 200:
                    return True
    return False

def ensure_user_in_group(app_id, user_id, group_id):
    key = im_group_member_key(app_id, group_id)
    redis = lanying_redis.get_redis_connection()
    if redis.hexists(key, user_id):
        return True
    group_apply(app_id, user_id, group_id)
    wait_user_in_group(app_id, user_id, group_id, 5)

def is_wechat_official_account(username):
    pattern = re.compile(r'^gh_[0-9a-f]{10,16}$')
    if pattern.match(username):
        return True
    return False

def maybe_sync_wechat_user_group_info_to_im(app_id, wid, wechat_group_id, group_id, from_user, from_user_id):
    logging.info(f"maybe_sync_wechat_user_group_info_to_im start | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, group_id:{group_id}, from_user:{from_user}, from_user_id:{from_user_id}")
    group_info = get_wechat_group_info_from_cache(app_id, wechat_group_id)
    members = group_info.get('chatRoomMembers',[])
    from_user_info = None
    for member in members:
        if member.get('userName', '') == from_user:
            from_user_info = member
            break
    if from_user_info is None:
        logging.info(f"maybe_sync_wechat_user_group_info_to_im cache not used | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, group_id:{group_id}, from_user:{from_user}, from_user_id:{from_user_id}")
        group_info = get_wechat_group_info(app_id, wid, wechat_group_id)
        members = group_info.get('chatRoomMembers',[])
        from_user_info = None
        for member in members:
            if member.get('userName', '') == from_user:
                from_user_info = member
                break
    redis = lanying_redis.get_redis_connection()
    group_name = group_info.get('nickName', '')
    if group_name is not None and len(group_name) > 0:
        group_info_key = im_group_info_key(app_id, group_id)
        old_groupname = lanying_redis.redis_hget(redis, group_info_key, 'name')
        if old_groupname is None or old_groupname != group_name:
            logging.info(f"maybe_sync_wechat_user_group_info_to_im sync group name | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, group_id:{group_id}, from_user:{from_user}, from_user_id:{from_user_id}, group_name:{group_name}, old_groupname:{old_groupname}")
            result = lanying_im_api.set_group_name(app_id, group_id, group_name)
            if result and result["code"] == 200:
                redis.hset(group_info_key, 'name', group_name)
            else:
                logging.info("maybe_sync_wechat_user_group_info_to_im fail to sync group_name")
    if from_user_info:
        nickname = from_user_info.get('nickName', '')
        if nickname is not None and nickname != '':
            logging.info(f"maybe_sync_wechat_user_group_info_to_im user found | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, group_id:{group_id}, from_user:{from_user}, from_user_id:{from_user_id}, nickname:{nickname}")
            member_info_key = im_group_member_info_key(app_id, group_id)
            old_nickname = lanying_redis.redis_hget(redis, member_info_key, from_user_id)
            if old_nickname is None or old_nickname != nickname:
                logging.info(f"maybe_sync_wechat_user_group_info_to_im sync user nickname | app_id:{app_id}, wid:{wid}, wechat_group_id:{wechat_group_id}, group_id:{group_id}, from_user:{from_user}, from_user_id:{from_user_id}, nickname:{nickname}")
                success = sync_user_profile_to_lanying_user(app_id, from_user_id, nickname)
                if success:
                    redis.hset(member_info_key, from_user_id, nickname)
                else:
                    logging.info("maybe_sync_wechat_user_group_info_to_im fail to sync user nickname")
