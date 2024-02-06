import lanying_redis
import time
import logging

def handle_msg_route_to_im(app_id, channel, send_msg_user_id, router_user_id, router_sub_user_ids):
    send_msg_user_id = str(send_msg_user_id)
    router_user_id = str(router_user_id)
    msg_type = 'CHAT'
    if len(router_sub_user_ids) > 0:
        now = int(time.time())
        set_router_binding(app_id, channel, router_user_id, send_msg_user_id, router_user_id, send_msg_user_id, 'CHAT', send_msg_user_id, router_user_id, send_msg_user_id, router_user_id, 'CHAT', now)
        for router_sub_user_id in router_sub_user_ids:
            router_sub_user_id = str(router_sub_user_id)
            set_router_binding(app_id, channel, router_sub_user_id, send_msg_user_id, router_user_id, send_msg_user_id, 'CHAT', send_msg_user_id, router_user_id, send_msg_user_id, router_sub_user_id, 'CHAT', now)
        router_state = get_router_state(app_id, channel, send_msg_user_id, router_user_id)
        if router_state:
            new_from_user_id = router_state['new_from_user_id']
            new_to_user_id = router_state['new_to_user_id']
            new_msg_type = router_state['new_msg_type']
            if new_from_user_id != send_msg_user_id or new_to_user_id != router_user_id or msg_type != 'CHAT':
                logging.info(f"handle_msg_route_to_im redirect | app_id:{app_id}, channel:{channel}, msg_type:{msg_type}, send_msg_user_id:{send_msg_user_id}, router_user_id:{router_user_id}, new_msg_type:{new_msg_type}, new_from_user_id:{new_from_user_id}, new_to_user_id:{new_to_user_id}")
            return {'result':'ok', 'from':new_from_user_id, 'to': new_to_user_id, 'msg_type': new_msg_type}
    return {'result':'ok', 'from':send_msg_user_id, 'to': router_user_id, 'msg_type': msg_type}

def handle_group_msg_route_to_im(app_id, channel, send_msg_user_id, router_user_id, router_sub_user_ids, group_id):
    send_msg_user_id = str(send_msg_user_id)
    router_user_id = str(router_user_id)
    group_id = str(group_id)
    msg_type = 'GROUPCHAT'
    if len(router_sub_user_ids) > 0:
        now = int(time.time())
        set_router_binding(app_id, channel, router_user_id, send_msg_user_id, router_user_id, group_id, 'GROUPCHAT', send_msg_user_id, group_id, send_msg_user_id, group_id, 'GROUPCHAT', now)
        for router_sub_user_id in router_sub_user_ids:
            router_sub_user_id = str(router_sub_user_id)
            set_router_binding(app_id, channel, router_sub_user_id, send_msg_user_id, router_user_id, group_id, 'GROUPCHAT', send_msg_user_id, group_id, send_msg_user_id, router_sub_user_id, 'CHAT', now)
        router_state = get_router_state(app_id, channel, send_msg_user_id, group_id)
        if router_state:
            new_from_user_id = router_state['new_from_user_id']
            new_to_user_id = router_state['new_to_user_id']
            new_msg_type = router_state['new_msg_type']
            if new_from_user_id != send_msg_user_id or new_to_user_id != group_id or new_msg_type != msg_type:
                logging.info(f"handle_msg_route_to_im redirect | app_id:{app_id}, channel:{channel}, msg_type:{msg_type}, send_msg_user_id:{send_msg_user_id}, group_id:{group_id}, router_user_id:{router_user_id}, new_msg_type:{new_msg_type}, new_from_user_id:{new_from_user_id}, new_to_user_id:{new_to_user_id}")
            return {'result':'ok', 'from':new_from_user_id, 'to': new_to_user_id, 'msg_type': new_msg_type}
    return {'result':'ok', 'from':send_msg_user_id, 'to': group_id, 'msg_type': msg_type}

def handle_msg_route_from_im(app_id, channel, from_user_id, to_user_id, msg_type):
    from_user_id = str(from_user_id)
    to_user_id = str(to_user_id)
    if msg_type in ['CHAT', 'GROUPCHAT', 'REPLACE', 'APPEND']:
        binding_info = get_router_binding(app_id, from_user_id, to_user_id)
        if binding_info:
            new_from_user_id = binding_info['new_from_user_id']
            new_to_user_id = binding_info['new_to_user_id']
            new_msg_type = binding_info['new_msg_type']
            router_state_from_user_id = binding_info['router_state_from_user_id']
            router_state_to_user_id = binding_info['router_state_to_user_id']
            router_state_new_from_user_id = binding_info['router_state_new_from_user_id']
            router_state_new_to_user_id = binding_info['router_state_new_to_user_id']
            router_state_new_msg_type = binding_info['router_state_new_msg_type']
            router_channel = binding_info['channel']
            if msg_type in ['REPLACE', 'APPEND']:
                new_msg_type = msg_type
            if router_channel == channel:
                now = int(time.time())
                redis = lanying_redis.get_redis_connection()
                state_key = get_router_state_key(app_id, channel, router_state_from_user_id, router_state_to_user_id)
                redis.hmset(state_key, {
                    'channel': channel,
                    'channel_msg_time': now,
                    'new_from_user_id': router_state_new_from_user_id,
                    'new_to_user_id': router_state_new_to_user_id,
                    'new_msg_type': router_state_new_msg_type
                })
                redis.expire(state_key, get_state_expire_time())
                if new_from_user_id != from_user_id or new_to_user_id != to_user_id  or msg_type != new_msg_type:
                    logging.info(f"handle_msg_route_from_im redirect | app_id:{app_id}, channel:{channel}, msg_type:{msg_type}, from_user_id:{from_user_id}, to_user_id:{to_user_id}, new_msg_type:{new_msg_type}, new_from_user_id:{new_from_user_id}, new_to_user_id:{new_to_user_id}")
                return {'result':'ok', 'from': new_from_user_id, 'to': new_to_user_id, 'msg_type': new_msg_type}
            else:
                return {'result': 'error', 'message': 'skip different channel message'}
    return {'result':'ok', 'from': from_user_id, 'to': to_user_id, 'msg_type':  msg_type}

def get_router_state(app_id, channel, send_msg_user_id, router_user_id):
    redis = lanying_redis.get_redis_connection()
    key = get_router_state_key(app_id, channel, send_msg_user_id, router_user_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if len(info) > 0:
        dto = {}
        for k,v in info.items():
            if k in ['channel_msg_time']:
                dto[key] = int(v)
            else:
                dto[key] = v
        return info
    return None

def get_router_binding(app_id, from_user_id, to_user_id):
    router_binding_key = get_router_binding_key(app_id, from_user_id, to_user_id)
    redis = lanying_redis.get_redis_connection()
    info = lanying_redis.redis_hgetall(redis, router_binding_key)
    if len(info) > 0:
        dto = {}
        for k,v in info.items():
            if k in ['channel_msg_time']:
                dto[k] = int(v)
            else:
                dto[k] = v
        return dto
    return None

def set_router_binding(app_id, channel, from_user_id, to_user_id, new_from_user_id, new_to_user_id, new_msg_type, router_state_from_user_id, router_state_to_user_id, router_state_new_from_user_id, router_state_new_to_user_id, router_state_new_msg_type, now):
    redis = lanying_redis.get_redis_connection()
    router_binding_key = get_router_binding_key(app_id, from_user_id, to_user_id)
    redis.hmset(router_binding_key,{
        'channel': channel,
        'channel_msg_time': now,
        'new_from_user_id': new_from_user_id,
        'new_to_user_id':  new_to_user_id,
        'new_msg_type': new_msg_type,
        'router_state_from_user_id': router_state_from_user_id,
        'router_state_to_user_id': router_state_to_user_id,
        'router_state_new_from_user_id': router_state_new_from_user_id,
        'router_state_new_to_user_id': router_state_new_to_user_id,
        'router_state_new_msg_type': router_state_new_msg_type
    })
    redis.expire(router_binding_key, get_connection_expire_time())

def get_router_state_key(app_id, channel, send_msg_user_id, router_user_id):
    return f"lanying_connector:router_state:v2:{app_id}:{channel}:{send_msg_user_id}:{router_user_id}"

def get_router_binding_key(app_id, from_user_id, to_user_id):
    return f"lanying_connector:router_binding:v2:{app_id}:{from_user_id}:{to_user_id}"

def get_state_expire_time():
    return 86400

def get_connection_expire_time():
    return 86400 * 30
