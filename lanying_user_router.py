import lanying_redis
import time
import logging

def handle_msg_route_to_im(app_id, channel, send_msg_user_id, router_user_id, router_sub_user_ids):
    if len(router_sub_user_ids) > 0:
        now = int(time.time())
        set_router_binding(app_id, channel, send_msg_user_id, router_user_id, router_user_id, now)
        for router_sub_user_id in router_sub_user_ids:
            set_router_binding(app_id, channel, send_msg_user_id, router_user_id, router_sub_user_id, now)
        router_state = get_router_state(app_id, channel, send_msg_user_id, router_user_id)
        if router_state:
            if 'redirect_to_user_id' in router_state:
                redirect_to_user_id = router_state['redirect_to_user_id']
                logging.info(f"handle_msg_route_to_im redirect to sub user | app_id:{app_id}, channel:{channel}, send_msg_user_id:{send_msg_user_id}, router_user_id:{router_user_id}, redirect_to_user_id:{redirect_to_user_id}")
                return {'result':'ok', 'from':send_msg_user_id, 'to': redirect_to_user_id}
    return {'result':'ok', 'from':send_msg_user_id, 'to': router_user_id}

def handle_msg_route_from_im(app_id, channel, from_user_id, to_user_id):
    binding_info = get_router_binding(app_id, from_user_id, to_user_id)
    if binding_info:
        router_user_id = binding_info['router_user_id']
        router_channel = binding_info['channel']
        if router_channel == channel:
            now = int(time.time())
            redis = lanying_redis.get_redis_connection()
            state_key = get_router_state_key(app_id, channel, to_user_id, router_user_id)
            redis.hmset(state_key, {
                'channel': channel,
                'channel_msg_time': now,
                'redirect_to_user_id': from_user_id
            })
            redis.expire(state_key, get_state_expire_time())
            if router_user_id != from_user_id:
                logging.info(f"handle_msg_route_from_im redirect from sub user| app_id:{app_id}, channel:{channel}, from_user_id:{from_user_id}, router_user_id:{router_user_id}, to_user_id:{to_user_id}")
            return {'result':'ok', 'from': router_user_id, 'to': to_user_id}
        else:
            return {'result': 'error', 'message': 'skip different channel message'}
    return {'result':'ok', 'from': from_user_id, 'to': to_user_id}

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
    router_binding_key = get_router_binding_key(app_id, to_user_id, from_user_id)
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

def set_router_binding(app_id, channel, send_msg_user_id, router_user_id, router_sub_user_id, now):
    redis = lanying_redis.get_redis_connection()
    router_binding_key = get_router_binding_key(app_id, send_msg_user_id, router_sub_user_id)
    redis.hmset(router_binding_key,{
        'channel': channel,
        'channel_msg_time': now,
        'router_user_id': router_user_id
    })
    redis.expire(router_binding_key, get_connection_expire_time())

def get_router_state_key(app_id, channel, send_msg_user_id, router_user_id):
    return f"lanying_connector:router_state:{app_id}:{channel}:{send_msg_user_id}:{router_user_id}"

def get_router_binding_key(app_id, send_msg_user_id, router_sub_user_id):
    return f"lanying_connector:router_binding:{app_id}:{send_msg_user_id}:{router_sub_user_id}"

def get_state_expire_time():
    return 86400

def get_connection_expire_time():
    return 86400 * 30
