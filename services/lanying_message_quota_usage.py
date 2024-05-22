import uuid
import time
import lanying_redis

def generate_trace_id():
    return f"{uuid.uuid4()}-{int(time.time()*100000)}"

def incr_usage(trace_id, usage):
    key = usage_key(trace_id)
    redis = lanying_redis.get_redis_connection()
    result = redis.incrbyfloat(key, usage)
    redis.expire(key, 3600)
    return result

def get_usage(trace_id):
    return incr_usage(trace_id, 0)

def usage_key(trace_id):
    return f"lanying_connector:message_quota_usage_trace:{trace_id}"