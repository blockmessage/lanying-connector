import os
from redis import StrictRedis, ConnectionPool
import logging

redisServer = os.getenv('LANYING_CONNECTOR_REDIS_SERVER', "redis://localhost:6379")
redisPool = None
if redisServer:
    redisPool = ConnectionPool.from_url(redisServer)

redisStackServer = os.getenv('LANYING_CONNECTOR_REDIS_STACK_SERVER', "redis://localhost:6379")
redisStackPool = None
if redisStackServer:
    redisStackPool = ConnectionPool.from_url(redisStackServer)

def get_redis_connection():
    conn = None
    if redisPool:
        conn = StrictRedis(connection_pool=redisPool)
    if not conn:
        logging.warning(f"get_redis_connection: fail to get connection")
    return conn

def get_redis_stack_connection():
    conn = None
    if redisStackPool:
        conn = StrictRedis(connection_pool=redisStackPool)
    if not conn:
        logging.warning(f"get_redis_stack_connection: fail to get connection")
    return conn

def get_task_redis_server():
    return os.getenv('LANYING_CONNECTOR_TASK_REDIS_SERVER', "redis://localhost:6379")

def redis_lrange(redis, key, start, end):
    return [bytes.decode('utf-8') for bytes in redis.lrange(key, start, end)]

def redis_hkeys(redis, key):
    return [bytes.decode('utf-8') for bytes in redis.hkeys(key)]

def redis_hgetall(redis, key):
    kvs = redis.hgetall(key)
    ret = {}
    if kvs:
        for k,v in kvs.items():
            ret[k.decode('utf-8')] = v.decode('utf-8')
    return ret

def redis_hget(redis, key, field):
    result = redis.hget(key, field)
    if result:
        return result.decode('utf-8')
    return None

def redis_get(redis, key):
    result = redis.get(key)
    if result:
        return result.decode('utf-8')
    return None
