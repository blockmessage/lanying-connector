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
