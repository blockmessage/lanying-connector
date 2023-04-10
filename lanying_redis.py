import os
from redis import StrictRedis, ConnectionPool
import logging

redisServer = os.getenv('LANYING_CONNECTOR_REDIS_SERVER')
redisPool = None
if redisServer:
    redisPool = ConnectionPool.from_url(redisServer)

def get_redis_connection():
    conn = None
    if redisPool:
        conn = StrictRedis(connection_pool=redisPool)
    if not conn:
        logging.warning(f"get_redis_connection: fail to get connection")
    return conn
