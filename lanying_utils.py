import socket
import ipaddress
from urllib.parse import urlparse
import logging
import json
import hashlib
import time
import uuid
import os
import random
import string


def is_valid_public_url(url):
    if url.startswith('http://') or url.startswith('https://'):
        try:
            parse_url= urlparse(url.strip(' '))
            domain = parse_url.netloc
            ip_addresses = get_ip_addresses(domain)
            if not ip_addresses:
                logging.info(f"check is public url:{url} | no address")
                return False
            for ip in ip_addresses:
                if is_public_ip(ip):
                    logging.info(f"check is public url:{url} | {ip} is a public IP address.")
                    return True
                else:
                    logging.info(f"check is public url:{url} | {ip} is a private IP address.")
            return False
        except Exception as e:
            logging.info(f"check is public url:{url} | exception")
            return False
    logging.info(f"check is public url:{url} | is not url")
    return False

def is_public_ip(ip_address):
    try:
        ip_obj = ipaddress.ip_address(ip_address)
        return not ip_obj.is_private
    except ValueError:
        return False

def get_ip_addresses(domain):
    try:
        ip_addresses = socket.getaddrinfo(domain, None)
        return [ip[4][0] for ip in ip_addresses]
    except socket.gaierror:
        return []

def bool_to_str(value):
    if value == True:
        return 'true'
    return 'false'

def str_to_bool(value):
    if value == "True" or value == "true":
        return True
    return False

def safe_json_loads(str, default={}):
    try:
        return json.loads(str)
    except Exception as e:
        return default

def sha256(text):
    value = hashlib.sha256(text.encode('utf-8')).hexdigest()
    return value

def is_lanying_url(url):
    parsed = urlparse(url)
    host = parsed.netloc.split(':')[0]
    return host.endswith(".maximtop.cn") or host.endswith("api.maximtop.com")

def get_temp_filename(app_id, suffix):
    return f"/tmp/{app_id}_{int(time.time())}_{uuid.uuid4()}{suffix}"

def is_preview_server():
    server = os.getenv("EMBEDDING_LANYING_CONNECTOR_SERVER", "https://lanying-connector.lanyingim.com")
    return 'preview' in server

def get_internet_connector_server():
    server = os.getenv("LANYING_CONNECTOR_INTERNET_SERVER", "https://connector.lanyingim.com")
    return server

def generate_random_text(size_in_bytes):
    characters = string.ascii_letters + string.digits + string.punctuation + ' '  # 包含所有可能的字符
    random_text = ''.join(random.choices(characters, k=size_in_bytes))  # 生成随机文本
    return random_text

def generate_random_letters(size_in_bytes):
    characters = string.ascii_lowercase # 包含所有可能的字符
    random_text = ''.join(random.choices(characters, k=size_in_bytes))  # 生成随机文本
    return random_text