import socket
import ipaddress
from urllib.parse import urlparse
import logging

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
