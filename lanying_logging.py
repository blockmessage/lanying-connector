import logging
import os
import re
import socket


_SENSITIVE_KEYS = {
    'authorization',
    'access_token',
    'access-token',
    'api_key',
    'api-key',
    'client_secret',
    'client-secret',
    'secret_key',
    'secret-key',
    'password',
    'x-oss-credential',
    'x-oss-signature',
}
_SENSITIVE_CONTAINERS = {
    'auth',
    'body',
    'envs',
    'headers',
    'params',
}
_SENSITIVE_QUERY_PATTERN = re.compile(
    r'(?i)(authorization|access[_-]?token|api[_-]?key|client[_-]?secret|'
    r'secret[_-]?key|password|x-oss-credential|x-oss-signature)=([^&\s]+)'
)
_AUTH_VALUE_PATTERN = re.compile(r'(?i)\b(Bearer|Basic)\s+[^\s,}\]]+')


def redact_sensitive_log_value(value):
    if isinstance(value, dict):
        return {
            key: (
                redact_log_container(item)
                if str(key).lower() in _SENSITIVE_CONTAINERS
                else ('[REDACTED]'
                      if str(key).lower() in _SENSITIVE_KEYS
                      else redact_sensitive_log_value(item)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive_log_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_log_value(item) for item in value)
    if isinstance(value, str):
        value = _SENSITIVE_QUERY_PATTERN.sub(r'\1=[REDACTED]', value)
        return _AUTH_VALUE_PATTERN.sub(r'\1 [REDACTED]', value)
    return value


def redact_log_container(value):
    if isinstance(value, dict):
        return {key: '[REDACTED]' for key in value}
    if isinstance(value, (list, tuple)):
        return ['[REDACTED]'] if value else []
    return '[REDACTED]'


class RedactingFormatter(logging.Formatter):
    def format(self, record):
        return redact_sensitive_log_value(super().format(record))

def init_logging():
    logdir = f"log/{socket.gethostname()}"
    os.makedirs(logdir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(f'{logdir}/info.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = RedactingFormatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)
    # urllib3 includes full request URLs in DEBUG logs. Some vendor APIs carry
    # credentials in their query string, so those lines must never be emitted.
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)

def debug():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
