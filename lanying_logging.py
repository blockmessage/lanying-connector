import logging
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
import json
import os
import re
import socket
import time
import uuid


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
    r'(?i)(authorization|access[_-]?token|refresh[_-]?token|'
    r'(?:[a-z0-9]+[_-])*token|api[_-]?key|client[_-]?secret|secret[_-]?key|'
    r'password|credential|private[_-]?key|signature|file[_-]?sign|'
    r'x-oss-credential|x-oss-signature)=([^&\s]+)'
)
_AUTH_VALUE_PATTERN = re.compile(r'(?i)\b(Bearer|Basic)\s+[^\s,}\]]+')
_SENSITIVE_KEY_PATTERN = re.compile(
    r'(?i)(^|[_-])(access[_-]?token|refresh[_-]?token|token|'
    r'api[_-]?key|client[_-]?secret|secret|secret[_-]?key|password|credential|'
    r'private[_-]?key|signature|cookie|file[_-]?sign)$')

HTTP_LOG_MAX_CHARS = 20000
HTTP_LOG_MAX_BODY_BYTES = 128 * 1024
HTTP_LOG_FILE_BACKUP_COUNT = 365


def _is_sensitive_key(key):
    normalized = str(key).lower()
    return normalized in _SENSITIVE_KEYS or bool(
        _SENSITIVE_KEY_PATTERN.search(normalized))


def redact_sensitive_log_value(value):
    if isinstance(value, dict):
        return {
            key: (
                redact_log_container(item)
                if str(key).lower() in _SENSITIVE_CONTAINERS
                else ('[REDACTED]'
                      if _is_sensitive_key(key)
                      else redact_sensitive_log_value(item)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive_log_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_log_value(item) for item in value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(('{', '[')):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, (dict, list)):
                    return json.dumps(
                        redact_sensitive_log_value(parsed),
                        ensure_ascii=False, separators=(',', ':'))
            except (TypeError, ValueError):
                pass
        value = _SENSITIVE_QUERY_PATTERN.sub(r'\1=[REDACTED]', value)
        return _AUTH_VALUE_PATTERN.sub(r'\1 [REDACTED]', value)
    return value


def redact_log_container(value):
    if isinstance(value, dict):
        return {key: '[REDACTED]' for key in value}
    if isinstance(value, (list, tuple)):
        return ['[REDACTED]'] if value else []
    return '[REDACTED]'


def format_log_value(value, max_chars=HTTP_LOG_MAX_CHARS):
    """Serialize a value for logs after recursively removing credentials."""
    try:
        text = json.dumps(
            redact_sensitive_log_value(value), ensure_ascii=False,
            separators=(',', ':'), default=str)
    except Exception:
        text = str(redact_sensitive_log_value(value))
    limit = max(256, int(max_chars or HTTP_LOG_MAX_CHARS))
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return f'{text[:limit]}... [truncated {omitted} chars]'


def _request_body_for_log(request):
    content_length = request.content_length
    if content_length is not None and content_length > HTTP_LOG_MAX_BODY_BYTES:
        return {
            '_omitted': 'request body exceeds log limit',
            'content_length': content_length,
        }
    if request.is_json:
        value = request.get_json(silent=True)
        if value is not None:
            return value
        raw = request.get_data(cache=True)
        return raw.decode('utf-8', errors='replace') if raw else None
    mimetype = str(request.mimetype or '')
    if mimetype.startswith('multipart/'):
        return {
            '_omitted': 'multipart request body',
            'content_length': content_length,
        }
    if request.form:
        return {'form': request.form.to_dict(flat=False)}
    raw = request.get_data(cache=True)
    if not raw:
        return None
    if mimetype.startswith('text/') or mimetype in {
            'application/xml', 'application/x-www-form-urlencoded'}:
        charset = request.mimetype_params.get('charset', 'utf-8')
        return raw.decode(charset, errors='replace')
    return {'_omitted': 'non-text request body', 'content_length': len(raw)}


def _response_body_for_log(response):
    if response.is_streamed or response.direct_passthrough:
        return {'_omitted': 'streaming response'}
    content_length = response.content_length
    if content_length is not None and content_length > HTTP_LOG_MAX_BODY_BYTES:
        return {
            '_omitted': 'response body exceeds log limit',
            'content_length': content_length,
        }
    mimetype = str(response.mimetype or '')
    if mimetype == 'application/json' or mimetype.endswith('+json'):
        value = response.get_json(silent=True)
        return value if value is not None else response.get_data(as_text=True)
    if mimetype.startswith('text/'):
        return response.get_data(as_text=True)
    data = response.get_data()
    if not data:
        return None
    return {'_omitted': 'non-text response body', 'content_length': len(data)}


def _request_path_for_log(request):
    view_args = request.view_args or {}
    if request.url_rule is not None and any(
            _is_sensitive_key(key) for key in view_args):
        return request.url_rule.rule
    return request.path


def register_http_logging(app):
    """Log Flask API request/response pairs without exposing credentials."""
    if app.extensions.get('lanying_http_logging'):
        return
    app.extensions['lanying_http_logging'] = True

    from flask import g, request

    @app.before_request
    def log_connector_http_request():
        g.connector_http_request_id = uuid.uuid4().hex[:16]
        g.connector_http_started_at = time.monotonic()
        try:
            logging.info(
                'connector http request | request_id:%s | method:%s | path:%s '
                '| remote:%s | content_type:%s | query:%s | body:%s',
                g.connector_http_request_id,
                request.method,
                _request_path_for_log(request),
                request.remote_addr or '',
                request.content_type or '',
                format_log_value(request.args.to_dict(flat=False)),
                format_log_value(_request_body_for_log(request)))
        except Exception:
            logging.warning('failed to log connector http request', exc_info=True)

    @app.after_request
    def log_connector_http_response(response):
        request_id = getattr(g, 'connector_http_request_id', '')
        started_at = getattr(g, 'connector_http_started_at', None)
        duration_ms = ((time.monotonic() - started_at) * 1000
                       if started_at is not None else -1)
        try:
            logging.info(
                'connector http response | request_id:%s | method:%s | path:%s '
                '| status:%s | duration_ms:%.1f | content_type:%s | body:%s',
                request_id,
                request.method,
                _request_path_for_log(request),
                response.status_code,
                duration_ms,
                response.content_type or '',
                format_log_value(_response_body_for_log(response)))
        except Exception:
            logging.warning(
                'failed to log connector http response | request_id:%s',
                request_id, exc_info=True)
        return response


class RedactingFormatter(logging.Formatter):
    def format(self, record):
        return redact_sensitive_log_value(super().format(record))

def init_logging():
    logdir = f"log/{socket.gethostname()}"
    os.makedirs(logdir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = ConcurrentTimedRotatingFileHandler(
        filename=f'{logdir}/info.log',
        when='midnight',
        interval=1,
        backupCount=HTTP_LOG_FILE_BACKUP_COUNT,
        use_gzip=True,
        encoding='utf-8')
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
