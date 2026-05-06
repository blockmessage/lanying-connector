import logging
import random
import re
import time

import requests
from acme import errors as acme_errors

import lanying_cdn
import lanying_cert


def probe_http01_url(challenge_url):
    try:
        response = requests.get(challenge_url, timeout=(10.0, 10.0), allow_redirects=False)
        location = response.headers.get('Location', '')
        body_preview = response.text[:300] if response.text else ''
        return {
            'ok': True,
            'status_code': response.status_code,
            'location': location,
            'body_preview': body_preview
        }
    except Exception as e:
        return {
            'ok': False,
            'error': str(e) or repr(e)
        }


def wait_http01_ready(domain_name, max_attempts=5, retry_delay_sec=3):
    last_result = {'result': 'error', 'message': 'http_not_ready'}
    for i in range(max_attempts):
        last_result = precheck_http01_reachable(domain_name)
        if last_result.get('result') == 'ok':
            return last_result
        if i < max_attempts - 1:
            time.sleep(retry_delay_sec)
    return last_result


def try_issue_cert_with_retry(domain_name, max_attempts=1, retry_delay_sec=20):
    last_exception = None
    try:
        client_acme = lanying_cert.get_acme_client()
        pkey_pem, csr_pem = lanying_cert.new_csr_comp(domain_name)
        orderr = client_acme.new_order(csr_pem)
        challb = lanying_cert.select_http01_chall(orderr)
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'reason': str(e) or repr(e)
        }

    for i in range(max_attempts):
        try:
            challenge_token = challb.to_json().get('token', '')
            challenge_url = f'http://{domain_name}/.well-known/acme-challenge/{challenge_token}'
            probe_before = probe_http01_url(challenge_url)
            logging.info(
                f"try_issue_cert_with_retry perform_http01 start | domain_name:{domain_name}, attempt:{i + 1}/{max_attempts}, challenge_url:{challenge_url}, probe_before:{probe_before}"
            )
            finalized_orderr = lanying_cert.perform_http01(client_acme, challb, orderr)
            return {
                'result': 'ok',
                'pkey_pem': pkey_pem,
                'finalized_orderr': finalized_orderr
            }
        except Exception as e:
            last_exception = e
            challenge_token = challb.to_json().get('token', '')
            challenge_url = f'http://{domain_name}/.well-known/acme-challenge/{challenge_token}'
            probe_after = probe_http01_url(challenge_url)
            logging.info(
                f"try_issue_cert_with_retry perform_http01 failed | domain_name:{domain_name}, attempt:{i + 1}/{max_attempts}, challenge_url:{challenge_url}, probe_after:{probe_after}, error:{str(e) or repr(e)}"
            )
            logging.exception(e)
            if isinstance(e, acme_errors.ValidationError) and i < max_attempts - 1:
                time.sleep(retry_delay_sec)
                continue
            break
    return {
        'result': 'error',
        'reason': str(last_exception) or repr(last_exception) if last_exception else 'unknown_error'
    }


def is_valid_domain(domain):
    pattern = r'^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9-]{2,}$'
    return bool(re.match(pattern, domain))


def precheck_http01_reachable(domain_name):
    now_ts = int(time.time())
    test_key = f'{domain_name}_{now_ts}'
    test_value = f'{domain_name}_{now_ts}_{random.randint(1, 100000000)}'
    lanying_cert.set_acme_challenge_value(test_key, test_value)
    try:
        url = f'http://{domain_name}/.well-known/acme-challenge/{test_key}'
        logging.info(
            f"issue_and_bind_ssl_for_existing_cdn http check start | domain_name:{domain_name}, url:{url}"
        )
        response = requests.get(url, timeout=(10.0, 10.0), allow_redirects=False)
        if response.status_code in (301, 302, 307, 308):
            location = response.headers.get('Location', '')
            logging.info(
                f"issue_and_bind_ssl_for_existing_cdn http check redirect | domain_name:{domain_name}, location:{location}"
            )
            if location.startswith('http://') or location.startswith('https://'):
                response = requests.get(location, timeout=(10.0, 10.0), allow_redirects=False)
        logging.info(
            f"issue_and_bind_ssl_for_existing_cdn http check response | domain_name:{domain_name}, response:{response.text}"
        )
        if response.text == test_value:
            return {'result': 'ok'}
        return {'result': 'error', 'message': 'http_not_ready'}
    except Exception as e:
        logging.exception(e)
        return {'result': 'error', 'message': 'http_not_ready'}


def issue_and_bind_ssl_for_existing_cdn(domain_name, source_domain=None):
    logging.info(
        f"issue_and_bind_ssl_for_existing_cdn start | domain_name:{domain_name}, source_domain:{source_domain}"
    )
    if not is_valid_domain(domain_name):
        return {
            'result': 'error',
            'message': 'domain_name_invalid'
        }

    try:
        challenge_origin_host = source_domain or 'connector.lanyingim.com'
        lanying_cdn.set_cdn_http01_challenge_route(domain_name, challenge_origin_host)
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'challenge_route_config_failed'
        }

    http_ready = wait_http01_ready(domain_name)
    if http_ready['result'] != 'ok':
        return http_ready

    cert_result = try_issue_cert_with_retry(domain_name, max_attempts=1)
    if cert_result['result'] != 'ok':
        return {
            'result': 'error',
            'message': 'cert_not_ready',
            'reason': cert_result['reason']
        }
    pkey_pem = cert_result['pkey_pem']
    finalized_orderr = cert_result['finalized_orderr']

    try:
        cert_key = pkey_pem.decode('utf-8') if isinstance(pkey_pem, bytes) else pkey_pem
        lanying_cdn.set_cdn_domain_cert(domain_name, finalized_orderr.fullchain_pem, cert_key)
        return {
            'result': 'ok',
            'data': {
                'cert_pem': str(finalized_orderr.fullchain_pem),
                'cert_key': cert_key
            }
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'cert_config_not_ready'
        }
