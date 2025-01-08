import os
import sys
import logging
from typing import List
import json
import time

from alibabacloud_cdn20180510.client import Client as Cdn20180510Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_cdn20180510 import models as cdn_20180510_models
from alibabacloud_tea_util import models as util_models

def test():
    pass

def create_client() -> Cdn20180510Client:
    config = open_api_models.Config(
        access_key_id=os.environ['ALIBABA_CLOUD_CDN_ACCESS_KEY_ID'],
        access_key_secret=os.environ['ALIBABA_CLOUD_CDN_ACCESS_KEY_SECRET']
    )
    config.endpoint = f'cdn.aliyuncs.com'
    return Cdn20180510Client(config)

def add_cdn(domain_name, source_domain, scope='global'):
    logging.info(f"Add cdn | domain_name:{domain_name}, source_domain:{source_domain}, scope:{scope}")
    try:
        client = create_client()
        sources = json.dumps([
            {
                "content": source_domain,
                "type": "domain",
                "priority": "100",
                "port": 443,
                "weight": "100"
            }
        ])
        add_cdn_domain_request = cdn_20180510_models.AddCdnDomainRequest(
            cdn_type='web',
            domain_name=domain_name,
            sources = sources,
            scope = scope
        )
        response = client.add_cdn_domain_with_options(add_cdn_domain_request, util_models.RuntimeOptions())
        logging.info(f"add_cdn response: {response}")
        return {
            'result': 'ok'
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'fail_to_create_cdn'
        }

def desc_cdn(domain_name):
    client = create_client()
    result = client.describe_cdn_domain_detail(cdn_20180510_models.DescribeCdnDomainDetailRequest(domain_name = domain_name))
    show_result(result)
    return result

def desc_cdn_config(domain_name):
    client = create_client()
    result = client.describe_cdn_domain_configs(cdn_20180510_models.DescribeCdnDomainConfigsRequest(domain_name=domain_name))
    show_result(result)
    return result

def desc_cdn_cname(domain_name):
    client = create_client()
    result = client.describe_domain_cname(cdn_20180510_models.DescribeDomainCnameRequest(domain_name = domain_name))
    show_result(result)
    return result

def show_result(result):
    try:
        logging.info(json.dumps(result.to_map(),indent=2))
    except Exception as e:
        logging.exception("exception:", e)
        logging.info(result)

def verify_domain_owner(domain_name, verify_type='dnsCheck'):
    try:
        client = create_client()
        result = client.verify_domain_owner(cdn_20180510_models.VerifyDomainOwnerRequest(domain_name=domain_name, verify_type=verify_type))
        show_result(result)
        return True
    except Exception as e:
        logging.exception(e)
        return False

def describe_domain_verify_data(domain_name):
    try:
        client = create_client()
        result = client.describe_domain_verify_data(cdn_20180510_models.DescribeDomainVerifyDataRequest(domain_name=domain_name))
        show_result(result)
        return {
            'result': 'ok',
            'data': {
                'verify_code': result.body.content['verifyCode'],
                'root_domain': result.body.content['RootDomain'],
                'verify_key': result.body.content['verifyKey']
            }
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'failed_to_get_domain_verify_data'
        }

def describe_cdn_user_quota():
    client = create_client()
    result = client.describe_cdn_user_quota(cdn_20180510_models.DescribeCdnUserQuotaRequest())
    show_result(result)
    return result

def delete_cdn_domain(domain_name):
    client = create_client()
    result = client.delete_cdn_domain(cdn_20180510_models.DeleteCdnDomainRequest(domain_name = domain_name))
    show_result(result)
    return result

def set_cdn_domain_config(domain_name, origin_host, sub_dir):
    client = create_client()
    functions = [
        {
            "functionArgs": [
                {"argName": "domain_name","argValue": origin_host}
            ],
            "functionName": "set_req_host_header"
        },
        {
            "functionArgs": [
                {"argName": "disable","argValue": "on"},
                {"argName": "keep_oss_args","argValue": "on"}
            ],
            "functionName": "set_hashkey_args"
        },
        {
            "functionArgs": [
                {"argName": "enable","argValue": "off"}
            ],
            "functionName": "range"
        },
        # {
        #     "functionArgs": [
        #         {"argName": "enable","argValue": "on"},
        #         {"argName": "scheme_origin","argValue": "https"},
        #         {"argName": "scheme_origin_port","argValue": "443"}
        #     ],
        #     "functionName": "forward_scheme"
        # },
        {
            "functionArgs": [
                {"argName": "enable","argValue": "on"},
                {"argName": "trim_js","argValue": "on"},
                {"argName": "trim_css","argValue": "on"}
            ],
            "functionName": "tesla"
        },
        {
            "functionArgs": [
                {"argName": "enable","argValue": "on"}
            ],
            "functionName": "gzip"
        },
        {
            "functionArgs": [
                {"argName": "path","argValue": "/"},
                {"argName": "ttl","argValue": "2592000"},
                {"argName": "weight","argValue": "99"}
            ],
            "functionName": "path_based_ttl_set"
        },
        {
            "functionArgs": [
                {"argName": "enable","argValue": "on"},
                {"argName": "name","argValue": "redirect_to_sub_site"},
                {"argName": "pos","argValue": "head"},
                {"argName": "pri","argValue": "0"},
                {"argName": "rule","argValue": "if match_re($uri, '^/\\.well-known/acme-challenge') {\n    rewrite(concat('https://connector.lanyingim.com',$uri), 'redirect')\n} else {\n    sub_dir = '/"+ sub_dir + "'\n    dst = concat(sub_dir, $uri)\n    rewrite(dst, 'break')\n}"},
                {"argName": "brk","argValue": "off"}
            ],
            "functionName": "edge_function"
        },
    ]
    functions_str = json.dumps(functions)
    result = client.batch_set_cdn_domain_config(cdn_20180510_models.BatchSetCdnDomainConfigRequest(domain_names=domain_name, functions=functions_str))
    show_result(result)
    return result

def set_cdn_domain_cert(domain_name, ssl_pem, ssl_key):
    client = create_client()
    req = cdn_20180510_models.SetCdnDomainSSLCertificateRequest(
        domain_name = domain_name,
        cert_type = 'upload',
        cert_name = 'auto_upload_'+str(int(time.time() * 1000)),
        sslprotocol = 'on',
        sslpub = ssl_pem,
        sslpri = ssl_key
    )
    result = client.set_cdn_domain_sslcertificate(req)
    show_result(result)
    return result
