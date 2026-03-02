import logging
import json
import os
import time
import base64
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

def _get_aliyun_access_keys():
    return (
        os.getenv('ALIYUN_ECS_ACCESS_KEY_ID'),
        os.getenv('ALIYUN_ECS_ACCESS_KEY_SECRET')
    )

def _build_user_data_for_openclaw_user():
    script = """#!/bin/bash
set -e
id -u openclaw >/dev/null 2>&1 || useradd -m -s /bin/bash openclaw
usermod -aG sudo openclaw 2>/dev/null || true
usermod -aG wheel openclaw 2>/dev/null || true
mkdir -p /home/openclaw/.ssh
if [ -f /root/.ssh/authorized_keys ]; then
  cp /root/.ssh/authorized_keys /home/openclaw/.ssh/authorized_keys
fi
chown -R openclaw:openclaw /home/openclaw/.ssh
chmod 700 /home/openclaw/.ssh
if [ -f /home/openclaw/.ssh/authorized_keys ]; then
  chmod 600 /home/openclaw/.ssh/authorized_keys
fi
echo 'openclaw ALL=(ALL) NOPASSWD:ALL' >/etc/sudoers.d/openclaw
chmod 440 /etc/sudoers.d/openclaw
loginctl enable-linger openclaw 2>/dev/null || true
"""
    return base64.b64encode(script.encode('utf-8')).decode('utf-8')

def test():
    buy_result = buy_ecs(
        instance_name="openclaw-node-test-1",
        region_id='cn-beijing',
        image_id='ubuntu_24_04_x64_20G_alibase_20260213.vhd',
        security_group_id='sg-2ze0mmb6uv5aphany7ct',
        vswitch_id='vsw-2zeryb0m63v0hgtzqdfd8',
        instance_type='ecs.c7.large',
        host_name="openclaw",
        key_pair_name='openclaw-node-create',
        zone_id='cn-beijing-l',
        vpc_id='vpc-2zex4fg7fqqedq23byzgx'
    )
    print(buy_result)
    if buy_result['result'] == 'ok':
        instance_ids = buy_result['data']['instance_ids']
        if len(instance_ids) > 0:
            region_id = 'cn-beijing'
            wait_result = wait_ecs_public_ip(region_id, instance_ids[0], 20, 2)
            print(wait_result)
    return buy_result

def buy_ecs(
    instance_name,
    region_id,
    image_id,
    security_group_id,
    vswitch_id,
    instance_type,
    vpc_id,
    host_name,
    key_pair_name,
    zone_id
):
    amount = 1
    access_key_id, access_key_secret = _get_aliyun_access_keys()
    host_name = host_name or instance_name

    required = {
        'ALIYUN_ACCESS_KEY_ID': access_key_id,
        'ALIYUN_ACCESS_KEY_SECRET': access_key_secret,
        'region_id': region_id,
        'image_id': image_id,
        'security_group_id': security_group_id,
        'vswitch_id': vswitch_id,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        return {
            'result': 'error',
            'message': 'missing_required_params',
            'data': {
                'missing': missing
            }
        }

    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('ecs.aliyuncs.com')
    request.set_version('2014-05-26')
    request.set_action_name('RunInstances')
    request.set_method('POST')
    request.add_query_param('RegionId', region_id)
    request.add_query_param('HostName', host_name)
    request.add_query_param('ImageId', image_id)
    request.add_query_param('InstanceType', instance_type)
    request.add_query_param('SecurityGroupId', security_group_id)
    request.add_query_param('KeyPairName', key_pair_name)
    request.add_query_param('UserData', _build_user_data_for_openclaw_user())
    request.add_query_param('VSwitchId', vswitch_id)
    request.add_query_param('ZoneId', zone_id)
    request.add_query_param('InstanceName', instance_name)
    request.add_query_param('InstanceChargeType', 'PostPaid')
    request.add_query_param('InternetChargeType', 'PayByTraffic')
    request.add_query_param('InternetMaxBandwidthIn', 100)
    request.add_query_param('InternetMaxBandwidthOut', 100)
    request.add_query_param('SystemDisk.Size', 40)
    request.add_query_param('SystemDisk.Category', 'cloud_essd')
    request.add_query_param('SpotStrategy', 'NoSpot')
    request.add_query_param('Amount', amount)
    request.add_query_param('VpcId', vpc_id)
    try:
        client = AcsClient(access_key_id, access_key_secret, region_id)
        logging.info(f"buy ecs start | request:{request}")
        response = client.do_action_with_exception(request)
        response_json = json.loads(response)
        logging.info(f"buy ecs finish | response:{response_json}")
        instance_ids = response_json.get('InstanceIdSets', {}).get('InstanceIdSet', [])
        return {
            'result': 'ok',
            'data': {
                'request_id': response_json.get('RequestId'),
                'order_id': response_json.get('OrderId'),
                'trade_price': response_json.get('TradePrice'),
                'instance_ids': instance_ids
            }
        }
    except Exception as e:
        logging.exception(f"buy_aliyun_ecs failed | error={e}")
        return {
            'result': 'error',
            'message': 'fail_to_buy_ecs'
        }

def describe_instances(region_id, instance_ids):
    access_key_id, access_key_secret = _get_aliyun_access_keys()
    required = {
        'ALIYUN_ACCESS_KEY_ID': access_key_id,
        'ALIYUN_ACCESS_KEY_SECRET': access_key_secret,
        'region_id': region_id,
        'instance_ids': instance_ids,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        return {
            'result': 'error',
            'message': 'missing_required_params',
            'data': {
                'missing': missing
            }
        }

    if isinstance(instance_ids, str):
        instance_ids = [instance_ids]

    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('ecs.aliyuncs.com')
    request.set_version('2014-05-26')
    request.set_action_name('DescribeInstances')
    request.set_method('POST')
    request.add_query_param('RegionId', region_id)
    request.add_query_param('InstanceIds', json.dumps(instance_ids, ensure_ascii=False))

    try:
        client = AcsClient(access_key_id, access_key_secret, region_id)
        logging.info(f"describe ecs start | request:{request}")
        response = client.do_action_with_exception(request)
        response_json = json.loads(response)
        logging.info(f"describe ecs finish | response:{response_json}")
        return {
            'result': 'ok',
            'data': {
                'request_id': response_json.get('RequestId'),
                'total_count': response_json.get('TotalCount', 0),
                'instances': response_json.get('Instances', {}).get('Instance', [])
            }
        }
    except Exception as e:
        logging.exception(f"describe_instances failed | error={e}")
        return {
            'result': 'error',
            'message': 'fail_to_describe_instances'
        }

def delete_ecs(region_id, instance_id):
    access_key_id, access_key_secret = _get_aliyun_access_keys()
    required = {
        'ALIYUN_ACCESS_KEY_ID': access_key_id,
        'ALIYUN_ACCESS_KEY_SECRET': access_key_secret,
        'region_id': region_id,
        'instance_id': instance_id,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        return {
            'result': 'error',
            'message': 'missing_required_params',
            'data': {
                'missing': missing
            }
        }

    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('ecs.aliyuncs.com')
    request.set_version('2014-05-26')
    request.set_action_name('DeleteInstance')
    request.set_method('POST')
    request.add_query_param('RegionId', region_id)
    request.add_query_param('InstanceId', instance_id)
    request.add_query_param('Force', True)

    try:
        client = AcsClient(access_key_id, access_key_secret, region_id)
        logging.info(f"delete ecs start | request:{request}")
        response = client.do_action_with_exception(request)
        response_json = json.loads(response)
        logging.info(f"delete ecs finish | response:{response_json}")
        return {
            'result': 'ok',
            'data': {
                'request_id': response_json.get('RequestId'),
                'instance_id': instance_id
            }
        }
    except Exception as e:
        logging.exception(f"delete_ecs failed | error={e}")
        return {
            'result': 'error',
            'message': 'fail_to_delete_ecs'
        }

def _extract_public_ip(instance):
    public_ip_list = instance.get('PublicIpAddress', {}).get('IpAddress', [])
    if isinstance(public_ip_list, list) and len(public_ip_list) > 0:
        ip = str(public_ip_list[0]).strip()
        if ip:
            return ip

    eip_ip = str(instance.get('EipAddress', {}).get('IpAddress', '')).strip()
    if eip_ip:
        return eip_ip
    return ''

def wait_ecs_public_ip(region_id, instance_id, max_attempts=30, interval_seconds=2):
    if max_attempts <= 0:
        return {
            'result': 'error',
            'message': 'invalid_max_attempts'
        }
    if interval_seconds < 0:
        return {
            'result': 'error',
            'message': 'invalid_interval_seconds'
        }

    last_message = ''
    for attempt in range(1, max_attempts + 1):
        result = describe_instances(region_id, [instance_id])
        if result.get('result') != 'ok':
            last_message = result.get('message', 'fail_to_describe_instances')
            logging.warning(
                f"wait_ecs_public_ip describe failed | region_id:{region_id}, "
                f"instance_id:{instance_id}, attempt:{attempt}, message:{last_message}"
            )
        else:
            instances = result.get('data', {}).get('instances', [])
            if len(instances) > 0:
                ip = _extract_public_ip(instances[0])
                if ip:
                    return {
                        'result': 'ok',
                        'data': {
                            'instance_id': instance_id,
                            'public_ip': ip,
                            'attempt': attempt
                        }
                    }
            last_message = 'public_ip_not_ready'

        if attempt < max_attempts:
            time.sleep(interval_seconds)

    return {
        'result': 'error',
        'message': 'timeout_wait_public_ip',
        'data': {
            'instance_id': instance_id,
            'attempts': max_attempts,
            'last_message': last_message
        }
    }
