import os
import josepy as jose
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from acme import client, messages
from josepy import JWKRSA
from acme import client, errors
from acme.client import ClientNetwork, ClientV2
from acme import messages
import OpenSSL
from acme import crypto_util
from acme import challenges
import json
import sys
import hashlib
import base64
import lanying_redis

# 配置常量
ACC_KEY_BITS = 2048
USER_AGENT = "python-acme"
DIRECTORY_URL = 'https://acme-v02.api.letsencrypt.org/directory' # 正式环境 
PRIVATE_KEY_FILE = "account.pem"
CERT_PKEY_BITS = 2048

def get_acme_challenge_value(key):
    redis = lanying_redis.get_redis_connection()
    result = lanying_redis.redis_get(redis, make_acme_challenge_key(key))
    if result:
        return {
            'result': 'ok',
            'value': result
        }
    else:
        return {
            'result': 'error',
            'reason': 'not_found'
        }

def set_acme_challenge_value(key, value):
    redis = lanying_redis.get_redis_connection()
    redis.set(make_acme_challenge_key(key), value)

def make_acme_challenge_key(key):
    return f'lanying-connector:acme_challenge_key:{key}'

# 生成 RSA 密钥并保存
def generate_and_save_key():
    # 使用 cryptography 创建 RSA 密钥
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=ACC_KEY_BITS,
        backend=default_backend()
    )

    # 使用 josepy 创建 JWKRSA 对象
    acc_key = jose.JWKRSA(key=private_key)

    # 导出私钥并保存为 PEM 格式
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,  # 直接使用 cryptography 库的 Encoding
        format=serialization.PrivateFormat.TraditionalOpenSSL,  # 使用传统的 OpenSSL 格式
        encryption_algorithm=serialization.NoEncryption()  # 不加密私钥
    )
    
    # 保存 PEM 格式的私钥
    with open(PRIVATE_KEY_FILE, "wb") as key_file:
        key_file.write(private_pem)

    return acc_key

def load_private_key_from_file():
    with open(PRIVATE_KEY_FILE, "rb") as key_file:
        private_pem = key_file.read()
        return base64.b64encode(private_pem)

# 从文件加载 RSA 私钥
def load_private_key():
    private_pem = base64.b64decode(os.getenv('LANYING_CONNECTOR_CERT_PRIVATE_KEY'))

    # 使用 cryptography 加载 PEM 格式的私钥
    private_key = serialization.load_pem_private_key(private_pem, password=None, backend=default_backend())
    
    # 将 cryptography 的私钥转换为 josepy 的 JWKRSA 对象
    acc_key = JWKRSA(key=private_key)
    return acc_key

def load_raw_private_key():
    # with open(PRIVATE_KEY_FILE, "rb") as key_file:
    #     private_pem = key_file.read()
    private_pem = base64.b64decode(os.getenv('LANYING_CONNECTOR_CERT_PRIVATE_KEY'))

    # 使用 cryptography 加载 PEM 格式的私钥
    private_key = serialization.load_pem_private_key(private_pem, password=None, backend=default_backend())
    return private_key

# 获取 Let's Encrypt 客户端对象
def get_acme_client():
    # 加载账户密钥
    acc_key = load_private_key()
    
    # 使用 ClientNetwork 和 ClientV2 获取客户端对象
    net = client.ClientNetwork(acc_key, user_agent=USER_AGENT)
    directory = client.ClientV2.get_directory(DIRECTORY_URL, net)
    client_acme = client.ClientV2(directory, net=net)
    
    reg = messages.NewRegistration(key=acc_key.public_key(), only_return_existing=True)
    response = client_acme._post(directory['newAccount'], reg)
    regr = client_acme._regr_from_response(response)
    client_acme.net.account=regr
    
    return client_acme

# 获取 Let's Encrypt 客户端对象
def get_register_account_acme_client():
    # 加载账户密钥
    acc_key = load_private_key()
    
    # 使用 ClientNetwork 和 ClientV2 获取客户端对象
    net = client.ClientNetwork(acc_key, user_agent=USER_AGENT)
    directory = client.ClientV2.get_directory(DIRECTORY_URL, net)
    client_acme = client.ClientV2(directory, net=net)
    
    return client_acme

# 注册账户
def register_account(email):
    # 获取客户端对象
    client_acme = get_register_account_acme_client()
    
    # 注册账户，提交电子邮件地址，并同意服务条款
    regr = client_acme.new_account(
        messages.NewRegistration.from_data(
            email=email,
            terms_of_service_agreed=True
        )
    )

    # 打印返回的注册信息
    print("Account registered:")
    print(f"regr:{regr}")
    print(f"Account URL: {regr.uri}")  # 使用 regr.uri 来访问注册信息的 URI
    print(f"Account key saved at: {PRIVATE_KEY_FILE}")

def new_csr_comp(domain_name, pkey_pem=None):
    """Create certificate signing request."""
    if pkey_pem is None:
        # Create private key.
        pkey = OpenSSL.crypto.PKey()
        pkey.generate_key(OpenSSL.crypto.TYPE_RSA, CERT_PKEY_BITS)
        pkey_pem = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM,
                                                  pkey)
    csr_pem = crypto_util.make_csr(pkey_pem, [domain_name])
    return pkey_pem, csr_pem

def select_http01_chall(orderr):
    """Extract authorization resource from within order resource."""
    # Authorization Resource: authz.
    # This object holds the offered challenges by the server and their status.
    authz_list = orderr.authorizations

    for authz in authz_list:
        # Choosing challenge.
        # authz.body.challenges is a set of ChallengeBody objects.
        for i in authz.body.challenges:
            # Find the supported challenge.
            if isinstance(i.chall, challenges.HTTP01):
                return i

    raise Exception('HTTP-01 challenge was not offered by the CA server.')

def perform_http01(client_acme, challb, orderr):
    """Set up standalone webserver and perform HTTP-01 challenge."""

    response, validation = challb.response_and_validation(client_acme.net.key)
    
    challenge_key = challb.to_json()['token']
    
    set_acme_challenge_value(challenge_key,validation)

    # Let the CA server know that we are ready for the challenge.
    client_acme.answer_challenge(challb, response)

    # Wait for challenge status and then issue a certificate.
    # It is possible to set a deadline time.
    finalized_orderr = client_acme.poll_and_finalize(orderr)

    return finalized_orderr
