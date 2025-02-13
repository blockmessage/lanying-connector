import os
from flask import Flask, request
import logging
import sys
import socket
import json
import importlib
import requests

enable_api_proxy_server = os.getenv("LANYING_CONNECTOR_ENABLE_API_PROXY_SERVER", "0") == "1"
enable_api_proxy_client = os.getenv("LANYING_CONNECTOR_ENABLE_API_PROXY_CLIENT", "0") == "1"
api_proxy_server_url = os.getenv("LANYING_CONNECTOR_API_PROXY_SERVER_URL")
accessToken = os.getenv('LANYING_CONNECTOR_API_PROXY_ACCESS_TOKEN')
loaded_modules = {}
app = None

def server_enabled():
    return enable_api_proxy_server

def client_enabled():
    return enable_api_proxy_client

def proxy_request(module_name, function_name, args):
    if client_enabled() and api_proxy_server_url and accessToken is not None:
        try:
            headers = {
                'access-token': accessToken
            }
            body = {
                'module_name': module_name,
                'function_name': function_name,
                'args': args
            }
            logging.info(f"proxy_request start | module_name:{module_name}, function_name:{function_name}, args:{args}")
            response = requests.post(api_proxy_server_url + '/api-proxy', headers = headers, json=body).json()
            logging.info(f"proxy_request finish | module_name:{module_name}, function_name:{function_name}, args:{args}, response:{response}")
            if response['code'] == 200:
                return response['data']
            else:
                return {
                    'result': 'error',
                    'message': response['message']
                }
        except Exception as e:
            logging.exception(e)
            return {
                'result': 'error',
                'message': 'api_response_error'
            }
    else:
        return {
            'result': 'error',
            'message': 'client_not_config_well'
        }

def init_logging():
    logdir = f"log/{socket.gethostname()}"
    os.makedirs(logdir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(f'{logdir}/info.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)

def create_app():
    app = Flask(__name__)
    app_upload_dir = '/data/upload/'
    os.makedirs(app_upload_dir, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = app_upload_dir
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    app.config["timeout"] = 120
    if os.environ.get("FLASK_DEBUG"):
        app.debug = True
    return app

def get_proxy_list():
    return [
        {
            'module_name': 'lanying_google_analytics',
            'function_name': 'create_properties'
        },
        {
            'module_name': 'lanying_google_analytics',
            'function_name': 'create_stream'
        },
        {
            'module_name': 'lanying_google_analytics',
            'function_name': 'get_report'
        }
    ]

if enable_api_proxy_server:
    init_logging()
    sys.path.append("services")
    app = create_app()
    logging.info("init app finish")

    @app.route("/", methods=["GET"])
    def index():
        resp = app.make_response('')
        return resp

    @app.route("/api-proxy", methods=["POST"])
    def apiProxy():
        headerToken = request.headers.get('access-token', "")
        if accessToken and accessToken == headerToken:
            try:
                text = request.get_data(as_text=True)
                data = json.loads(text)
                module_name = data['module_name']
                function_name = data['function_name']
                args = dict(data['args'])
                proxy_list = get_proxy_list()
                for proxy in proxy_list:
                    if proxy['module_name'] == module_name and proxy['function_name'] == function_name:
                        if module_name in loaded_modules:
                            now_module = loaded_modules[module_name]
                        else:
                            now_module = importlib.import_module(module_name)
                            loaded_modules[module_name] = now_module
                        func = getattr(now_module, function_name)
                        try:
                            logging.info(f"api proxy start | module:{module_name}, function:{function_name}, args: {args}")
                            result = func(**args)
                            logging.info(f"api proxy finish | module:{module_name}, function:{function_name}, args: {args}, result:{result}")
                            resp = app.make_response({'code':200, 'data':result})
                            return resp
                        except Exception as e:
                            logging.exception(e)
                            resp = app.make_response({'code':500, 'message':'server internal error'})
                            return resp
            except Exception as e:
                logging.exception(e)
                pass
        resp = app.make_response({'code':400, 'message':'bad request data'})
        return resp
