from flask import Blueprint, request, make_response, send_file
import logging
import os
import json
import lanying_grow_ai
service = 'grow_ai'
bp = Blueprint(service, __name__)


@bp.route("/service/grow_ai/open_service", methods=["POST"])
def open_service():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    product_id = int(data['product_id'])
    price = int(data['price'])
    website_storage_limit = int(data.get('website_storage_limit', '0'))
    website_traffic_limit = int(data.get('website_traffic_limit', '0'))
    result = lanying_grow_ai.open_service(app_id, product_id, price, website_storage_limit, website_traffic_limit)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/close_service", methods=["POST"])
def close_service():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    product_id = int(data['product_id'])
    result = lanying_grow_ai.close_service(app_id, product_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/get_service_usage", methods=["POST"])
def get_service_usage():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_grow_ai.get_service_usage(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/create_task", methods=["POST"])
def create_task():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    name = str(data['name'])
    note = str(data['note'])
    chatbot_id = str(data['chatbot_id'])
    prompt = str(data['prompt'])
    keywords = str(data['keywords'])
    word_count_min = int(data['word_count_min'])
    word_count_max = int(data['word_count_max'])
    image_count = int(data['image_count'])
    article_count = int(data['article_count'])
    article_count = max(1, article_count)
    article_count = min(100000, article_count)
    cycle_type = str(data['cycle_type'])
    cycle_interval = int(data['cycle_interval'])
    cycle_interval = max(3600, cycle_interval)
    file_list = list(data.get('file_list', []))
    deploy = dict(data.get('deploy', {'type': 'none'}))
    title_reuse = str(data.get('title_reuse', 'off'))
    site_id_list = list(data.get('site_id_list', []))
    if 'target_dir' in data:
        target_dir = str(data.get('target_dir')).strip()
    else:
        target_dir = deploy.get('gitbook_target_dir', '/articles').strip()
    if 'commit_type' in data:
        commit_type = str(data.get('commit_type')).strip()
    else:
        commit_type = deploy.get('gitbook_commit_type', 'pull_request').strip()
    task_setting = lanying_grow_ai.TaskSetting(
        app_id = app_id,
        name = name,
        note = note,
        chatbot_id = chatbot_id,
        prompt = prompt,
        keywords = keywords,
        word_count_min = word_count_min,
        word_count_max = word_count_max,
        image_count = image_count,
        article_count = article_count,
        cycle_type = cycle_type,
        cycle_interval = cycle_interval,
        file_list = file_list,
        deploy = deploy,
        title_reuse = title_reuse,
        site_id_list = site_id_list,
        target_dir = target_dir,
        commit_type = commit_type
    )
    result = lanying_grow_ai.create_task(task_setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/configure_task", methods=["POST"])
def configure_task():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_id = str(data['task_id'])
    name = str(data['name'])
    note = str(data['note'])
    chatbot_id = str(data['chatbot_id'])
    prompt = str(data['prompt'])
    keywords = str(data['keywords'])
    word_count_min = int(data['word_count_min'])
    word_count_max = int(data['word_count_max'])
    image_count = int(data['image_count'])
    article_count = int(data['article_count'])
    article_count = max(1, article_count)
    article_count = min(100000, article_count)
    cycle_type = str(data['cycle_type'])
    cycle_interval = int(data['cycle_interval'])
    cycle_interval = max(3600, cycle_interval)
    file_list = list(data.get('file_list', []))
    deploy = dict(data.get('deploy', {'type': 'none'}))
    title_reuse = str(data.get('title_reuse', 'off'))
    site_id_list = list(data.get('site_id_list', []))
    if 'target_dir' in data:
        target_dir = str(data.get('target_dir')).strip()
    else:
        target_dir = deploy.get('gitbook_target_dir', '/articles').strip()
    if 'commit_type' in data:
        commit_type = str(data.get('commit_type')).strip()
    else:
        commit_type = deploy.get('gitbook_commit_type', 'pull_request').strip()
    task_setting = lanying_grow_ai.TaskSetting(
        app_id = app_id,
        name = name,
        note = note,
        chatbot_id = chatbot_id,
        prompt = prompt,
        keywords = keywords,
        word_count_min = word_count_min,
        word_count_max = word_count_max,
        image_count = image_count,
        article_count = article_count,
        cycle_type = cycle_type,
        cycle_interval = cycle_interval,
        file_list = file_list,
        deploy = deploy,
        title_reuse = title_reuse,
        site_id_list = site_id_list,
        target_dir = target_dir,
        commit_type = commit_type
    )
    result = lanying_grow_ai.configure_task(task_id, task_setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/run_task", methods=["POST"])
def run_task():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_id = str(data['task_id'])
    result = lanying_grow_ai.run_task(app_id, task_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/set_task_schedule", methods=["POST"])
def set_task_schedule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_id = str(data['task_id'])
    schedule = str(data['schedule'])
    result = lanying_grow_ai.set_task_schedule(app_id, task_id, schedule)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/delete_task", methods=["POST"])
def delete_task():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_id = str(data['task_id'])
    result = lanying_grow_ai.delete_task(app_id, task_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/get_task_list", methods=["POST"])
def get_task_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_grow_ai.get_task_list(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/get_task_run_list", methods=["POST"])
def get_task_run_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_id = str(data['task_id'])
    result = lanying_grow_ai.get_task_run_list(app_id, task_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/task_run_retry", methods=["POST"])
def task_run_retry():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_run_id = str(data['task_run_id'])
    result = lanying_grow_ai.task_run_retry(app_id, task_run_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/deploy_task_run", methods=["POST"])
def deploy_task_run():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_run_id = str(data['task_run_id'])
    result = lanying_grow_ai.deploy_task_run(app_id, task_run_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/delete_task_run", methods=["POST"])
def delete_task_run():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_run_id = str(data['task_run_id'])
    result = lanying_grow_ai.delete_task_run(app_id, task_run_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/get_task_run_result_list", methods=["POST"])
def get_task_run_result_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_run_id = str(data['task_run_id'])
    result = lanying_grow_ai.get_task_run_result_list(app_id, task_run_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route('/service/grow_ai/file/download', methods=['GET'])
def download_file():
    file_sign = request.args.get('file_sign')
    result = lanying_grow_ai.get_download_file(file_sign)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
        return resp
    else:
        file_path = result['data']['file_path']
        object_name = result['data']['object_name']
        return send_file(file_path, as_attachment=True, download_name=object_name)

@bp.route("/service/grow_ai/download_task_run_result", methods=["POST"])
def download_task_run_result():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    task_run_id = str(data['task_run_id'])
    result = lanying_grow_ai.download_task_run_result(app_id, task_run_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/grow_ai/release_finish", methods=["POST"])
def release_finish():
    text = request.get_data(as_text=True)
    data = json.loads(text)
    repository = str(data['repository'])
    release = str(data['release'])
    result = lanying_grow_ai.release_finish(repository, release)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/grow_ai/check_deploy", methods=["POST"])
def check_deploy():
    text = request.get_data(as_text=True)
    data = json.loads(text)
    code = request.args.get('code')
    release_size = int(data['release_size'])
    result = lanying_grow_ai.check_deploy(code, release_size)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/grow_ai/deploy_finish", methods=["POST"])
def deploy_finish():
    text = request.get_data(as_text=True)
    data = json.loads(text)
    code = request.args.get('code')
    deploy_result = str(data['status'])
    result = lanying_grow_ai.deploy_finish(code, deploy_result)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp


@bp.route("/service/grow_ai/create_site", methods=["POST"])
def create_site():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    name = str(data['name'])
    type = str(data['type'])
    github_url = str(data['github_url'])
    github_token = str(data['github_token'])
    github_base_branch = str(data['github_base_branch'])
    github_base_dir = str(data['github_base_dir'])
    footer_note = str(data['footer_note'])
    lanying_link = str(data['lanying_link'])
    title = str(data.get('title', ''))
    copyright = str(data.get('copyright', ''))
    canonical_link = str(data.get('canonical_link', ''))
    meta_keywords = str(data.get('meta_keywords', ''))
    baidu_token = str(data.get('baidu_token', ''))
    official_website_url = str(data.get('official_website_url', ''))
    site_setting = lanying_grow_ai.SiteSetting(
        app_id = app_id,
        name = name,
        type = type,
        github_url = github_url,
        github_token = github_token,
        github_base_branch = github_base_branch,
        github_base_dir = github_base_dir,
        footer_note = footer_note,
        lanying_link = lanying_link,
        title = title,
        copyright = copyright,
        canonical_link = canonical_link,
        meta_keywords = meta_keywords,
        baidu_token = baidu_token,
        official_website_url = official_website_url
    )
    result = lanying_grow_ai.create_site(site_setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/configure_site", methods=["POST"])
def configure_site():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    name = str(data['name'])
    type = str(data['type'])
    github_url = str(data['github_url'])
    github_token = str(data['github_token'])
    github_base_branch = str(data['github_base_branch'])
    github_base_dir = str(data['github_base_dir'])
    footer_note = str(data['footer_note'])
    lanying_link = str(data['lanying_link'])
    title = str(data.get('title', ''))
    copyright = str(data.get('copyright', ''))
    canonical_link = str(data.get('canonical_link', ''))
    meta_keywords = str(data.get('meta_keywords', ''))
    baidu_token = str(data.get('baidu_token', ''))
    official_website_url = str(data.get('official_website_url', ''))
    site_setting = lanying_grow_ai.SiteSetting(
        app_id = app_id,
        name = name,
        type = type,
        github_url = github_url,
        github_token = github_token,
        github_base_branch = github_base_branch,
        github_base_dir = github_base_dir,
        footer_note = footer_note,
        lanying_link = lanying_link,
        title = title,
        copyright = copyright,
        canonical_link = canonical_link,
        meta_keywords = meta_keywords,
        baidu_token = baidu_token,
        official_website_url = official_website_url
    )
    result = lanying_grow_ai.configure_site(site_id, site_setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/grow_ai/get_site_list", methods=["POST"])
def get_site_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_grow_ai.get_site_list(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False