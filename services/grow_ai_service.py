from flask import Blueprint, request, make_response, send_file, abort
import logging
import os
import json
import lanying_grow_ai
import lanying_cert
from datetime import date as datetime_date
from datetime import timedelta as datetime_timedelta
service = 'grow_ai'
bp = Blueprint(service, __name__)

GROW_AI_DOWNLOAD_ALLOWED_ORIGINS = {
    'https://console.seenical.ai',
    'https://localhost:1024',
    'http://localhost:1024'
}


def add_download_cors_headers(resp):
    origin = request.headers.get('Origin', '')
    resp.vary.add('Origin')
    if origin in GROW_AI_DOWNLOAD_ALLOWED_ORIGINS:
        resp.headers['Access-Control-Allow-Origin'] = origin
        resp.headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Range'
        resp.headers['Access-Control-Expose-Headers'] = (
            'Content-Disposition, Content-Length, Content-Range, Accept-Ranges'
        )
        resp.headers['Access-Control-Max-Age'] = '3600'
    return resp


@bp.route("/.well-known/acme-challenge/<string:key>", methods=["GET"])
def acme_challenge(key):
    logging.info(f"acme_challenge got challenge | {key}")
    result = lanying_cert.get_acme_challenge_value(key)
    logging.info(f"acme_challenge challenge result | key:{key}, result:{result}")
    if result['result'] == 'ok':
        resp = make_response(result['value'])
        return resp
    else:
        abort(404)

@bp.route("/service/grow_ai/generate_ssl_cert", methods=["POST"])
def generate_ssl_cert():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    domain = str(data['domain'])
    result = lanying_grow_ai.generate_ssl_cert(app_id, domain)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

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
    article_prompt = str(data.get('article_prompt', ''))
    article_language = str(data.get('article_language', 'auto'))
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
    auto_deploy = 'on' if str(data.get('auto_deploy', 'on' if site_id_list else 'off')) == 'on' else 'off'
    if 'target_dir' in data:
        target_dir = str(data.get('target_dir')).strip()
    else:
        target_dir = deploy.get('gitbook_target_dir', '/articles').strip()
    if 'commit_type' in data:
        commit_type = str(data.get('commit_type')).strip()
    else:
        commit_type = deploy.get('gitbook_commit_type', 'branch').strip()
    target_summary_dir = str(data.get('target_summary_dir', ''))
    embedding_condition = dict(data.get('embedding_condition', {}))
    task_setting = lanying_grow_ai.TaskSetting(
        app_id = app_id,
        name = name,
        note = note,
        chatbot_id = chatbot_id,
        prompt = prompt,
        article_prompt = article_prompt,
        article_language = article_language,
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
        commit_type = commit_type,
        target_summary_dir = target_summary_dir,
        embedding_condition = embedding_condition,
        auto_deploy = auto_deploy
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
    task = lanying_grow_ai.get_task(app_id, task_id)
    article_prompt = str(data.get('article_prompt', task.get('article_prompt', '') if task else ''))
    article_language = str(data.get('article_language', task.get('article_language', 'auto') if task else 'auto'))
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
    if 'auto_deploy' in data:
        auto_deploy = 'on' if str(data['auto_deploy']) == 'on' else 'off'
    else:
        auto_deploy = task.get('auto_deploy', 'on' if site_id_list else 'off') if task else ('on' if site_id_list else 'off')
    if 'target_dir' in data:
        target_dir = str(data.get('target_dir')).strip()
    else:
        target_dir = deploy.get('gitbook_target_dir', '/articles').strip()
    if 'commit_type' in data:
        commit_type = str(data.get('commit_type')).strip()
    else:
        commit_type = deploy.get('gitbook_commit_type', 'branch').strip()
    target_summary_dir = str(data.get('target_summary_dir', ''))
    embedding_condition = dict(data.get('embedding_condition', {}))
    task_setting = lanying_grow_ai.TaskSetting(
        app_id = app_id,
        name = name,
        note = note,
        chatbot_id = chatbot_id,
        prompt = prompt,
        article_prompt = article_prompt,
        article_language = article_language,
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
        commit_type = commit_type,
        target_summary_dir = target_summary_dir,
        embedding_condition = embedding_condition,
        auto_deploy = auto_deploy
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

@bp.route("/service/grow_ai/task_run_preview", methods=["POST"])
def task_run_preview():
    if not check_access_token_valid():
        return make_response({'code':401, 'message':'bad authorization'})
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.task_run_preview(str(data['app_id']), str(data['task_run_id']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

@bp.route("/service/grow_ai/preview_retry", methods=["POST"])
def preview_retry():
    if not check_access_token_valid():
        return make_response({'code':401, 'message':'bad authorization'})
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.preview_retry(str(data['app_id']), str(data['preview_id']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

@bp.route("/service/grow_ai/preview_publish", methods=["POST"])
def preview_publish():
    if not check_access_token_valid():
        return make_response({'code':401, 'message':'bad authorization'})
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.preview_publish(str(data['app_id']), str(data['preview_id']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

@bp.route("/service/grow_ai/preview_discard", methods=["POST"])
def preview_discard():
    if not check_access_token_valid():
        return make_response({'code':401, 'message':'bad authorization'})
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.preview_discard(str(data['app_id']), str(data['preview_id']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

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

@bp.route('/service/grow_ai/file/download', methods=['GET', 'OPTIONS'])
def download_file():
    if request.method == 'OPTIONS':
        return add_download_cors_headers(make_response('', 204))
    file_sign = request.args.get('file_sign')
    result = lanying_grow_ai.get_download_file(file_sign)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
        return add_download_cors_headers(resp)
    else:
        file_path = result['data']['file_path']
        object_name = result['data']['object_name']
        resp = send_file(file_path, as_attachment=True, download_name=object_name)
        return add_download_cors_headers(resp)

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

@bp.route("/grow_ai/check_preview_deploy", methods=["POST"])
def check_preview_deploy():
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.check_preview_deploy(request.args.get('code'), int(data['release_size']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

@bp.route("/grow_ai/preview_deploy_finish", methods=["POST"])
def preview_deploy_finish():
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.preview_deploy_finish(request.args.get('code'), str(data['status']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})

@bp.route("/grow_ai/preview_clear_finish", methods=["POST"])
def preview_clear_finish():
    data = json.loads(request.get_data(as_text=True))
    result = lanying_grow_ai.preview_clear_finish(request.args.get('code'), str(data['status']))
    return make_response({'code':400, 'message':result['message']}) if result['result'] == 'error' else make_response({'code':200, 'data':result['data']})


@bp.route("/service/grow_ai/create_site", methods=["POST"])
def create_site():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    tenement_id = str(data.get('tenement_id', ''))
    name = str(data['name'])
    type = str(data['type'])
    github_url = str(data.get('github_url', ''))
    github_token = str(data.get('github_token', ''))
    github_base_branch = str(data.get('github_base_branch', 'master'))
    github_base_dir = str(data.get('github_base_dir', '/'))
    footer_note = str(data['footer_note'])
    lanying_link = maybe_add_https_prefix(str(data['lanying_link']))
    title = str(data.get('title', ''))
    copyright = str(data.get('copyright', ''))
    canonical_link = maybe_add_https_prefix(str(data.get('canonical_link', '')))
    meta_keywords = str(data.get('meta_keywords', ''))
    baidu_token = str(data.get('baidu_token', ''))
    official_website_url = maybe_add_https_prefix(str(data.get('official_website_url', '')))
    google_token = str(data.get('google_token', ''))
    max_latest_num = int(data.get('max_latest_num', '10'))
    language = str(data.get('language', 'zh-hans'))
    commit_type = str(data.get('commit_type','branch')).strip()
    icp_number = str(data.get('icp_number','')).strip()
    hook_sentence_slogan = str(data.get('hook_sentence_slogan', ''))
    hook_sentence_image = str(data.get('hook_sentence_image', '')).strip()
    github_hosting = ensure_value_on_off(str(data.get('github_hosting', 'off')))
    collaborator = str(data.get('collaborator', '')).strip()
    site_setting = lanying_grow_ai.SiteSetting(
        app_id = app_id,
        tenement_id = tenement_id,
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
        official_website_url = official_website_url,
        google_token = google_token,
        max_latest_num = max_latest_num,
        language = language,
        commit_type = commit_type,
        icp_number = icp_number,
        hook_sentence_slogan = hook_sentence_slogan,
        hook_sentence_image = hook_sentence_image,
        github_hosting = github_hosting,
        collaborator = collaborator
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
    tenement_id = str(data.get('tenement_id', ''))
    site_id = str(data['site_id'])
    name = str(data['name'])
    type = str(data['type'])
    github_url = str(data.get('github_url', ''))
    github_token = str(data.get('github_token', ''))
    github_base_branch = str(data.get('github_base_branch', 'master'))
    github_base_dir = str(data.get('github_base_dir', '/'))
    footer_note = str(data['footer_note'])
    lanying_link = maybe_add_https_prefix(str(data['lanying_link']))
    title = str(data.get('title', ''))
    copyright = str(data.get('copyright', ''))
    canonical_link = maybe_add_https_prefix(str(data.get('canonical_link', '')))
    meta_keywords = str(data.get('meta_keywords', ''))
    baidu_token = str(data.get('baidu_token', ''))
    official_website_url = maybe_add_https_prefix(str(data.get('official_website_url', '')))
    google_token = str(data.get('google_token', ''))
    max_latest_num = int(data.get('max_latest_num', '10'))
    language = str(data.get('language', 'zh-hans'))
    commit_type = str(data.get('commit_type','branch')).strip()
    icp_number = str(data.get('icp_number','')).strip()
    hook_sentence_slogan = str(data.get('hook_sentence_slogan', ''))
    hook_sentence_image = str(data.get('hook_sentence_image', '')).strip()
    github_hosting = ensure_value_on_off(str(data.get('github_hosting', 'off')))
    collaborator = str(data.get('collaborator', '')).strip()
    site_setting = lanying_grow_ai.SiteSetting(
        app_id = app_id,
        tenement_id = tenement_id,
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
        official_website_url = official_website_url,
        google_token = google_token,
        max_latest_num = max_latest_num,
        language = language,
        commit_type = commit_type,
        icp_number = icp_number,
        hook_sentence_slogan = hook_sentence_slogan,
        hook_sentence_image = hook_sentence_image,
        github_hosting = github_hosting,
        collaborator = collaborator
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

@bp.route("/service/grow_ai/create_custom_domain", methods=["POST"])
def create_custom_domain():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    tenement_id = str(data['tenement_id'])
    domain_name = str(data['domain_name']).strip()
    scope = str(data['scope'])
    max_domain_num = int(data['max_domain_num'])
    check_verify_owner = str(data.get('check_verify_owner', 'off'))
    result = lanying_grow_ai.create_custum_domain(app_id, site_id, domain_name, scope, tenement_id, check_verify_owner, max_domain_num)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/get_site_custom_domain_info", methods=["POST"])
def get_site_custom_domain_info():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    result = lanying_grow_ai.get_site_custom_domain_info(app_id, site_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/get_site_custom_domain_info_list", methods=["POST"])
def get_site_custom_domain_info_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_grow_ai.get_site_custom_domain_info_list(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/check_domain_cname", methods=["POST"])
def check_domain_cname():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    tenement_id = str(data['tenement_id'])
    result = lanying_grow_ai.check_domain_cname(app_id, site_id, tenement_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/domain_num_limit_changed", methods=["POST"])
def domain_num_limit_changed():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    max_domain_num = int(data['max_domain_num'])
    tenement_id = str(data['tenement_id'])
    result = lanying_grow_ai.domain_num_limit_changed(app_id, max_domain_num, tenement_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/site_statistics", methods=["POST"])
def site_statistics():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    default_end_date = datetime_date.today()
    default_start_date = default_end_date - datetime_timedelta(14)
    start_date = str(data.get('start_date', default_start_date))
    end_date = str(data.get('end_date', default_end_date))
    targets = list(data['targets'])
    result = lanying_grow_ai.get_site_statistics(app_id, site_id, start_date, end_date, targets)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/site_index_info_list", methods=["POST"])
def site_index_info_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    result = lanying_grow_ai.get_site_index_info_list()
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result.get("data",{})})
    return resp

@bp.route("/service/grow_ai/upload_image", methods=["POST"])
def upload_image():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    site_id = str(data['site_id'])
    file_name = str(data['file_name'])
    result = lanying_grow_ai.upload_image(app_id, site_id, file_name)
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

def maybe_add_https_prefix(url):
    url = url.strip(' ')
    if url == '':
        return url
    if url.startswith('http'):
        return url
    return f'https://{url}'

def ensure_value_on_off(value):
    if value == 'on':
        return 'on'
    else:
        return 'off'
