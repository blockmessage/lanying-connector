import html
import json
import logging
import os
import secrets
import socket
import threading
import time

from lanying_async import executor
import lanying_config
import lanying_im_api
import lanying_openclaw as core


STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_PASSED = 'passed'
STATUS_FAILED = 'failed'
STATUS_ERROR = 'error'
DEFAULT_TIMEOUT_MS = 30000
DEFAULT_POLL_INTERVAL_MS = 1000
DEFAULT_DUPLICATE_OBSERVATION_WINDOW_MS = 10000
LOG_SUBDIR = 'openclaw_sync_validation'

tasks = {}
tasks_lock = threading.Lock()


def pick_int_config(value, default_value):
    if value is None:
        return int(default_value)
    try:
        return int(value)
    except Exception:
        return int(default_value)


def get_task(task_id):
    with tasks_lock:
        return tasks.get(str(task_id or '').strip())


def get_base_dir():
    base_dir = os.path.join('log', socket.gethostname(), LOG_SUBDIR)
    os.makedirs(base_dir, exist_ok=True)
    return base_dir


def get_task_dir(task_id):
    return os.path.join(get_base_dir(), str(task_id or '').strip())


def get_report_path(task_id):
    return os.path.join(get_task_dir(task_id), 'report.html')


def get_log_path(task_id):
    return os.path.join(get_task_dir(task_id), 'run.log')


def get_metadata_path(task_id):
    return os.path.join(get_task_dir(task_id), 'task.txt')


def format_timestamp_text(timestamp_ms):
    try:
        if int(timestamp_ms or 0) <= 0:
            return ''
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(timestamp_ms) / 1000.0))
    except Exception:
        return ''


def format_duration_text(started_at, ended_at):
    start_value = int(started_at or 0)
    end_value = int(ended_at or 0)
    if start_value <= 0:
        return ''
    if end_value <= 0:
        end_value = int(time.time() * 1000)
    diff_ms = max(0, end_value - start_value)
    if diff_ms < 1000:
        return f"{diff_ms}ms"
    return f"{diff_ms / 1000.0:.2f}s"


def safe_json_loads(raw, default=None):
    if default is None:
        default = {}
    if not isinstance(raw, str):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def append_log(task, text):
    task_dir = task.get('task_dir', '')
    if task_dir == '':
        return
    os.makedirs(task_dir, exist_ok=True)
    line = f"[{format_timestamp_text(int(time.time() * 1000))}] {text}"
    try:
        with open(task.get('log_path', get_log_path(task.get('task_id', ''))), 'a', encoding='utf-8') as log_file:
            log_file.write(line + '\n')
    except Exception:
        logging.exception("append_log failed")


def write_metadata(task):
    task_dir = task.get('task_dir', '')
    if task_dir == '':
        return
    lines = [
        f"task_id={task.get('task_id', '')}",
        f"app_id={task.get('app_id', '')}",
        f"node_id={task.get('node_id', '')}",
        f"status={task.get('status', '')}",
        f"requested_scenarios={','.join(task.get('requested_scenarios', []))}",
        f"report_path={task.get('report_path', '')}",
        f"started_at={task.get('started_at', 0)}",
        f"ended_at={task.get('ended_at', 0)}",
    ]
    try:
        with open(task.get('metadata_path', get_metadata_path(task.get('task_id', ''))), 'w', encoding='utf-8') as meta_file:
            meta_file.write('\n'.join(lines) + '\n')
    except Exception:
        logging.exception("write_metadata failed")


def html_escape(value):
    return html.escape(str(value if value is not None else ''))


def render_value(value):
    if isinstance(value, (dict, list)):
        return html_escape(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    return html_escape(value)


def render_key_value_table(rows):
    if not isinstance(rows, list) or len(rows) == 0:
        return '<p class="muted">none</p>'
    html_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        html_rows.append(
            f"<tr><th>{html_escape(row.get('label', ''))}</th><td><pre>{render_value(row.get('value', ''))}</pre></td></tr>"
        )
    if len(html_rows) == 0:
        return '<p class="muted">none</p>'
    return '<table class="kv-table">' + ''.join(html_rows) + '</table>'


def render_message_table(messages):
    if not isinstance(messages, list) or len(messages) == 0:
        return '<p class="muted">none</p>'
    rows = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        rows.append(
            '<tr>'
            f"<td>{html_escape(message.get('msg_id', ''))}</td>"
            f"<td>{html_escape(message.get('from_user_id', ''))}</td>"
            f"<td>{html_escape(message.get('to_user_id', ''))}</td>"
            f"<td>{html_escape(message.get('ctype', ''))}</td>"
            f"<td><pre>{html_escape(message.get('content', ''))}</pre></td>"
            f"<td><pre>{render_value(message.get('ext', ''))}</pre></td>"
            f"<td>{html_escape(format_timestamp_text(message.get('timestamp', 0)))}</td>"
            '</tr>'
        )
    if len(rows) == 0:
        return '<p class="muted">none</p>'
    return (
        '<table class="message-table"><thead><tr>'
        '<th>msg_id</th><th>from</th><th>to</th><th>ctype</th><th>content</th><th>ext</th><th>timestamp</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
    )


def render_report_html(task):
    scenarios = task.get('scenarios', [])
    pass_count = len([item for item in scenarios if item.get('status') == STATUS_PASSED])
    fail_count = len([item for item in scenarios if item.get('status') in [STATUS_FAILED, STATUS_ERROR]])
    summary_rows = []
    for scenario in scenarios:
        summary_rows.append(
            '<tr>'
            f"<td>{html_escape(scenario.get('name', ''))}</td>"
            f"<td>{html_escape(scenario.get('description', ''))}</td>"
            f"<td class='status status-{html_escape(scenario.get('status', ''))}'>{html_escape(scenario.get('status', ''))}</td>"
            f"<td>{html_escape(format_duration_text(scenario.get('started_at', 0), scenario.get('ended_at', 0)))}</td>"
            f"<td>{html_escape(scenario.get('failure_reason', ''))}</td>"
            '</tr>'
        )
    scenario_sections = []
    for scenario in scenarios:
        section_html = [
            f"<section class='scenario'><h2>{html_escape(scenario.get('name', ''))}</h2>",
            f"<p><strong>Status:</strong> <span class='status status-{html_escape(scenario.get('status', ''))}'>{html_escape(scenario.get('status', ''))}</span></p>",
            f"<p>{html_escape(scenario.get('description', ''))}</p>",
        ]
        if scenario.get('failure_reason', '') != '':
            section_html.append(f"<p class='failure'><strong>Failure:</strong> {html_escape(scenario.get('failure_reason', ''))}</p>")
        section_html.append('<h3>Participants</h3>')
        section_html.append(render_key_value_table(scenario.get('participant_rows', [])))
        section_html.append('<h3>Send Request</h3>')
        section_html.append(render_key_value_table(scenario.get('request_rows', [])))
        section_html.append('<h3>Expectation vs Actual</h3>')
        section_html.append(render_key_value_table(scenario.get('comparison_rows', [])))
        section_html.append('<h3>Relevant Messages</h3>')
        section_html.append(render_message_table(scenario.get('messages', [])))
        section_html.append('<h3>Relevant Session Mappings</h3>')
        section_html.append(render_key_value_table(scenario.get('mapping_rows', [])))
        if scenario.get('notes', '') != '':
            section_html.append(f"<h3>Notes</h3><pre>{html_escape(scenario.get('notes', ''))}</pre>")
        section_html.append('</section>')
        scenario_sections.append(''.join(section_html))
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>OpenClaw Sync Validation Report</title>'
        '<style>'
        'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:24px;color:#222;}'
        'h1,h2,h3{margin:0 0 12px 0;}'
        '.meta,.summary-table,.message-table,.kv-table{width:100%;border-collapse:collapse;margin:12px 0 24px 0;}'
        'th,td{border:1px solid #d9d9d9;padding:8px;vertical-align:top;text-align:left;}'
        'pre{margin:0;white-space:pre-wrap;word-break:break-word;}'
        '.status{font-weight:600;text-transform:uppercase;}'
        '.status-passed{color:#0a7f2e;}.status-failed,.status-error{color:#b42318;}.status-running,.status-pending{color:#b26b00;}'
        '.muted{color:#666;}.failure{color:#b42318;}.scenario{margin-bottom:36px;}'
        '</style></head><body>'
        '<h1>OpenClaw Sync Validation Report</h1>'
        '<table class="meta">'
        f"<tr><th>task_id</th><td>{html_escape(task.get('task_id', ''))}</td></tr>"
        f"<tr><th>app_id</th><td>{html_escape(task.get('app_id', ''))}</td></tr>"
        f"<tr><th>node_id</th><td>{html_escape(task.get('node_id', ''))}</td></tr>"
        f"<tr><th>status</th><td class='status status-{html_escape(task.get('status', ''))}'>{html_escape(task.get('status', ''))}</td></tr>"
        f"<tr><th>started_at</th><td>{html_escape(format_timestamp_text(task.get('started_at', 0)))}</td></tr>"
        f"<tr><th>ended_at</th><td>{html_escape(format_timestamp_text(task.get('ended_at', 0)))}</td></tr>"
        f"<tr><th>duration</th><td>{html_escape(format_duration_text(task.get('started_at', 0), task.get('ended_at', 0)))}</td></tr>"
        f"<tr><th>report_path</th><td>{html_escape(task.get('report_path', ''))}</td></tr>"
        '</table>'
        f"<p><strong>Scenarios:</strong> total={len(scenarios)}, pass={pass_count}, fail={fail_count}</p>"
        '<table class="summary-table"><thead><tr><th>Scenario</th><th>Description</th><th>Status</th><th>Duration</th><th>Failure</th></tr></thead><tbody>'
        + ''.join(summary_rows) + '</tbody></table>' + ''.join(scenario_sections) + '</body></html>'
    )


def write_report(task):
    task_dir = task.get('task_dir', '')
    if task_dir == '':
        return
    os.makedirs(task_dir, exist_ok=True)
    with open(task.get('report_path', get_report_path(task.get('task_id', ''))), 'w', encoding='utf-8') as report_file:
        report_file.write(render_report_html(task))
    write_metadata(task)


def build_status_page(task):
    report_url = f"/service/openclaw/sync_validation/{html_escape(task.get('task_id', ''))}"
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8"><title>OpenClaw Sync Validation</title></head><body>'
        '<h1>OpenClaw Sync Validation Task</h1>'
        f"<p><strong>task_id:</strong> {html_escape(task.get('task_id', ''))}</p>"
        f"<p><strong>status:</strong> {html_escape(task.get('status', ''))}</p>"
        f"<p><strong>app_id:</strong> {html_escape(task.get('app_id', ''))}</p>"
        f"<p><strong>node_id:</strong> {html_escape(task.get('node_id', ''))}</p>"
        f"<p><strong>report_dir:</strong> {html_escape(task.get('task_dir', ''))}</p>"
        f"<p><a href=\"{report_url}\">View status/report</a></p>"
        '</body></html>'
    )


def get_validation_config(app_id, node_id=None):
    raw = os.getenv('LANYING_OPENCLAW_SYNC_VALIDATION_CONFIG', '')
    parsed = safe_json_loads(raw, {})
    if not isinstance(parsed, dict):
        return {}
    app_config = parsed.get(str(app_id), parsed.get('default', {}))
    if not isinstance(app_config, dict):
        return {}
    if node_id is None:
        return app_config
    nodes = app_config.get('nodes', {})
    if isinstance(nodes, dict):
        node_config = nodes.get(str(node_id), {})
        if isinstance(node_config, dict):
            merged = dict(app_config)
            merged.update(node_config)
            return merged
    return dict(app_config)


def normalize_scenarios(scenario=None, scenarios=None):
    available = ['group_openclaw', 'group_chatbot', 'direct_openclaw', 'direct_chatbot']
    names = []
    if isinstance(scenarios, list):
        for item in scenarios:
            text = str(item or '').strip()
            if text != '':
                names.append(text)
    elif isinstance(scenarios, str):
        for item in scenarios.split(','):
            text = str(item or '').strip()
            if text != '':
                names.append(text)
    single = str(scenario or '').strip()
    if single != '':
        names.append(single)
    if len(names) == 0 or 'all' in names:
        return available
    normalized = []
    seen = set()
    for name in names:
        if name in available and name not in seen:
            seen.add(name)
            normalized.append(name)
    return normalized


def parse_result_code(result):
    if not isinstance(result, dict):
        return 0
    try:
        return int(result.get('code', 0) or 0)
    except Exception:
        return 0


def extract_data_user_id(result):
    if not isinstance(result, dict):
        return ''
    data = result.get('data', {})
    if isinstance(data, dict):
        return str(data.get('user_id', '')).strip()
    return ''


def extract_data_group_id(result):
    if not isinstance(result, dict):
        return ''
    data = result.get('data', {})
    if isinstance(data, dict):
        return str(data.get('group_id', '')).strip()
    return ''


def create_validation_user(app_id, username_prefix='sync_validation'):
    username = f"{username_prefix}_{secrets.token_hex(4)}"
    password = secrets.token_hex(16)
    result = lanying_im_api.register(app_id, username, password)
    if parse_result_code(result) != 200:
        return {
            'result': 'error',
            'message': 'register validation user failed',
            'details': result,
        }
    user_id = extract_data_user_id(result)
    if user_id == '':
        return {
            'result': 'error',
            'message': 'register validation user missing user_id',
            'details': result,
        }
    return {
        'result': 'ok',
        'data': {
            'user_id': user_id,
            'username': username,
            'password': password,
        },
    }


def ensure_direct_roster_pair(app_id, left_user_id, right_user_id):
    left = str(left_user_id or '').strip()
    right = str(right_user_id or '').strip()
    if left == '' or right == '':
        return {
            'result': 'error',
            'message': 'roster users missing',
            'details': {'left_user_id': left, 'right_user_id': right},
        }
    try:
        add_right_result = lanying_im_api.admin_add_roster_direct(app_id, left, [int(right)])
        add_left_result = lanying_im_api.admin_add_roster_direct(app_id, right, [int(left)])
    except Exception as e:
        logging.exception("ensure_direct_roster_pair failed")
        return {
            'result': 'error',
            'message': str(e),
        }
    if parse_result_code(add_right_result) != 200 or parse_result_code(add_left_result) != 200:
        return {
            'result': 'error',
            'message': 'admin_add_roster_direct failed',
            'details': {
                'left_to_right': add_right_result,
                'right_to_left': add_left_result,
            },
        }
    return {
        'result': 'ok',
        'data': {
            'left_to_right': add_right_result,
            'right_to_left': add_left_result,
        },
    }


def create_validation_group(app_id, owner_user_id, group_name, member_user_ids=None):
    result = lanying_im_api.create_group(app_id, owner_user_id, group_name, group_type=0, user_list=member_user_ids or [])
    if parse_result_code(result) != 200:
        return {
            'result': 'error',
            'message': 'create validation group failed',
            'details': result,
        }
    group_id = extract_data_group_id(result)
    if group_id == '':
        return {
            'result': 'error',
            'message': 'create validation group missing group_id',
            'details': result,
        }
    return {
        'result': 'ok',
        'data': {
            'group_id': group_id,
            'create_result': result,
        },
    }


def build_runtime(app_id, node_id):
    node_info = core.get_node(app_id, node_id)
    if not isinstance(node_info, dict):
        return {'result': 'error', 'message': 'node not exist'}
    config = lanying_config.get_lanying_connector(app_id)
    if not isinstance(config, dict) or not config.get('lanying_admin_token'):
        return {'result': 'error', 'message': 'lanying admin token not exist'}
    validation_config = get_validation_config(app_id, node_id)
    chatbot_user_id = core.resolve_bound_chatbot_user_id(app_id, node_id)
    sender_result = create_validation_user(
        app_id,
        str(validation_config.get('sender_username_prefix', 'sync_validation')).strip() or 'sync_validation',
    )
    if sender_result.get('result') != 'ok':
        return sender_result
    sender_info = sender_result.get('data', {})
    sender_user_id = str(sender_info.get('user_id', '')).strip()
    openclaw_user_id = str(node_info.get('user_id', '')).strip()
    chatbot_user_id = str(chatbot_user_id or '').strip()
    provisioning_rows = [
        {'label': 'validation_sender_user_id', 'value': sender_user_id},
        {'label': 'validation_sender_username', 'value': sender_info.get('username', '')},
        {'label': 'validation_sender_password', 'value': sender_info.get('password', '')},
    ]
    roster_rows = []
    if openclaw_user_id != '':
        roster_result = ensure_direct_roster_pair(app_id, sender_user_id, openclaw_user_id)
        roster_rows.append({'label': 'sender<->openclaw', 'value': roster_result})
        if roster_result.get('result') != 'ok':
            return {
                'result': 'error',
                'message': 'prepare sender/openclaw roster failed',
                'details': roster_result,
            }
    if chatbot_user_id != '':
        roster_result = ensure_direct_roster_pair(app_id, sender_user_id, chatbot_user_id)
        roster_rows.append({'label': 'sender<->chatbot', 'value': roster_result})
        if roster_result.get('result') != 'ok':
            return {
                'result': 'error',
                'message': 'prepare sender/chatbot roster failed',
                'details': roster_result,
            }
    group_openclaw_id = ''
    group_chatbot_id = ''
    group_rows = []
    if openclaw_user_id != '':
        group_result = create_validation_group(
            app_id,
            openclaw_user_id,
            f"openclaw_sync_group_{node_id}_{secrets.token_hex(3)}",
            [sender_user_id],
        )
        group_rows.append({'label': 'group_openclaw', 'value': group_result})
        if group_result.get('result') != 'ok':
            return {
                'result': 'error',
                'message': 'create openclaw validation group failed',
                'details': group_result,
            }
        group_openclaw_id = str(group_result.get('data', {}).get('group_id', '')).strip()
    if chatbot_user_id != '':
        group_result = create_validation_group(
            app_id,
            chatbot_user_id,
            f"chatbot_sync_group_{node_id}_{secrets.token_hex(3)}",
            [sender_user_id],
        )
        group_rows.append({'label': 'group_chatbot', 'value': group_result})
        if group_result.get('result') != 'ok':
            return {
                'result': 'error',
                'message': 'create chatbot validation group failed',
                'details': group_result,
            }
        group_chatbot_id = str(group_result.get('data', {}).get('group_id', '')).strip()
    return {
        'result': 'ok',
        'data': {
            'app_id': app_id,
            'node_id': str(node_id),
            'node_info': node_info,
            'config': config,
            'validation_config': validation_config,
            'sender_user_id': sender_user_id,
            'sender_username': str(sender_info.get('username', '')).strip(),
            'sender_password': str(sender_info.get('password', '')).strip(),
            'openclaw_user_id': openclaw_user_id,
            'chatbot_user_id': chatbot_user_id,
            'group_openclaw_id': group_openclaw_id,
            'group_chatbot_id': group_chatbot_id,
            'timeout_ms': pick_int_config(validation_config.get('timeout_ms'), DEFAULT_TIMEOUT_MS),
            'poll_interval_ms': pick_int_config(validation_config.get('poll_interval_ms'), DEFAULT_POLL_INTERVAL_MS),
            'duplicate_observation_window_ms': pick_int_config(
                validation_config.get('duplicate_observation_window_ms'),
                DEFAULT_DUPLICATE_OBSERVATION_WINDOW_MS,
            ),
            'provisioning_rows': provisioning_rows + roster_rows + group_rows,
        },
    }


def build_scenario_definition(runtime, scenario_name):
    sender_user_id = runtime.get('sender_user_id', '')
    openclaw_user_id = runtime.get('openclaw_user_id', '')
    chatbot_user_id = runtime.get('chatbot_user_id', '')
    definitions = {
        'group_openclaw': {
            'name': 'group_openclaw',
            'description': '群聊 @OpenClawUserId 发送普通消息后，验证回复回 IM 的身份和 mapping。',
            'chat_type': 'group',
            'target_user_id': openclaw_user_id,
            'conversation_id': str(runtime.get('group_openclaw_id', '')),
            'sender_user_id': sender_user_id,
            'expected_reply_user_id': openclaw_user_id,
            'require_mapping': True,
        },
        'group_chatbot': {
            'name': 'group_chatbot',
            'description': '群聊 @ChatbotUserId 发送普通消息后，验证回复回 IM 的身份和 mapping。',
            'chat_type': 'group',
            'target_user_id': chatbot_user_id,
            'conversation_id': str(runtime.get('group_chatbot_id', '')),
            'sender_user_id': sender_user_id,
            'expected_reply_user_id': chatbot_user_id,
            'require_mapping': True,
        },
        'direct_openclaw': {
            'name': 'direct_openclaw',
            'description': '单聊 OpenClawUserId 发送普通消息后，验证回复回 IM 仍对应发送者。',
            'chat_type': 'direct',
            'target_user_id': openclaw_user_id,
            'conversation_id': openclaw_user_id,
            'sender_user_id': sender_user_id,
            'expected_reply_user_id': openclaw_user_id,
            'require_mapping': True,
        },
        'direct_chatbot': {
            'name': 'direct_chatbot',
            'description': '单聊 ChatbotUserId 发送普通消息后，验证回复回 IM 仍对应发送者。',
            'chat_type': 'direct',
            'target_user_id': chatbot_user_id,
            'conversation_id': chatbot_user_id,
            'sender_user_id': sender_user_id,
            'expected_reply_user_id': chatbot_user_id,
            'require_mapping': True,
        },
    }
    return definitions.get(scenario_name)


def build_trigger_text(scenario_def, now_ms):
    name = str(scenario_def.get('name', '')).strip()
    marker = f"SYNC_OK_{name}_{now_ms}"
    return f"这是联调测试，请直接回复“{marker}”，不要回复 NO_REPLY，也不要输出其他内容。"


def summarize_messages(messages):
    summary = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        summary.append({
            'msg_id': str(message.get('msg_id', '')),
            'from_user_id': str((message.get('from_xid') or {}).get('uid', '')),
            'to_user_id': str((message.get('to_xid') or {}).get('uid', '')),
            'content': str(message.get('content', '')),
            'ctype': str(message.get('ctype', '')),
            'ext': safe_json_loads(message.get('ext', ''), message.get('ext', '')),
            'timestamp': int(message.get('timestamp', 0) or 0),
        })
    return summary


def merge_message_snapshots(primary, secondary):
    merged = []
    seen = set()
    for message in (primary or []) + (secondary or []):
        if not isinstance(message, dict):
            continue
        msg_id = str(message.get('msg_id', '')).strip()
        dedupe_key = msg_id if msg_id != '' else json.dumps(message, ensure_ascii=False, sort_keys=True)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(message)
    merged.sort(key=lambda item: (int(item.get('timestamp', 0) or 0), str(item.get('msg_id', ''))))
    return merged


def get_mapping_sender_user_id(mapping):
    if not isinstance(mapping, dict):
        return ''
    return (
        str(mapping.get('origin_user_id', '')).strip() or
        str(mapping.get('sender_user_id', '')).strip()
    )


def send_message(runtime, scenario_def, content):
    config = runtime.get('config', {})
    sender_user_id = str(scenario_def.get('sender_user_id', ''))
    target_id = str(scenario_def.get('conversation_id', '')) if scenario_def.get('chat_type') == 'group' else str(scenario_def.get('target_user_id', ''))
    target_user_id = str(scenario_def.get('target_user_id', ''))
    extra = {}
    if scenario_def.get('chat_type') == 'group':
        mention_value = target_user_id
        try:
            mention_value = int(target_user_id)
        except Exception:
            pass
        extra['msg_config'] = {
            'mentionAll': False,
            'mentionList': [mention_value],
            'mentionedMessage': '',
            'pushMessage': '',
            'senderNickname': str(runtime.get('sender_username', '')).strip(),
        }
    response = lanying_im_api.post_send_message(
        config,
        runtime.get('app_id', ''),
        sender_user_id,
        target_id,
        2 if scenario_def.get('chat_type') == 'group' else 1,
        0,
        content,
        extra,
    )
    try:
        response_json = response.json()
    except Exception:
        response_json = {'code': response.status_code, 'message': 'invalid_json'}
    return {'http_status': getattr(response, 'status_code', 0), 'result': response_json}


def fetch_conversation(runtime, scenario_def):
    return lanying_im_api.fetch_conversation_messages(
        runtime.get('config', {}),
        runtime.get('app_id', ''),
        scenario_def.get('sender_user_id', ''),
        scenario_def.get('conversation_id', ''),
        limit=20,
        msg_id_start=0,
    )


def find_reply(messages, expected_reply_user_id, trigger_timestamp):
    replies = find_replies(messages, expected_reply_user_id, trigger_timestamp, '')
    return replies[0] if len(replies) > 0 else None


def extract_request_msg_id(send_result):
    if not isinstance(send_result, dict):
        return ''
    result = send_result.get('result', {})
    if not isinstance(result, dict):
        return ''
    msg_ids = result.get('msg_ids')
    if isinstance(msg_ids, list) and len(msg_ids) > 0:
        return str(msg_ids[0]).strip()
    data = result.get('data', {})
    if isinstance(data, dict):
        data_msg_ids = data.get('msg_ids')
        if isinstance(data_msg_ids, list) and len(data_msg_ids) > 0:
            return str(data_msg_ids[0]).strip()
    return ''


def find_replies(messages, expected_reply_user_id, trigger_timestamp, request_msg_id=''):
    expected = str(expected_reply_user_id or '').strip()
    expected_request_msg_id = str(request_msg_id or '').strip()
    matched = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if int(message.get('timestamp', 0) or 0) < int(trigger_timestamp or 0):
            continue
        if expected != '' and str(message.get('from_user_id', '')) != expected:
            continue
        ext = message.get('ext', {})
        openclaw_info = ext.get('openclaw', {}) if isinstance(ext, dict) else {}
        openclaw_type = str(openclaw_info.get('type', '')).strip()
        if openclaw_type not in ['session_sync_delivery', 'router_reply', 'im_reply_delivery'] and str(message.get('content', '')).strip() == '':
            continue
        message_request_msg_id = str(openclaw_info.get('request_msg_id', '')).strip()
        message_trigger_msg_id = str(openclaw_info.get('trigger_msg_id', '')).strip()
        if expected_request_msg_id != '':
            if message_request_msg_id == '' and message_trigger_msg_id == '':
                continue
            if expected_request_msg_id not in [message_request_msg_id, message_trigger_msg_id]:
                continue
        matched.append(message)
    return matched


def find_duplicate_visible_replies_by_content(messages, primary_reply, expected_reply_user_id, trigger_timestamp):
    if not isinstance(primary_reply, dict):
        return []
    primary_content = str(primary_reply.get('content', '')).strip()
    primary_from_user_id = str(primary_reply.get('from_user_id', '')).strip()
    if primary_content == '' or primary_from_user_id == '':
        return [primary_reply]
    matched = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if int(message.get('timestamp', 0) or 0) < int(trigger_timestamp or 0):
            continue
        if str(message.get('from_user_id', '')).strip() != primary_from_user_id:
            continue
        if expected_reply_user_id != '' and str(message.get('from_user_id', '')).strip() != str(expected_reply_user_id).strip():
            continue
        if str(message.get('content', '')).strip() != primary_content:
            continue
        matched.append(message)
    return matched


def merge_reply_candidates(primary, secondary):
    merged = []
    seen = set()
    for message in (primary or []) + (secondary or []):
        if not isinstance(message, dict):
            continue
        msg_id = str(message.get('msg_id', '')).strip()
        dedupe_key = msg_id if msg_id != '' else json.dumps(message, ensure_ascii=False, sort_keys=True)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(message)
    return merged


def select_relevant_mappings(runtime, scenario_def, trigger_timestamp):
    mappings = core.list_session_mappings_for_node(runtime.get('app_id', ''), runtime.get('node_id', ''))
    selected = []
    sender_user_id = str(scenario_def.get('sender_user_id', ''))
    conversation_id = str(scenario_def.get('conversation_id', ''))
    expected_reply_user_id = str(scenario_def.get('expected_reply_user_id', ''))
    expected_session_key = str(scenario_def.get('expected_session_key', '')).strip()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        score = 0
        if expected_session_key != '' and str(mapping.get('session_key', '')).strip() == expected_session_key:
            score += 5
        if get_mapping_sender_user_id(mapping) == sender_user_id:
            score += 3
        if scenario_def.get('chat_type') == 'group' and str(mapping.get('group_id', '')) == conversation_id:
            score += 3
        if str(mapping.get('openclaw_user_id', '')) == expected_reply_user_id:
            score += 1
        if int(mapping.get('updated_at', 0) or 0) >= int(trigger_timestamp or 0):
            score += 1
        if score > 0:
            selected.append((score, mapping))
    selected.sort(key=lambda item: (-item[0], -int(item[1].get('updated_at', 0) or 0)))
    return [item[1] for item in selected[:5]]


def build_mapping_rows(runtime, mappings):
    rows = []
    for mapping in mappings:
        session_key = str(mapping.get('session_key', ''))
        rows.append({
            'label': session_key if session_key != '' else 'mapping',
            'value': {
                'session_key': mapping.get('session_key', ''),
                'group_id': str(mapping.get('group_id', '')),
                'sender_user_id': str(mapping.get('sender_user_id', '')),
                'origin_user_id': str(mapping.get('origin_user_id', '')),
                'effective_sender_user_id': get_mapping_sender_user_id(mapping),
                'management_user_id': str(mapping.get('management_user_id', '')),
                'openclaw_user_id': str(mapping.get('openclaw_user_id', '')),
                'root_session_key': mapping.get('root_session_key', ''),
                'parent_session_key': mapping.get('parent_session_key', ''),
                'effective_target_session_key': mapping.get('effective_target_session_key', ''),
                'updated_at': int(mapping.get('updated_at', 0) or 0),
                'last_message_time': core.get_session_last_message_time(runtime.get('app_id', ''), runtime.get('node_id', ''), session_key),
            },
        })
    return rows


def merge_mapping_candidates(primary, secondary):
    merged = []
    seen = set()
    for mapping in (primary or []) + (secondary or []):
        if not isinstance(mapping, dict):
            continue
        session_key = str(mapping.get('session_key', '')).strip()
        dedupe_key = session_key if session_key != '' else json.dumps(mapping, ensure_ascii=False, sort_keys=True)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(mapping)
    return merged


def execute_scenario(runtime, scenario_def):
    missing_fields = []
    for field in ['sender_user_id', 'target_user_id', 'conversation_id']:
        if str(scenario_def.get(field, '')).strip() == '':
            missing_fields.append(field)
    if len(missing_fields) > 0:
        return {
            'status': STATUS_ERROR,
            'failure_reason': f"缺少场景前置资源: {', '.join(missing_fields)}",
            'participant_rows': [
                {'label': 'scenario_sender_user_id', 'value': scenario_def.get('sender_user_id', '')},
                {'label': 'scenario_target_user_id', 'value': scenario_def.get('target_user_id', '')},
                {'label': 'conversation_id', 'value': scenario_def.get('conversation_id', '')},
                {'label': 'chat_type', 'value': scenario_def.get('chat_type', '')},
            ] + list(runtime.get('provisioning_rows', [])),
            'request_rows': [],
            'comparison_rows': [],
            'messages': [],
            'mapping_rows': [],
            'notes': '',
        }
    now_ms = int(time.time() * 1000)
    unique_text = build_trigger_text(scenario_def, now_ms)
    send_result = send_message(runtime, scenario_def, unique_text)
    request_msg_id = extract_request_msg_id(send_result)
    conversation_snapshot = []
    all_observed_messages = []
    matched_reply = None
    matched_replies = []
    duplicate_replies = []
    deadline = now_ms + pick_int_config(runtime.get('timeout_ms', DEFAULT_TIMEOUT_MS), DEFAULT_TIMEOUT_MS)
    poll_interval_ms = pick_int_config(runtime.get('poll_interval_ms', DEFAULT_POLL_INTERVAL_MS), DEFAULT_POLL_INTERVAL_MS)
    duplicate_observation_window_ms = pick_int_config(
        runtime.get('duplicate_observation_window_ms', DEFAULT_DUPLICATE_OBSERVATION_WINDOW_MS),
        DEFAULT_DUPLICATE_OBSERVATION_WINDOW_MS,
    )
    while int(time.time() * 1000) <= deadline:
        conversation_result = fetch_conversation(runtime, scenario_def)
        conversation_snapshot = summarize_messages(((conversation_result.get('data') or {}).get('messages') or []))
        all_observed_messages = merge_message_snapshots(all_observed_messages, conversation_snapshot)
        matched_replies = find_replies(
            conversation_snapshot,
            scenario_def.get('expected_reply_user_id', ''),
            now_ms,
            request_msg_id,
        )
        matched_reply = matched_replies[0] if len(matched_replies) > 0 else None
        if matched_reply is not None:
            break
        time.sleep(max(poll_interval_ms, 100) / 1000.0)
    if matched_reply is not None:
        duplicate_deadline = int(time.time() * 1000) + max(0, duplicate_observation_window_ms)
        while True:
            conversation_result = fetch_conversation(runtime, scenario_def)
            conversation_snapshot = summarize_messages(((conversation_result.get('data') or {}).get('messages') or []))
            all_observed_messages = merge_message_snapshots(all_observed_messages, conversation_snapshot)
            matched_replies = find_replies(
                conversation_snapshot,
                scenario_def.get('expected_reply_user_id', ''),
                now_ms,
                request_msg_id,
            )
            matched_reply = matched_replies[0] if len(matched_replies) > 0 else None
            duplicate_replies = find_duplicate_visible_replies_by_content(
                conversation_snapshot,
                matched_reply,
                scenario_def.get('expected_reply_user_id', ''),
                now_ms,
            )
            if len(matched_replies) > 1 or int(time.time() * 1000) >= duplicate_deadline:
                break
            time.sleep(max(poll_interval_ms, 100) / 1000.0)
    expected_session_key = str(scenario_def.get('expected_session_key', '')).strip()
    reply_session_key = ''
    if isinstance((matched_reply or {}).get('ext', {}), dict):
        reply_session_key = str((((matched_reply or {}).get('ext', {}) or {}).get('openclaw', {}) or {}).get('session', '')).strip()
    relevant_mappings = select_relevant_mappings(runtime, scenario_def, now_ms)
    exact_reply_mapping = None
    if reply_session_key != '':
        exact_reply_mapping = core.get_session_mapping_by_session(runtime.get('app_id', ''), runtime.get('node_id', ''), reply_session_key)
    relevant_mappings = merge_mapping_candidates([exact_reply_mapping] if isinstance(exact_reply_mapping, dict) else [], relevant_mappings)
    mapping_sender_ok = True
    if scenario_def.get('require_mapping'):
        if isinstance(exact_reply_mapping, dict):
            mapping_sender_ok = get_mapping_sender_user_id(exact_reply_mapping) == str(scenario_def.get('sender_user_id', ''))
        elif len(relevant_mappings) == 0:
            mapping_sender_ok = False
        else:
            mapping_sender_ok = any(get_mapping_sender_user_id(mapping) == str(scenario_def.get('sender_user_id', '')) for mapping in relevant_mappings)
    session_key_ok = True
    if expected_session_key != '':
        session_key_ok = reply_session_key == expected_session_key and any(
            str(mapping.get('session_key', '')).strip() == expected_session_key for mapping in relevant_mappings
        )
    reply_from_user_id = str((matched_reply or {}).get('from_user_id', ''))
    reply_ok = matched_reply is not None and reply_from_user_id == str(scenario_def.get('expected_reply_user_id', ''))
    deduped_visible_replies = merge_reply_candidates(matched_replies, duplicate_replies)
    reply_count = len(deduped_visible_replies)
    duplicate_reply_detected = reply_count > 1
    matched_reply_msg_ids = [str(message.get('msg_id', '')).strip() for message in deduped_visible_replies if isinstance(message, dict)]
    failure_reason = ''
    status = STATUS_PASSED
    if not reply_ok:
        status = STATUS_FAILED
        failure_reason = '未拉到预期回复或回复身份不正确'
    elif duplicate_reply_detected:
        status = STATUS_FAILED
        failure_reason = '重复消息：检测到多条最终可见回复'
    elif not session_key_ok:
        status = STATUS_FAILED
        failure_reason = f"未命中预期 session_key: {expected_session_key}"
    elif not mapping_sender_ok:
        status = STATUS_FAILED
        failure_reason = 'session mapping 未体现正确发送者身份'
    scenario_sender_user_id = str(scenario_def.get('sender_user_id', '')).strip()
    validation_sender_user_id = str(runtime.get('sender_user_id', '')).strip()
    is_validation_sender = scenario_sender_user_id == validation_sender_user_id
    return {
        'status': status,
        'failure_reason': failure_reason,
        'participant_rows': [
            {'label': 'scenario_sender_user_id', 'value': scenario_sender_user_id},
            {'label': 'scenario_sender_source', 'value': 'validation_sender' if is_validation_sender else 'management_user_id'},
            {'label': 'validation_sender_user_id', 'value': validation_sender_user_id},
            {'label': 'validation_sender_username', 'value': runtime.get('sender_username', '')},
            {'label': 'validation_sender_password', 'value': runtime.get('sender_password', '')},
            {'label': 'scenario_target_user_id', 'value': scenario_def.get('target_user_id', '')},
            {'label': 'conversation_id', 'value': scenario_def.get('conversation_id', '')},
            {'label': 'chat_type', 'value': scenario_def.get('chat_type', '')},
        ] + list(runtime.get('provisioning_rows', [])),
        'request_rows': [
            {'label': 'trigger_text', 'value': unique_text},
            {'label': 'send_response', 'value': send_result},
        ],
        'comparison_rows': [
            {'label': 'expected_reply_user_id', 'value': scenario_def.get('expected_reply_user_id', '')},
            {'label': 'actual_reply_user_id', 'value': reply_from_user_id},
            {'label': 'expected_session_key', 'value': expected_session_key},
            {'label': 'actual_reply_session_key', 'value': reply_session_key},
            {'label': 'exact_reply_mapping_found', 'value': isinstance(exact_reply_mapping, dict)},
            {'label': 'exact_reply_mapping_sender_user_id', 'value': get_mapping_sender_user_id(exact_reply_mapping)},
            {'label': 'reply_found', 'value': matched_reply is not None},
            {'label': 'matched_visible_reply_count', 'value': reply_count},
            {'label': 'duplicate_reply_detected', 'value': duplicate_reply_detected},
            {'label': 'matched_visible_reply_msg_ids', 'value': matched_reply_msg_ids},
            {'label': 'duplicate_content_fallback_used', 'value': len(duplicate_replies) > len(matched_replies)},
            {'label': 'session_key_ok', 'value': session_key_ok},
            {'label': 'mapping_sender_ok', 'value': mapping_sender_ok},
        ],
        'messages': all_observed_messages,
        'mapping_rows': build_mapping_rows(runtime, relevant_mappings),
        'notes': '',
    }


def run_task(task_id):
    task = get_task(task_id)
    if not isinstance(task, dict):
        return
    task['status'] = STATUS_RUNNING
    task['started_at'] = int(time.time() * 1000)
    append_log(task, f"task started | task_id:{task_id}, app_id:{task.get('app_id', '')}, node_id:{task.get('node_id', '')}")
    write_report(task)
    runtime_result = build_runtime(task.get('app_id', ''), task.get('node_id', ''))
    if runtime_result.get('result') != 'ok':
        task['status'] = STATUS_ERROR
        task['ended_at'] = int(time.time() * 1000)
        task['scenarios'] = [{
            'name': 'bootstrap',
            'description': 'load runtime config',
            'status': STATUS_ERROR,
            'started_at': task['started_at'],
            'ended_at': task['ended_at'],
            'failure_reason': runtime_result.get('message', 'runtime init failed'),
            'participant_rows': [],
            'request_rows': [],
            'comparison_rows': [],
            'messages': [],
            'mapping_rows': [],
            'notes': '',
        }]
        append_log(task, f"task failed before scenarios | reason:{runtime_result.get('message', '')}")
        write_report(task)
        return
    runtime = runtime_result.get('data', {})
    task['scenarios'] = []
    overall_failed = False
    for scenario_name in task.get('requested_scenarios', []):
        scenario_def = build_scenario_definition(runtime, scenario_name)
        scenario_started_at = int(time.time() * 1000)
        if not isinstance(scenario_def, dict):
            scenario_result = {
                'status': STATUS_ERROR,
                'failure_reason': 'unknown scenario',
                'participant_rows': [],
                'request_rows': [],
                'comparison_rows': [],
                'messages': [],
                'mapping_rows': [],
                'notes': '',
            }
        else:
            append_log(task, f"scenario start | name:{scenario_name}")
            try:
                scenario_result = execute_scenario(runtime, scenario_def)
            except Exception as err:
                logging.exception("execute_scenario failed")
                scenario_result = {
                    'status': STATUS_ERROR,
                    'failure_reason': str(err),
                    'participant_rows': [],
                    'request_rows': [],
                    'comparison_rows': [],
                    'messages': [],
                    'mapping_rows': [],
                    'notes': '',
                }
            append_log(task, f"scenario finish | name:{scenario_name}, status:{scenario_result.get('status', '')}, reason:{scenario_result.get('failure_reason', '')}")
        task['scenarios'].append({
            'name': scenario_name,
            'description': str((scenario_def or {}).get('description', '')),
            'status': scenario_result.get('status', STATUS_ERROR),
            'started_at': scenario_started_at,
            'ended_at': int(time.time() * 1000),
            'failure_reason': scenario_result.get('failure_reason', ''),
            'participant_rows': scenario_result.get('participant_rows', []),
            'request_rows': scenario_result.get('request_rows', []),
            'comparison_rows': scenario_result.get('comparison_rows', []),
            'messages': scenario_result.get('messages', []),
            'mapping_rows': scenario_result.get('mapping_rows', []),
            'notes': scenario_result.get('notes', ''),
        })
        if scenario_result.get('status') != STATUS_PASSED:
            overall_failed = True
        write_report(task)
    task['status'] = STATUS_FAILED if overall_failed else STATUS_PASSED
    task['ended_at'] = int(time.time() * 1000)
    append_log(task, f"task finished | status:{task.get('status', '')}")
    write_report(task)


def start(app_id, node_id, scenario=None, scenarios=None):
    try:
        normalized_app_id = str(app_id or '').strip()
        normalized_node_id = str(node_id or '').strip()
        if normalized_app_id == '' or normalized_node_id == '':
            return {'result': 'error', 'message': 'app_id or node_id is empty'}
        requested_scenarios = normalize_scenarios(scenario, scenarios)
        if len(requested_scenarios) == 0:
            return {'result': 'error', 'message': 'no valid scenario selected'}
        task_id = f"sv_{int(time.time() * 1000)}_{secrets.token_hex(4)}"
        task = {
            'task_id': task_id,
            'app_id': normalized_app_id,
            'node_id': normalized_node_id,
            'status': STATUS_PENDING,
            'requested_scenarios': requested_scenarios,
            'started_at': 0,
            'ended_at': 0,
            'task_dir': get_task_dir(task_id),
            'report_path': get_report_path(task_id),
            'log_path': get_log_path(task_id),
            'metadata_path': get_metadata_path(task_id),
            'scenarios': [],
        }
        with tasks_lock:
            tasks[task_id] = task
        os.makedirs(task['task_dir'], exist_ok=True)
        write_report(task)
        executor.submit(run_task, task_id)
        return {
            'result': 'ok',
            'data': {
                'task_id': task_id,
                'task_dir': task['task_dir'],
                'report_path': task['report_path'],
                'status_url': f"/service/openclaw/sync_validation/{task_id}",
            },
        }
    except Exception as err:
        logging.exception("start sync validation failed")
        return {
            'result': 'error',
            'message': f'sync validation bootstrap failed: {str(err)}',
        }
