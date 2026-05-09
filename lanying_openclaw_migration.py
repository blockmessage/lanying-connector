"""
OpenClaw session-mapping inspection and migration helpers.

Common call patterns:

1. Inspect one node and render HTML:
   render_inspect_session_mapping_group_states_html_for_node(app_id, node_id)

2. Inspect one node and get structured reports:
   inspect_session_mapping_group_states_for_node(app_id, {"node_id": "15"})
   inspect_session_mapping_group_states_for_node(app_id, full_node_info_dict)

3. Migrate one session:
   migrate_inspected_session_mapping_group_state(app_id, "15", session_key, dry_run=True)
   migrate_inspected_session_mapping_group_state(app_id, "15", session_key, dry_run=False)
   migrate_inspected_session_mapping_group_state(app_id, {"node_id": "15"}, session_key, dry_run=True)
   migrate_inspected_session_mapping_group_state(app_id, full_node_info_dict, session_key, dry_run=False)

4. Migrate all dirty sessions for one node:
   migrate_inspected_session_mapping_group_states_for_node(app_id, "15", dry_run=True)
   migrate_inspected_session_mapping_group_states_for_node(app_id, "15", dry_run=False)
   migrate_inspected_session_mapping_group_states_for_node(app_id, {"node_id": "15"}, dry_run=True)
   migrate_inspected_session_mapping_group_states_for_node(app_id, full_node_info_dict, dry_run=False)

5. Migrate all nodes for one app:
   migrate_inspected_session_mapping_group_states_for_app(app_id, dry_run=True)
   migrate_inspected_session_mapping_group_states_for_app(app_id, dry_run=False)

6. Migrate all nodes for all apps:
   migrate_inspected_session_mapping_group_states_for_all_apps(dry_run=True)
   migrate_inspected_session_mapping_group_states_for_all_apps(dry_run=False)

Notes:
- dry_run=True only predicts the post-migration report and does not write data or call IM mutation APIs.
- node_info may be a full node dict or just a node_id string where supported.
"""

import html
import logging
from datetime import datetime
import lanying_im_api
import lanying_openclaw


def _escape_session_mapping_html(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return html.escape(str(value), quote=True)


def _build_session_mapping_html_table(rows):
    return '<table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;">' + ''.join(rows) + '</table>'


def _build_session_mapping_html_key_value_rows(data, ordered_keys=None):
    if not isinstance(data, dict):
        return ['<tr><td colspan="2"></td></tr>']
    rows = []
    seen_keys = set()
    keys = []
    if isinstance(ordered_keys, list):
        for key in ordered_keys:
            if key in data:
                keys.append(key)
                seen_keys.add(key)
    for key in data.keys():
        if key not in seen_keys:
            keys.append(key)
            seen_keys.add(key)
    for key in keys:
        rows.append(
            '<tr>'
            f'<th align="left" valign="top">{_escape_session_mapping_html(key)}</th>'
            f'<td valign="top">{_escape_session_mapping_html(data.get(key, ""))}</td>'
            '</tr>'
        )
    if len(rows) == 0:
        rows.append('<tr><td colspan="2"></td></tr>')
    return rows


def _format_unix_timestamp(value, scale='seconds'):
    try:
        numeric_value = int(value)
    except Exception:
        return value
    if numeric_value <= 0:
        return value
    try:
        timestamp = numeric_value / 1000.0 if scale == 'milliseconds' else numeric_value
        formatted = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return f'{numeric_value} ({formatted})'
    except Exception:
        return value


def _annotate_time_fields(data, second_keys=None, millisecond_keys=None):
    if not isinstance(data, dict):
        return {}
    annotated = {}
    second_key_set = set(second_keys or [])
    millisecond_key_set = set(millisecond_keys or [])
    for key, value in data.items():
        if key in second_key_set:
            annotated[key] = _format_unix_timestamp(value, scale='seconds')
        elif key in millisecond_key_set:
            annotated[key] = _format_unix_timestamp(value, scale='milliseconds')
        else:
            annotated[key] = value
    return annotated


def render_inspect_session_mapping_group_states_html_for_node(app_id, node_id):
    details = sorted(
        lanying_openclaw.list_session_mapping_details_for_node(app_id, node_id),
        key=lambda item: str((item or {}).get('session_key', '')).strip(),
    )
    node_info = {
        'node_id': str(node_id).strip(),
    }
    inspect_result = inspect_session_mapping_group_states_for_node(app_id, node_info)
    reports_by_session_key = {}
    if isinstance(inspect_result, dict):
        for report in sorted(
            list(inspect_result.get('data', {}).get('mapping_reports', []) or []),
            key=lambda item: str((item or {}).get('session_key', '')).strip(),
        ):
            session_key = str(report.get('session_key', '')).strip()
            if session_key != '':
                reports_by_session_key[session_key] = report
    header = (
        '<tr>'
        '<th>session_key</th>'
        '<th>session_facts</th>'
        '<th>mapping</th>'
        '<th>group_info</th>'
        '<th>member_summary</th>'
        '<th>key_user_status</th>'
        '<th>errors</th>'
        '<th>group</th>'
        '<th>status</th>'
        '<th>owner</th>'
        '<th>issues</th>'
        '<th>proposed_changes</th>'
        '</tr>'
    )
    rows = [header]
    for detail in details:
        session_key = str(detail.get('session_key', '')).strip()
        report = reports_by_session_key.get(session_key, {})
        session_facts = lanying_openclaw.get_session_key_facts(session_key)
        mapping_summary = _annotate_time_fields({
            'group_id': detail.get('group_id', ''),
            'openclaw_user_id': detail.get('openclaw_user_id', ''),
            'management_user_id': detail.get('management_user_id', ''),
            'origin_user_id': detail.get('origin_user_id', ''),
            'chatbot_user_id': detail.get('chatbot_user_id', ''),
            'parent_session_key': detail.get('parent_session_key', ''),
            'root_session_key': detail.get('root_session_key', ''),
            'effective_target_session_key': detail.get('effective_target_session_key', ''),
            'created_at': detail.get('created_at', ''),
            'updated_at': detail.get('updated_at', ''),
        }, second_keys=['created_at', 'updated_at'])
        session_facts_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                session_facts,
                ['canonical_session_key', 'channel', 'chat_type', 'target_id', 'is_legacy_alias', 'is_router', 'is_group', 'is_direct', 'is_subagent'],
            )
        )
        group_info_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                _annotate_time_fields(
                    detail.get('group_info', {}),
                    millisecond_keys=['created_at', 'updated_at'],
                ),
                ['group_id', 'name', 'owner_id', 'count', 'type', 'status', 'created_at', 'updated_at'],
            )
        )
        member_summary = dict(detail.get('member_summary', {}))
        member_rows = _build_session_mapping_html_key_value_rows(
            {
                'member_count_reported': member_summary.get('member_count_reported', ''),
                'member_count_loaded': member_summary.get('member_count_loaded', ''),
                'members_loaded_complete': member_summary.get('members_loaded_complete', ''),
            }
        )
        for member in member_summary.get('members', []):
            if not isinstance(member, dict):
                continue
            member_rows.append(
                '<tr><td colspan="2">'
                + _build_session_mapping_html_table(
                    _build_session_mapping_html_key_value_rows(
                        member,
                        ['user_id', 'display_name', 'join_time', 'expired_time'],
                    )
                )
                + '</td></tr>'
            )
        member_summary_html = _build_session_mapping_html_table(member_rows)
        key_user_rows = []
        for role_key, status in detail.get('key_user_status', {}).items():
            key_user_rows.append(
                '<tr><td valign="top">'
                f'{_escape_session_mapping_html(role_key)}'
                '</td><td>'
                + _build_session_mapping_html_table(
                    _build_session_mapping_html_key_value_rows(
                        status,
                        ['user_id', 'present_in_group', 'is_group_owner', 'admin_status'],
                    )
                )
                + '</td></tr>'
            )
        key_user_status_html = _build_session_mapping_html_table(key_user_rows or ['<tr><td colspan="2"></td></tr>'])
        error_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                {
                    'group_info_error': detail.get('group_info_error', ''),
                    'member_list_error': detail.get('member_list_error', ''),
                    'admin_list_error': detail.get('admin_list_error', ''),
                    'member_list_viewer_user_id': detail.get('member_list_viewer_user_id', ''),
                    'admin_list_viewer_user_id': detail.get('admin_list_viewer_user_id', ''),
                }
            )
        )
        group_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                {
                    'group_id': report.get('group_id', ''),
                    'root_mode': report.get('root_mode', ''),
                }
            )
        )
        owner_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                {
                    'current_owner_user_id': report.get('current_owner_user_id', ''),
                    'expected_owner_user_id': report.get('expected_owner_user_id', ''),
                }
            )
        )
        issue_rows = []
        for issue in report.get('issues', []):
            issue_rows.append(
                '<tr><td colspan="2">'
                + _build_session_mapping_html_table(
                    _build_session_mapping_html_key_value_rows(
                        issue,
                        ['severity', 'code', 'summary', 'current', 'expected'],
                    )
                )
                + '</td></tr>'
            )
        issues_html = _build_session_mapping_html_table(issue_rows or ['<tr><td colspan="2"></td></tr>'])
        change_rows = []
        for change in report.get('proposed_changes', []):
            change_rows.append(
                '<tr><td colspan="2">'
                + _build_session_mapping_html_table(
                    _build_session_mapping_html_key_value_rows(
                        change,
                        ['action', 'target_type', 'group_id', 'session_key', 'field', 'user_id', 'from', 'to', 'reason', 'risk'],
                    )
                )
                + '</td></tr>'
            )
        changes_html = _build_session_mapping_html_table(change_rows or ['<tr><td colspan="2"></td></tr>'])
        rows.append(
            '<tr>'
            f'<td valign="top">{_escape_session_mapping_html(session_key)}</td>'
            f'<td valign="top">{session_facts_html}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(mapping_summary))}</td>'
            f'<td valign="top">{group_info_html}</td>'
            f'<td valign="top">{member_summary_html}</td>'
            f'<td valign="top">{key_user_status_html}</td>'
            f'<td valign="top">{error_html}</td>'
            f'<td valign="top">{group_html}</td>'
            f'<td valign="top">{_escape_session_mapping_html(report.get("status", ""))}</td>'
            f'<td valign="top">{owner_html}</td>'
            f'<td valign="top">{issues_html}</td>'
            f'<td valign="top">{changes_html}</td>'
            '</tr>'
        )
    return _build_session_mapping_html_table(rows)


def render_inspect_session_mapping_group_state_html_for_session(app_id, node_id, session_key, inspect_result=None, detail=None):
    normalized_node_id = str(node_id).strip()
    normalized_session_key = str(session_key).strip()
    if normalized_node_id == '' or normalized_session_key == '':
        return _build_session_mapping_html_table(['<tr><td colspan="12"></td></tr>'])
    if not isinstance(detail, dict):
        detail = lanying_openclaw.get_session_mapping_detail_by_session(
            app_id,
            normalized_node_id,
            normalized_session_key,
        )
    if not isinstance(detail, dict):
        return _build_session_mapping_html_table(['<tr><td colspan="12"></td></tr>'])
    report = _find_mapping_report_by_session_key(inspect_result or {}, normalized_session_key) or {}
    session_facts = lanying_openclaw.get_session_key_facts(normalized_session_key)
    mapping_summary = _annotate_time_fields({
        'group_id': detail.get('group_id', ''),
        'openclaw_user_id': detail.get('openclaw_user_id', ''),
        'management_user_id': detail.get('management_user_id', ''),
        'origin_user_id': detail.get('origin_user_id', ''),
        'chatbot_user_id': detail.get('chatbot_user_id', ''),
        'parent_session_key': detail.get('parent_session_key', ''),
        'root_session_key': detail.get('root_session_key', ''),
        'effective_target_session_key': detail.get('effective_target_session_key', ''),
        'created_at': detail.get('created_at', ''),
        'updated_at': detail.get('updated_at', ''),
    }, second_keys=['created_at', 'updated_at'])
    session_facts_html = _build_session_mapping_html_table(
        _build_session_mapping_html_key_value_rows(
            session_facts,
            ['canonical_session_key', 'channel', 'chat_type', 'target_id', 'is_legacy_alias', 'is_router', 'is_group', 'is_direct', 'is_subagent'],
        )
    )
    group_info_html = _build_session_mapping_html_table(
        _build_session_mapping_html_key_value_rows(
            _annotate_time_fields(
                detail.get('group_info', {}),
                millisecond_keys=['created_at', 'updated_at'],
            ),
            ['group_id', 'name', 'owner_id', 'count', 'type', 'status', 'created_at', 'updated_at'],
        )
    )
    member_summary = dict(detail.get('member_summary', {}))
    member_rows = _build_session_mapping_html_key_value_rows(
        {
            'member_count_reported': member_summary.get('member_count_reported', ''),
            'member_count_loaded': member_summary.get('member_count_loaded', ''),
            'members_loaded_complete': member_summary.get('members_loaded_complete', ''),
        }
    )
    for member in member_summary.get('members', []):
        if not isinstance(member, dict):
            continue
        member_rows.append(
            '<tr><td colspan="2">'
            + _build_session_mapping_html_table(
                _build_session_mapping_html_key_value_rows(
                    member,
                    ['user_id', 'display_name', 'join_time', 'expired_time'],
                )
            )
            + '</td></tr>'
        )
    member_summary_html = _build_session_mapping_html_table(member_rows)
    key_user_rows = []
    for role_key, status in detail.get('key_user_status', {}).items():
        key_user_rows.append(
            '<tr><td valign="top">'
            f'{_escape_session_mapping_html(role_key)}'
            '</td><td>'
            + _build_session_mapping_html_table(
                _build_session_mapping_html_key_value_rows(
                    status,
                    ['user_id', 'present_in_group', 'is_group_owner', 'admin_status'],
                )
            )
            + '</td></tr>'
        )
    key_user_status_html = _build_session_mapping_html_table(key_user_rows or ['<tr><td colspan="2"></td></tr>'])
    error_html = _build_session_mapping_html_table(
        _build_session_mapping_html_key_value_rows(
            {
                'group_info_error': detail.get('group_info_error', ''),
                'member_list_error': detail.get('member_list_error', ''),
                'admin_list_error': detail.get('admin_list_error', ''),
                'member_list_viewer_user_id': detail.get('member_list_viewer_user_id', ''),
                'admin_list_viewer_user_id': detail.get('admin_list_viewer_user_id', ''),
            }
        )
    )
    group_html = _build_session_mapping_html_table(
        _build_session_mapping_html_key_value_rows(
            {
                'group_id': report.get('group_id', ''),
                'root_mode': report.get('root_mode', ''),
            }
        )
    )
    owner_html = _build_session_mapping_html_table(
        _build_session_mapping_html_key_value_rows(
            {
                'current_owner_user_id': report.get('current_owner_user_id', ''),
                'expected_owner_user_id': report.get('expected_owner_user_id', ''),
            }
        )
    )
    issue_rows = []
    for issue in report.get('issues', []):
        issue_rows.append(
            '<tr><td colspan="2">'
            + _build_session_mapping_html_table(
                _build_session_mapping_html_key_value_rows(
                    issue,
                    ['severity', 'code', 'summary', 'current', 'expected'],
                )
            )
            + '</td></tr>'
        )
    issues_html = _build_session_mapping_html_table(issue_rows or ['<tr><td colspan="2"></td></tr>'])
    change_rows = []
    for change in report.get('proposed_changes', []):
        change_rows.append(
            '<tr><td colspan="2">'
            + _build_session_mapping_html_table(
                _build_session_mapping_html_key_value_rows(
                    change,
                    ['action', 'target_type', 'group_id', 'session_key', 'field', 'user_id', 'from', 'to', 'reason', 'risk'],
                )
            )
            + '</td></tr>'
        )
    changes_html = _build_session_mapping_html_table(change_rows or ['<tr><td colspan="2"></td></tr>'])
    header = (
        '<tr>'
        '<th>session_key</th>'
        '<th>session_facts</th>'
        '<th>mapping</th>'
        '<th>group_info</th>'
        '<th>member_summary</th>'
        '<th>key_user_status</th>'
        '<th>errors</th>'
        '<th>group</th>'
        '<th>status</th>'
        '<th>owner</th>'
        '<th>issues</th>'
        '<th>proposed_changes</th>'
        '</tr>'
    )
    row = (
        '<tr>'
        f'<td valign="top">{_escape_session_mapping_html(normalized_session_key)}</td>'
        f'<td valign="top">{session_facts_html}</td>'
        f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(mapping_summary))}</td>'
        f'<td valign="top">{group_info_html}</td>'
        f'<td valign="top">{member_summary_html}</td>'
        f'<td valign="top">{key_user_status_html}</td>'
        f'<td valign="top">{error_html}</td>'
        f'<td valign="top">{group_html}</td>'
        f'<td valign="top">{_escape_session_mapping_html(report.get("status", ""))}</td>'
        f'<td valign="top">{owner_html}</td>'
        f'<td valign="top">{issues_html}</td>'
        f'<td valign="top">{changes_html}</td>'
        '</tr>'
    )
    return _build_session_mapping_html_table([header, row])


def _append_group_state_issue(issues, proposed_changes, severity, code, summary, current=None, expected=None, proposal=None):
    issues.append({
        'severity': severity,
        'code': code,
        'summary': summary,
        'current': current,
        'expected': expected,
    })
    if isinstance(proposal, dict):
        proposed_changes.append(proposal)


def _build_group_relation_change(action, group_id, user_id, current, target, reason, risk='low'):
    return {
        'action': action,
        'target_type': 'group_relation',
        'group_id': str(group_id).strip(),
        'user_id': str(user_id).strip(),
        'from': current,
        'to': target,
        'reason': reason,
        'risk': risk,
    }


def _is_clawchat_session_root_mode(root_mode):
    return root_mode in ['clawchat_group', 'clawchat_direct', 'router_group', 'router_direct']


def _build_mapping_field_change(session_key, field, current, target, reason, risk='low'):
    return {
        'action': 'mapping_field_update',
        'target_type': 'session_mapping',
        'session_key': str(session_key).strip(),
        'field': str(field).strip(),
        'from': current,
        'to': target,
        'reason': reason,
        'risk': risk,
    }


def _resolve_effective_key_user_status(detail, role_key, effective_user_id):
    normalized_user_id = str(effective_user_id).strip()
    key_user_status = dict((detail or {}).get('key_user_status', {}))
    direct_status = dict(key_user_status.get(role_key, {}))
    if str(direct_status.get('user_id', '')).strip() == normalized_user_id:
        return direct_status

    for status in key_user_status.values():
        if not isinstance(status, dict):
            continue
        if str(status.get('user_id', '')).strip() == normalized_user_id:
            return dict(status)

    owner_user_id = str(dict((detail or {}).get('group_info', {})).get('owner_id', '')).strip()
    member_user_ids = set()
    member_summary = dict((detail or {}).get('member_summary', {}))
    for member in list(member_summary.get('members', []) or []):
        if isinstance(member, dict):
            user_id = str(member.get('user_id', '')).strip()
            if user_id != '':
                member_user_ids.add(user_id)
    admin_list_error = str((detail or {}).get('admin_list_error', '')).strip()
    return {
        'user_id': normalized_user_id,
        'present_in_group': normalized_user_id != '' and normalized_user_id in member_user_ids,
        'is_group_owner': normalized_user_id != '' and normalized_user_id == owner_user_id,
        'admin_status': 'unknown' if normalized_user_id == '' or admin_list_error != '' else 'not_admin',
    }


def _analyze_session_mapping_group_detail(app_id, node_info, detail, default_management_user_id='', bound_chatbot_user_id=''):
    openclaw = lanying_openclaw
    normalized_detail = openclaw.normalize_session_mapping_record(detail)
    session_key = str(normalized_detail.get('session_key', '')).strip()
    group_id = str(normalized_detail.get('group_id', '')).strip()
    issues = []
    proposed_changes = []
    if group_id == '':
        return {
            'session_key': session_key,
            'group_id': '',
            'status': 'ignored',
            'issues': issues,
            'proposed_changes': proposed_changes,
        }
    group_info_error = str(normalized_detail.get('group_info_error', '')).strip()
    member_list_error = str(normalized_detail.get('member_list_error', '')).strip()
    admin_list_error = str(normalized_detail.get('admin_list_error', '')).strip()
    if group_info_error != '':
        _append_group_state_issue(
            issues,
            proposed_changes,
            'warning',
            'group_info_unavailable',
            '群信息读取失败，无法可靠判断群主是否正确',
            current=group_info_error,
            expected='group info available',
        )
    if member_list_error != '':
        _append_group_state_issue(
            issues,
            proposed_changes,
            'warning',
            'member_list_unavailable',
            '群成员列表读取失败，无法可靠判断成员是否齐全',
            current=member_list_error,
            expected='member list available',
        )
    if admin_list_error != '':
        _append_group_state_issue(
            issues,
            proposed_changes,
            'warning',
            'admin_list_unavailable',
            '群管理员列表读取失败，无法可靠判断管理员是否正确',
            current=admin_list_error,
            expected='admin list available',
        )

    group_info = dict(normalized_detail.get('group_info', {}))
    owner_user_id = str(group_info.get('owner_id', '')).strip()
    root_session_key = str(normalized_detail.get('root_session_key', '')).strip()
    if root_session_key == '':
        root_session_key = session_key
    root_identity = openclaw.parse_clawchat_session_identity(root_session_key)
    root_mode = openclaw.resolve_root_session_sync_mode(root_identity)
    openclaw_user_id = str(normalized_detail.get('openclaw_user_id', '')).strip()
    management_user_id = str(normalized_detail.get('management_user_id', '')).strip()
    origin_user_id = str(normalized_detail.get('origin_user_id', '')).strip()
    chatbot_user_id = str(normalized_detail.get('chatbot_user_id', '')).strip()
    group_type = group_info.get('type')
    is_temporary_group = int(group_type) == openclaw.TEMPORARY_GROUP_TYPE if group_type not in [None, ''] else False
    is_clawchat_session = _is_clawchat_session_root_mode(root_mode)

    if management_user_id == '' and str(default_management_user_id).strip() != '':
        _append_group_state_issue(
            issues,
            proposed_changes,
            'error',
            'missing_management_user_id',
            'mapping 缺少 management_user_id',
            current='',
            expected=str(default_management_user_id).strip(),
            proposal=_build_mapping_field_change(
                session_key,
                'management_user_id',
                '',
                str(default_management_user_id).strip(),
                '补齐 app manager user，便于后续管理员修复',
            ),
        )
        management_user_id = str(default_management_user_id).strip()

    expected_owner_user_id = ''
    if openclaw.is_router_root_session(root_identity):
        router_expected_owner_user_id = chatbot_user_id or str(bound_chatbot_user_id).strip()
        if chatbot_user_id == '' and str(bound_chatbot_user_id).strip() != '':
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_chatbot_user_id',
                'router session mapping 缺少 chatbot_user_id',
                current='',
                expected=str(bound_chatbot_user_id).strip(),
                proposal=_build_mapping_field_change(
                    session_key,
                    'chatbot_user_id',
                    '',
                    str(bound_chatbot_user_id).strip(),
                    'router session 应绑定 chatbot_user_id',
                ),
            )
            chatbot_user_id = str(bound_chatbot_user_id).strip()
        if is_temporary_group:
            expected_owner_user_id = router_expected_owner_user_id
    else:
        if is_temporary_group:
            expected_owner_user_id = openclaw_user_id or management_user_id

    required_members = []
    if openclaw.is_router_root_session(root_identity):
        if chatbot_user_id != '':
            required_members.append(('chatbot_user_id', chatbot_user_id))
        if origin_user_id != '' and origin_user_id != chatbot_user_id:
            required_members.append(('origin_user_id', origin_user_id))
    else:
        if openclaw_user_id != '':
            required_members.append(('openclaw_user_id', openclaw_user_id))
        if (
            isinstance(root_identity, dict) and
            root_identity.get('chat_type') in ['group', 'direct'] and
            origin_user_id != '' and
            origin_user_id != openclaw_user_id
        ):
            required_members.append(('origin_user_id', origin_user_id))

    for role_key, user_id in required_members:
        status = _resolve_effective_key_user_status(normalized_detail, role_key, user_id)
        if not bool(status.get('present_in_group', False)):
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_required_member',
                f'{role_key} 不在群成员中',
                current={'role': role_key, 'user_id': user_id, 'present_in_group': False},
                expected={'role': role_key, 'user_id': user_id, 'present_in_group': True},
                proposal=_build_group_relation_change(
                    'group_member_add',
                    group_id,
                    user_id,
                    'not_in_group',
                    'member',
                    f'补齐 {role_key} 的群成员关系',
                ),
            )

    key_user_status = dict(normalized_detail.get('key_user_status', {}))
    management_status = dict(key_user_status.get('management_user_id', {}))
    management_present_in_group = bool(management_status.get('present_in_group', False))
    management_admin_status = str(management_status.get('admin_status', '')).strip()
    is_regular_group = not is_temporary_group

    if not is_clawchat_session and management_user_id != '':
        if not bool(management_status.get('present_in_group', False)):
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_management_group_member',
                '非 ClawChat session 的 management_user_id 必须提前加入群',
                current={'role': 'management_user_id', 'user_id': management_user_id, 'present_in_group': False},
                expected={'role': 'management_user_id', 'user_id': management_user_id, 'present_in_group': True},
                proposal=_build_group_relation_change(
                    'group_member_add',
                    group_id,
                    management_user_id,
                    'not_in_group',
                    'member',
                    '补齐 management_user_id 的群成员关系',
                ),
            )
        if str(admin_list_error).strip() == '' and str(management_status.get('admin_status', '')).strip() != 'admin':
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_management_group_admin',
                '非 ClawChat session 的 management_user_id 必须成为群管理员',
                current={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': management_status.get('admin_status', '')},
                expected={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': 'admin'},
                proposal=_build_group_relation_change(
                    'group_admin_add',
                    group_id,
                    management_user_id,
                    management_admin_status or 'unknown',
                    'admin',
                    '补齐 management_user_id 的群管理员关系',
                ),
            )
    elif is_clawchat_session and is_regular_group and management_user_id != '':
        if (
            management_present_in_group and
            str(admin_list_error).strip() == '' and
            management_admin_status != 'admin'
        ):
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_management_group_admin',
                '普通群中的 ClawChat session，management_user_id 已在群里时也必须成为群管理员',
                current={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': management_status.get('admin_status', '')},
                expected={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': 'admin'},
                proposal=_build_group_relation_change(
                    'group_admin_add',
                    group_id,
                    management_user_id,
                    management_admin_status or 'unknown',
                    'admin',
                    '补齐普通群中 management_user_id 的群管理员关系',
                ),
            )

    if (
        is_temporary_group and
        expected_owner_user_id != '' and
        owner_user_id != '' and
        owner_user_id != expected_owner_user_id
    ):
        previous_expected_owner = ''
        if expected_owner_user_id == chatbot_user_id:
            previous_expected_owner = openclaw_user_id
        elif expected_owner_user_id == openclaw_user_id:
            previous_expected_owner = chatbot_user_id
        _append_group_state_issue(
            issues,
            proposed_changes,
            'error',
            'temporary_group_owner_mismatch',
            '临时群群主与当前 session mapping 推导结果不一致，必须转移群主',
            current=owner_user_id,
            expected=expected_owner_user_id,
            proposal={
                'action': 'group_owner_transfer',
                'target_type': 'group_relation',
                'group_id': group_id,
                'from': owner_user_id,
                'to': expected_owner_user_id,
                'reason': '临时群群主必须与 session mapping 推导结果一致',
                'risk': 'medium',
            },
        )
        if previous_expected_owner != '':
            proposed_changes.append(
                _build_group_relation_change(
                    'group_member_remove',
                    group_id,
                    previous_expected_owner,
                    'member_or_owner',
                    'removed',
                    '群主转移后，旧的会话 owner 需要退出临时群',
                    risk='medium',
                )
            )

    return {
        'session_key': session_key,
        'group_id': group_id,
        'root_mode': root_mode,
        'is_temporary_group': is_temporary_group,
        'is_clawchat_session': is_clawchat_session,
        'expected_owner_user_id': expected_owner_user_id,
        'current_owner_user_id': owner_user_id,
        'issues': issues,
        'proposed_changes': proposed_changes,
        'status': 'dirty' if len(issues) > 0 else 'clean',
    }


def inspect_session_mapping_group_states_for_node(app_id, node_info):
    openclaw = lanying_openclaw
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    default_management_user_id = ''
    app_user_result = openclaw.ensure_openclaw_app_manager_user(app_id)
    if isinstance(app_user_result, dict) and app_user_result.get('result') == 'ok':
        default_management_user_id = str(app_user_result.get('data', {}).get('user_id', '')).strip()
    bound_chatbot_user_id = openclaw.resolve_bound_chatbot_user_id(app_id, node_id)
    details = openclaw.list_session_mapping_details_for_node(app_id, node_id)
    reports = []
    dirty_count = 0
    for detail in details:
        report = _analyze_session_mapping_group_detail(
            app_id,
            node_info,
            detail,
            default_management_user_id=default_management_user_id,
            bound_chatbot_user_id=bound_chatbot_user_id,
        )
        if report.get('group_id', '') == '':
            continue
        if report.get('status') == 'dirty':
            dirty_count += 1
        reports.append(report)
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'default_management_user_id': default_management_user_id,
            'bound_chatbot_user_id': bound_chatbot_user_id,
            'checked_mapping_count': len(reports),
            'dirty_mapping_count': dirty_count,
            'mapping_reports': reports,
        }
    }


def inspect_session_mapping_group_state_for_session(app_id, node_info, session_key):
    openclaw = lanying_openclaw
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    normalized_session_key = str(session_key).strip()
    if node_id == '' or normalized_session_key == '':
        return {'result': 'ignored', 'message': 'bad node id or session key'}
    default_management_user_id = ''
    app_user_result = openclaw.ensure_openclaw_app_manager_user(app_id)
    if isinstance(app_user_result, dict) and app_user_result.get('result') == 'ok':
        default_management_user_id = str(app_user_result.get('data', {}).get('user_id', '')).strip()
    bound_chatbot_user_id = openclaw.resolve_bound_chatbot_user_id(app_id, node_id)
    detail = openclaw.get_session_mapping_detail_by_session(app_id, node_id, normalized_session_key)
    if not isinstance(detail, dict):
        return {'result': 'error', 'message': 'session mapping detail not found'}
    report = _analyze_session_mapping_group_detail(
        app_id,
        node_info,
        detail,
        default_management_user_id=default_management_user_id,
        bound_chatbot_user_id=bound_chatbot_user_id,
    )
    mapping_reports = []
    dirty_count = 0
    if report.get('group_id', '') != '':
        mapping_reports.append(report)
        if report.get('status') == 'dirty':
            dirty_count = 1
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'default_management_user_id': default_management_user_id,
            'bound_chatbot_user_id': bound_chatbot_user_id,
            'checked_mapping_count': len(mapping_reports),
            'dirty_mapping_count': dirty_count,
            'mapping_reports': mapping_reports,
        }
    }


def _find_mapping_report_by_session_key(inspect_result, session_key):
    normalized_session_key = str(session_key).strip()
    if not isinstance(inspect_result, dict):
        return None
    for report in list(inspect_result.get('data', {}).get('mapping_reports', []) or []):
        if str(report.get('session_key', '')).strip() == normalized_session_key:
            return report
    return None


def _resolve_node_info(app_id, node_info):
    if isinstance(node_info, dict):
        normalized_node_info = dict(node_info)
        normalized_node_info['node_id'] = str(normalized_node_info.get('node_id', '')).strip()
        return normalized_node_info
    node_id = str(node_info).strip()
    if node_id == '':
        return None
    loaded_node_info = lanying_openclaw.get_node(app_id, node_id)
    if isinstance(loaded_node_info, dict):
        normalized_node_info = dict(loaded_node_info)
        normalized_node_info['node_id'] = str(normalized_node_info.get('node_id', node_id)).strip() or node_id
        return normalized_node_info
    return {'node_id': node_id}


def _predict_report_after_changes(report):
    predicted = dict(report or {})
    predicted['issues'] = []
    predicted['proposed_changes'] = []
    predicted['status'] = 'clean'
    for change in list((report or {}).get('proposed_changes', []) or []):
        action = str(change.get('action', '')).strip()
        if action == 'group_owner_transfer':
            predicted['current_owner_user_id'] = str(change.get('to', '')).strip()
        elif action == 'mapping_field_update' and str(change.get('field', '')).strip() == 'management_user_id':
            predicted['management_user_id'] = change.get('to', '')
        elif action == 'mapping_field_update' and str(change.get('field', '')).strip() == 'chatbot_user_id':
            predicted['chatbot_user_id'] = change.get('to', '')
    return predicted


def _apply_group_state_change(app_id, node_id, session_key, change):
    openclaw = lanying_openclaw
    normalized_change = dict(change or {})
    action = str(normalized_change.get('action', '')).strip()
    if action == 'mapping_field_update':
        mapping = openclaw.get_session_mapping_by_session(app_id, node_id, session_key)
        if not isinstance(mapping, dict):
            return {'result': 'error', 'message': 'session mapping not found'}
        field = str(normalized_change.get('field', '')).strip()
        mapping[field] = normalized_change.get('to', '')
        return openclaw.set_session_mapping(app_id, node_id, mapping)
    group_id = str(normalized_change.get('group_id', '')).strip()
    user_id = str(normalized_change.get('user_id', '')).strip()
    if action == 'group_member_add':
        response = lanying_im_api.admin_join_group_direct(app_id, group_id, [int(user_id)])
        return {'result': 'ok' if isinstance(response, dict) and response.get('code') == 200 else 'error', 'data': response}
    if action == 'group_admin_add':
        response = lanying_im_api.admin_add_group_admin(app_id, group_id, [int(user_id)])
        return {'result': 'ok' if isinstance(response, dict) and response.get('code') == 200 else 'error', 'data': response}
    if action == 'group_owner_transfer':
        response = lanying_im_api.group_owner_transfer(
            app_id,
            group_id,
            normalized_change.get('from', ''),
            normalized_change.get('to', ''),
        )
        return {'result': 'ok' if isinstance(response, dict) and response.get('code') == 200 else 'error', 'data': response}
    if action == 'group_member_remove':
        response = lanying_im_api.admin_kick_group_member(app_id, group_id, [int(user_id)])
        return {'result': 'ok' if isinstance(response, dict) and response.get('code') == 200 else 'error', 'data': response}
    return {'result': 'ignored', 'message': f'unsupported action: {action}'}


def _proposed_changes_signature(report):
    signature_parts = []
    for change in list((report or {}).get('proposed_changes', []) or []):
        normalized_change = dict(change or {})
        signature_parts.append((
            str(normalized_change.get('action', '')).strip(),
            str(normalized_change.get('target_type', '')).strip(),
            str(normalized_change.get('group_id', '')).strip(),
            str(normalized_change.get('session_key', '')).strip(),
            str(normalized_change.get('field', '')).strip(),
            str(normalized_change.get('user_id', '')).strip(),
            str(normalized_change.get('from', '')),
            str(normalized_change.get('to', '')),
        ))
    return tuple(signature_parts)


def migrate_inspected_session_mapping_group_state(app_id, node_info, session_key, dry_run=False):
    normalized_node_info = _resolve_node_info(app_id, node_info)
    node_id = str((normalized_node_info or {}).get('node_id', '')).strip()
    normalized_session_key = str(session_key).strip()
    if node_id == '' or normalized_session_key == '':
        return {'result': 'error', 'message': 'bad node_id or session_key'}
    before_inspect = inspect_session_mapping_group_state_for_session(
        app_id,
        normalized_node_info,
        normalized_session_key,
    )
    before_report = _find_mapping_report_by_session_key(before_inspect, normalized_session_key)
    if not isinstance(before_report, dict):
        return {'result': 'error', 'message': 'session mapping inspect report not found'}
    before_html = render_inspect_session_mapping_group_state_html_for_session(
        app_id,
        node_id,
        normalized_session_key,
        inspect_result=before_inspect,
    )
    logging.info(f"migrate_inspected_session_mapping_group_state before | app_id:{app_id}, node_id:{node_id}, session_key:{normalized_session_key}, report:{before_report}")
    applied_changes = []
    if dry_run:
        predicted_after_report = _predict_report_after_changes(before_report)
        return {
            'result': 'ok',
            'data': {
                'session_key': normalized_session_key,
                'dry_run': True,
                'before_report': before_report,
                'after_report': predicted_after_report,
                'applied_changes': applied_changes,
                'before_html': before_html,
                'after_html': before_html,
            }
        }
    max_rounds = 5
    seen_signatures = set()
    current_report = before_report
    after_report = before_report
    stop_reason = 'max_rounds_reached'
    for round_index in range(max_rounds):
        if not isinstance(current_report, dict):
            stop_reason = 'report_missing'
            break
        if str(current_report.get('status', '')).strip() != 'dirty':
            after_report = current_report
            stop_reason = 'clean'
            break
        proposed_changes = list(current_report.get('proposed_changes', []) or [])
        if len(proposed_changes) == 0:
            after_report = current_report
            stop_reason = 'no_proposed_changes'
            break
        report_signature = _proposed_changes_signature(current_report)
        if report_signature in seen_signatures:
            after_report = current_report
            stop_reason = 'repeated_proposed_changes'
            break
        seen_signatures.add(report_signature)
        for change in proposed_changes:
            apply_result = _apply_group_state_change(app_id, node_id, normalized_session_key, change)
            applied_changes.append({
                'round': round_index + 1,
                'change': change,
                'apply_result': apply_result,
            })
            if apply_result.get('result') not in ['ok', 'ignored']:
                return {
                    'result': 'error',
                    'message': 'apply proposed change failed',
                    'data': {
                        'before_report': before_report,
                        'applied_changes': applied_changes,
                    }
                }
        after_inspect = inspect_session_mapping_group_state_for_session(
            app_id,
            normalized_node_info,
            normalized_session_key,
        )
        current_report = _find_mapping_report_by_session_key(after_inspect, normalized_session_key)
        if not isinstance(current_report, dict):
            stop_reason = 'report_missing_after_apply'
            after_report = None
            break
        after_report = current_report
    if not isinstance(after_report, dict):
        return {
            'result': 'error',
            'message': 'session mapping inspect report not found after apply',
            'data': {
                'before_report': before_report,
                'applied_changes': applied_changes,
            }
        }
    after_html = render_inspect_session_mapping_group_state_html_for_session(
        app_id,
        node_id,
        normalized_session_key,
        inspect_result={'result': 'ok', 'data': {'mapping_reports': [after_report]}},
    )
    logging.info(
        f"migrate_inspected_session_mapping_group_state after | app_id:{app_id}, node_id:{node_id}, "
        f"session_key:{normalized_session_key}, stop_reason:{stop_reason}, report:{after_report}"
    )
    return {
        'result': 'ok',
        'data': {
            'session_key': normalized_session_key,
            'dry_run': False,
            'before_report': before_report,
            'after_report': after_report,
            'applied_changes': applied_changes,
            'stop_reason': stop_reason,
            'before_html': before_html,
            'after_html': after_html,
        }
    }


def migrate_inspected_session_mapping_group_states_for_node(app_id, node_info, dry_run=False):
    normalized_node_info = _resolve_node_info(app_id, node_info)
    node_id = str((normalized_node_info or {}).get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'error', 'message': 'bad node id'}
    inspect_result = inspect_session_mapping_group_states_for_node(app_id, normalized_node_info)
    reports = sorted(
        list(inspect_result.get('data', {}).get('mapping_reports', []) or []),
        key=lambda item: str((item or {}).get('session_key', '')).strip(),
    )
    dirty_reports = [report for report in reports if str(report.get('status', '')).strip() == 'dirty']
    results = []
    for report in dirty_reports:
        session_key = str(report.get('session_key', '')).strip()
        if session_key == '':
            continue
        results.append(migrate_inspected_session_mapping_group_state(app_id, normalized_node_info, session_key, dry_run=dry_run))
    after_inspect = inspect_session_mapping_group_states_for_node(app_id, normalized_node_info)
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'dry_run': bool(dry_run),
            'dirty_before_count': len(dirty_reports),
            'migration_results': results,
            'after_inspect': after_inspect,
        }
    }


def migrate_inspected_session_mapping_group_states_for_app(app_id, dry_run=False):
    current_app_id = str(app_id).strip()
    if current_app_id == '':
        return {'result': 'error', 'message': 'bad app id'}
    node_list_result = lanying_openclaw.get_node_list(current_app_id)
    if not isinstance(node_list_result, dict) or node_list_result.get('result') != 'ok':
        return {'result': 'error', 'message': 'get node list failed', 'data': {'app_id': current_app_id, 'node_list_result': node_list_result}}
    nodes = list(node_list_result.get('data', {}).get('list', []) or [])
    nodes = sorted(nodes, key=lambda item: str((item or {}).get('node_id', '')).strip())
    node_results = []
    dirty_before_count = 0
    for node in nodes:
        result = migrate_inspected_session_mapping_group_states_for_node(current_app_id, node, dry_run=dry_run)
        node_results.append(result)
        dirty_before_count += int((result.get('data', {}) if isinstance(result, dict) else {}).get('dirty_before_count', 0) or 0)
    return {
        'result': 'ok',
        'data': {
            'app_id': current_app_id,
            'dry_run': bool(dry_run),
            'node_count': len(nodes),
            'dirty_before_count': dirty_before_count,
            'node_results': node_results,
        }
    }


def migrate_inspected_session_mapping_group_states_for_all_apps(dry_run=False):
    app_ids = list(lanying_openclaw.list_openclaw_node_list_app_ids() or [])
    app_ids = sorted([str(app_id).strip() for app_id in app_ids if str(app_id).strip() != ''])
    app_results = []
    node_count = 0
    dirty_before_count = 0
    for app_id in app_ids:
        result = migrate_inspected_session_mapping_group_states_for_app(app_id, dry_run=dry_run)
        app_results.append(result)
        data = result.get('data', {}) if isinstance(result, dict) else {}
        node_count += int(data.get('node_count', 0) or 0)
        dirty_before_count += int(data.get('dirty_before_count', 0) or 0)
    return {
        'result': 'ok',
        'data': {
            'dry_run': bool(dry_run),
            'app_count': len(app_ids),
            'node_count': node_count,
            'dirty_before_count': dirty_before_count,
            'app_results': app_results,
        }
    }
