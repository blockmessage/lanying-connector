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

7. Inspect unreasonable canonical session-mapping states and render HTML:
   render_inspect_session_mapping_canonical_html_for_node(app_id, node_id)

8. Inspect unreasonable canonical session-mapping states and get structured reports:
   inspect_session_mapping_canonical_states_for_node(app_id, {"node_id": "15"})
   inspect_session_mapping_canonical_states_for_node(app_id, full_node_info_dict)

9. Migrate one unreasonable canonical session-mapping state:
   migrate_inspected_session_mapping_canonical_state(app_id, "15", session_key, dry_run=True)
   migrate_inspected_session_mapping_canonical_state(app_id, "15", session_key, dry_run=False)

10. Migrate all unreasonable canonical session-mapping states for one node:
   migrate_inspected_session_mapping_canonical_states_for_node(app_id, "15", dry_run=True)
   migrate_inspected_session_mapping_canonical_states_for_node(app_id, "15", dry_run=False)

11. Backward-compatible origin-only aliases:
   render_inspect_session_mapping_origin_identity_html_for_node(app_id, node_id)
   inspect_session_mapping_origin_identity_for_node(app_id, {"node_id": "15"})
   migrate_inspected_session_mapping_origin_identity_state(app_id, "15", session_key, dry_run=True)
   migrate_inspected_session_mapping_origin_identity_states_for_node(app_id, "15", dry_run=False)

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


def _build_session_mapping_html_list_rows(items):
    normalized_items = list(items or [])
    if len(normalized_items) == 0:
        return ['<tr><td></td></tr>']
    rows = []
    for item in normalized_items:
        rows.append(f'<tr><td valign="top">{_escape_session_mapping_html(item)}</td></tr>')
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
            'last_message_time': detail.get('last_message_time', ''),
        }, second_keys=['created_at', 'updated_at'], millisecond_keys=['last_message_time'])
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
        'last_message_time': detail.get('last_message_time', ''),
    }, second_keys=['created_at', 'updated_at'], millisecond_keys=['last_message_time'])
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
    elif is_clawchat_session and management_user_id != '':
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
                'ClawChat session 中，management_user_id 已在群里时必须成为群管理员',
                current={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': management_status.get('admin_status', '')},
                expected={'role': 'management_user_id', 'user_id': management_user_id, 'admin_status': 'admin'},
                proposal=_build_group_relation_change(
                    'group_admin_add',
                    group_id,
                    management_user_id,
                    management_admin_status or 'unknown',
                    'admin',
                    '补齐 ClawChat session 中 management_user_id 的群管理员关系',
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


def _append_session_mapping_state_issue(issues, proposed_changes, severity, code, message, current=None, expected=None, proposal=None):
    issues.append({
        'severity': severity,
        'code': code,
        'message': message,
        'current': current,
        'expected': expected,
    })
    if isinstance(proposal, dict):
        proposed_changes.append(proposal)


def _resolve_expected_root_session_key(app_id, node_id, session_key, parent_session_key, root_session_key, session_facts):
    openclaw = lanying_openclaw
    normalized_session_key = str(session_key).strip()
    normalized_parent_session_key = str(parent_session_key).strip()
    normalized_root_session_key = str(root_session_key).strip()
    if normalized_parent_session_key != '':
        parent_mapping = openclaw.get_session_mapping_by_session(app_id, node_id, normalized_parent_session_key)
        if isinstance(parent_mapping, dict):
            inherited_root = (
                openclaw.normalize_optional_session_key(parent_mapping.get('root_session_key', '')) or
                openclaw.normalize_optional_session_key(parent_mapping.get('effective_target_session_key', '')) or
                normalized_parent_session_key
            )
            if inherited_root != '':
                return inherited_root, 'parent_mapping'
    if bool((session_facts or {}).get('is_clawchat_session')) and not bool((session_facts or {}).get('is_subagent')):
        return normalized_session_key, 'session_identity'
    if normalized_root_session_key != '':
        return normalized_root_session_key, 'existing_root'
    return '', ''


def _resolve_expected_effective_target_session_key(node_info, session_key, parent_session_key, root_session_key):
    openclaw = lanying_openclaw
    normalized_session_key = str(session_key).strip()
    normalized_parent_session_key = str(parent_session_key).strip()
    normalized_root_session_key = str(root_session_key).strip()
    if normalized_session_key == '':
        return ''
    if normalized_parent_session_key == '' and normalized_root_session_key == '':
        return ''
    return openclaw.resolve_effective_target_session_key(
        normalized_session_key,
        {
            'session_key': normalized_session_key,
            'parent_session_key': normalized_parent_session_key,
            'root_session_key': normalized_root_session_key or normalized_parent_session_key or normalized_session_key,
        },
        openclaw.is_merge_sub_sessions_enabled(node_info),
    )


def _resolve_expected_direct_origin_identity(root_identity):
    openclaw = lanying_openclaw
    if not openclaw.is_direct_session_identity(root_identity):
        return '', ''
    target_user_id = str((root_identity or {}).get('target_id', '')).strip()
    if target_user_id == '':
        return '', ''
    return 'direct_user', target_user_id


def _analyze_session_mapping_canonical_detail(
    app_id,
    node_info,
    detail,
    default_management_user_id='',
    bound_chatbot_user_id='',
):
    openclaw = lanying_openclaw
    normalized_detail = openclaw.normalize_session_mapping_record(detail)
    session_key = str(normalized_detail.get('session_key', '')).strip()
    if session_key == '':
        return {
            'session_key': '',
            'status': 'ignored',
            'issues': [],
            'proposed_changes': [],
        }
    normalized_node_info = dict(node_info or {})
    node_id = str(normalized_node_info.get('node_id', '')).strip()
    session_facts = openclaw.get_session_key_facts(session_key)
    session_identity = openclaw.parse_clawchat_session_identity(session_key)
    parent_session_key = str(normalized_detail.get('parent_session_key', '')).strip()
    current_root_session_key = str(normalized_detail.get('root_session_key', '')).strip()
    expected_root_session_key, root_source = _resolve_expected_root_session_key(
        app_id,
        node_id,
        session_key,
        parent_session_key,
        current_root_session_key,
        session_facts,
    )
    root_session_key_for_eval = expected_root_session_key or current_root_session_key or session_key
    root_identity = openclaw.parse_clawchat_session_identity(root_session_key_for_eval)
    root_mode = openclaw.resolve_root_session_sync_mode(root_identity)
    target_user_id = ''
    if openclaw.is_direct_session_identity(root_identity):
        target_user_id = str((root_identity or {}).get('target_id', '')).strip()

    issues = []
    proposed_changes = []
    expected_fields = {}
    current_fields = {
        'app_id': str(normalized_detail.get('app_id', '')).strip(),
        'node_id': str(normalized_detail.get('node_id', '')).strip(),
        'openclaw_user_id': str(normalized_detail.get('openclaw_user_id', '')).strip(),
        'management_user_id': str(normalized_detail.get('management_user_id', '')).strip(),
        'origin_kind': str(normalized_detail.get('origin_kind', '')).strip(),
        'origin_user_id': str(normalized_detail.get('origin_user_id', '')).strip(),
        'chatbot_user_id': str(normalized_detail.get('chatbot_user_id', '')).strip(),
        'group_id': str(normalized_detail.get('group_id', '')).strip(),
        'parent_session_key': parent_session_key,
        'root_session_key': current_root_session_key,
        'effective_target_session_key': str(normalized_detail.get('effective_target_session_key', '')).strip(),
    }

    def expect_field(field, target, severity, code, message, reason, risk='low', allow_empty=False):
        if target is None:
            return
        target_text = str(target).strip()
        if target_text == '' and not allow_empty:
            return
        expected_fields[field] = target_text
        current_text = str(current_fields.get(field, '')).strip()
        if current_text == target_text:
            return
        _append_session_mapping_state_issue(
            issues,
            proposed_changes,
            severity,
            code,
            message,
            current=current_text,
            expected=target_text,
            proposal=_build_mapping_field_change(
                session_key,
                field,
                current_text,
                target_text,
                reason,
                risk=risk,
            ),
        )

    expect_field('app_id', app_id, 'error', 'mapping_app_id_mismatch', 'mapping 的 app_id 与当前 app 不一致', 'session mapping 的 app_id 应与当前 app_id 一致')
    expect_field('node_id', node_id, 'error', 'mapping_node_id_mismatch', 'mapping 的 node_id 与当前节点不一致', 'session mapping 的 node_id 应与当前 node_id 一致')
    expect_field(
        'openclaw_user_id',
        str(normalized_node_info.get('user_id', '')).strip(),
        'error',
        'mapping_openclaw_user_mismatch',
        'mapping 的 openclaw_user_id 与当前节点用户不一致',
        'session mapping 的 openclaw_user_id 应与当前节点 user_id 一致',
    )
    expect_field(
        'management_user_id',
        str(default_management_user_id).strip(),
        'error',
        'mapping_management_user_mismatch',
        'mapping 的 management_user_id 与当前 app manager 不一致',
        'session mapping 的 management_user_id 应与当前 app manager user 一致',
    )

    if expected_root_session_key != '':
        expect_field(
            'root_session_key',
            expected_root_session_key,
            'error',
            'mapping_root_session_key_mismatch',
            'mapping 的 root_session_key 与当前 lineage 推导结果不一致',
            f'按当前 lineage 规则补齐 root_session_key 来源: {root_source or "session_identity"}',
        )

    effective_root_session_key = expected_root_session_key or current_root_session_key
    expected_effective_target_session_key = _resolve_expected_effective_target_session_key(
        normalized_node_info,
        session_key,
        parent_session_key,
        effective_root_session_key,
    )
    if expected_effective_target_session_key != '':
        expect_field(
            'effective_target_session_key',
            expected_effective_target_session_key,
            'error',
            'mapping_effective_target_mismatch',
            'mapping 的 effective_target_session_key 与当前规则推导结果不一致',
            '按当前 merge_sub_sessions 与 lineage 规则重建 effective_target_session_key',
        )

    if bool(session_facts.get('is_direct')):
        expect_field(
            'group_id',
            '',
            'error',
            'direct_session_group_id_should_be_empty',
            'direct clawchat session 不应绑定 group_id',
            'direct clawchat session 在当前规则下应为 metadata_only，group_id 必须为空',
            allow_empty=True,
        )
    elif bool(session_facts.get('is_group')) and not bool(session_facts.get('is_subagent')):
        expected_group_id = str((session_identity or {}).get('target_id', '')).strip()
        expect_field(
            'group_id',
            expected_group_id,
            'error',
            'group_session_group_id_mismatch',
            'group clawchat session 的 group_id 与 session_key target 不一致',
            'group clawchat session 应直接复用 session_key 里的群 ID',
        )

    if root_mode in ['router_group', 'router_direct']:
        expect_field(
            'chatbot_user_id',
            str(bound_chatbot_user_id).strip(),
            'error',
            'router_session_chatbot_user_mismatch',
            'router session 的 chatbot_user_id 与当前绑定 chatbot user 不一致',
            'router session 应绑定当前节点关联的 chatbot_user_id',
        )

    expected_origin_kind, expected_origin_user_id = _resolve_expected_direct_origin_identity(root_identity)
    if expected_origin_kind != '' and expected_origin_user_id != '':
        expect_field(
            'origin_kind',
            expected_origin_kind,
            'error',
            'direct_root_origin_kind_mismatch',
            'direct root lineage 的 origin_kind 与当前规则不一致',
            'direct root lineage 的 origin_kind 应固定为 direct_user',
        )
        expect_field(
            'origin_user_id',
            expected_origin_user_id,
            'error',
            'direct_root_origin_user_mismatch',
            'direct root lineage 的 origin_user_id 与当前规则不一致',
            'direct root lineage 的 origin_user_id 应等于 direct target user',
        )

    return {
        'session_key': session_key,
        'session_facts': session_facts,
        'root_mode': root_mode,
        'target_user_id': target_user_id,
        'current_fields': current_fields,
        'expected_fields': expected_fields,
        'status': 'dirty' if len(issues) > 0 else 'clean',
        'issues': issues,
        'proposed_changes': proposed_changes,
    }


def inspect_session_mapping_canonical_states_for_node(app_id, node_info):
    openclaw = lanying_openclaw
    normalized_node_info = _resolve_node_info(app_id, node_info)
    node_id = str((normalized_node_info or {}).get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    default_management_user_id = ''
    app_user_result = openclaw.ensure_openclaw_app_manager_user(app_id)
    if isinstance(app_user_result, dict) and app_user_result.get('result') == 'ok':
        default_management_user_id = str(app_user_result.get('data', {}).get('user_id', '')).strip()
    bound_chatbot_user_id = openclaw.resolve_bound_chatbot_user_id(app_id, node_id)
    mappings = openclaw.list_session_mappings_for_node(app_id, node_id)
    reports = []
    dirty_count = 0
    for mapping in mappings:
        report = _analyze_session_mapping_canonical_detail(
            app_id,
            normalized_node_info,
            mapping,
            default_management_user_id=default_management_user_id,
            bound_chatbot_user_id=bound_chatbot_user_id,
        )
        if str(report.get('status', '')).strip() == 'ignored':
            continue
        if str(report.get('status', '')).strip() == 'dirty':
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


def render_inspect_session_mapping_canonical_html_for_node(app_id, node_id):
    openclaw = lanying_openclaw
    mappings = sorted(
        openclaw.list_session_mappings_for_node(app_id, node_id),
        key=lambda item: str((item or {}).get('session_key', '')).strip(),
    )
    inspect_result = inspect_session_mapping_canonical_states_for_node(app_id, {'node_id': str(node_id).strip()})
    reports_by_session_key = {}
    if isinstance(inspect_result, dict):
        for report in list(inspect_result.get('data', {}).get('mapping_reports', []) or []):
            session_key = str(report.get('session_key', '')).strip()
            if session_key != '':
                reports_by_session_key[session_key] = report
    header = (
        '<tr>'
        '<th>session_key</th>'
        '<th>mapping</th>'
        '<th>session_facts</th>'
        '<th>origin_identity</th>'
        '<th>expected_fields</th>'
        '<th>status</th>'
        '<th>issues</th>'
        '<th>proposed_changes</th>'
        '</tr>'
    )
    rows = [header]
    for mapping in mappings:
        normalized_mapping = openclaw.normalize_session_mapping_record(mapping)
        session_key = str((normalized_mapping or {}).get('session_key', '')).strip()
        report = reports_by_session_key.get(session_key)
        if not isinstance(report, dict):
            continue
        session_facts = openclaw.get_session_key_facts(session_key)
        mapping_summary = _annotate_time_fields({
            'app_id': normalized_mapping.get('app_id', ''),
            'node_id': normalized_mapping.get('node_id', ''),
            'openclaw_user_id': normalized_mapping.get('openclaw_user_id', ''),
            'management_user_id': normalized_mapping.get('management_user_id', ''),
            'origin_kind': normalized_mapping.get('origin_kind', ''),
            'origin_user_id': normalized_mapping.get('origin_user_id', ''),
            'chatbot_user_id': normalized_mapping.get('chatbot_user_id', ''),
            'group_id': normalized_mapping.get('group_id', ''),
            'parent_session_key': normalized_mapping.get('parent_session_key', ''),
            'root_session_key': normalized_mapping.get('root_session_key', ''),
            'effective_target_session_key': normalized_mapping.get('effective_target_session_key', ''),
            'updated_at': normalized_mapping.get('updated_at', ''),
            'created_at': normalized_mapping.get('created_at', ''),
            'last_message_time': openclaw.get_session_last_message_time(
                app_id,
                normalized_mapping.get('node_id', ''),
                session_key,
            ),
        }, second_keys=['created_at', 'updated_at'], millisecond_keys=['last_message_time'])
        expected_summary = dict(report.get('expected_fields', {}))
        expected_summary['root_mode'] = report.get('root_mode', '')
        expected_summary['target_user_id'] = report.get('target_user_id', '')
        origin_issue_codes = set()
        for issue in list(report.get('issues', []) or []):
            origin_issue_codes.add(str((issue or {}).get('code', '')).strip())
        origin_expected_kind = str(expected_summary.get('origin_kind', '')).strip()
        origin_expected_user_id = str(expected_summary.get('origin_user_id', '')).strip()
        origin_identity_summary = {
            'current_origin_kind': normalized_mapping.get('origin_kind', ''),
            'current_origin_user_id': normalized_mapping.get('origin_user_id', ''),
            'expected_origin_kind': origin_expected_kind,
            'expected_origin_user_id': origin_expected_user_id,
            'origin_repair_reason': (
                'direct root lineage inferred from root_session_key'
                if (
                    'direct_root_origin_kind_mismatch' in origin_issue_codes or
                    'direct_root_origin_user_mismatch' in origin_issue_codes
                )
                else ''
            ),
        }
        issues_html = _build_session_mapping_html_table(_build_session_mapping_html_list_rows([
            f"{str(issue.get('severity', '')).strip()}: {str(issue.get('code', '')).strip()} - {str(issue.get('message', '')).strip()}"
            for issue in list(report.get('issues', []) or [])
        ]))
        change_rows = []
        for change in list(report.get('proposed_changes', []) or []):
            change_rows.append(
                '<tr><td>'
                + _build_session_mapping_html_table(
                    _build_session_mapping_html_key_value_rows(
                        change,
                        ['action', 'field', 'from', 'to', 'reason', 'risk'],
                    )
                )
                + '</td></tr>'
            )
        proposed_changes_html = _build_session_mapping_html_table(change_rows or ['<tr><td></td></tr>'])
        rows.append(
            '<tr>'
            f'<td valign="top">{_escape_session_mapping_html(session_key)}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(mapping_summary))}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(session_facts, ["canonical_session_key", "channel", "chat_type", "target_id", "is_router", "is_direct", "is_subagent"]))}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(origin_identity_summary))}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(expected_summary))}</td>'
            f'<td valign="top">{_escape_session_mapping_html(report.get("status", ""))}</td>'
            f'<td valign="top">{issues_html}</td>'
            f'<td valign="top">{proposed_changes_html}</td>'
            '</tr>'
        )
    return _build_session_mapping_html_table(rows)


def _find_canonical_mapping_report_by_session_key(inspect_result, session_key):
    normalized_session_key = str(session_key).strip()
    if not isinstance(inspect_result, dict):
        return None
    for report in list(inspect_result.get('data', {}).get('mapping_reports', []) or []):
        if str(report.get('session_key', '')).strip() == normalized_session_key:
            return report
    return None


def _predict_canonical_mapping_report_after_changes(report):
    predicted = dict(report or {})
    predicted_current_fields = dict(predicted.get('current_fields', {}))
    for change in list((report or {}).get('proposed_changes', []) or []):
        if str((change or {}).get('action', '')).strip() != 'mapping_field_update':
            continue
        field = str((change or {}).get('field', '')).strip()
        if field == '':
            continue
        predicted_current_fields[field] = str(change.get('to', '')).strip()
    predicted['current_fields'] = predicted_current_fields
    predicted['issues'] = []
    predicted['status'] = 'clean'
    predicted['proposed_changes'] = []
    return predicted


def _canonical_mapping_change_signature(report):
    signature = []
    for change in list((report or {}).get('proposed_changes', []) or []):
        if not isinstance(change, dict):
            continue
        signature.append((
            str(change.get('action', '')).strip(),
            str(change.get('field', '')).strip(),
            str(change.get('from', '')).strip(),
            str(change.get('to', '')).strip(),
            str(change.get('session_key', '')).strip(),
        ))
    return tuple(signature)


def migrate_inspected_session_mapping_canonical_state(app_id, node_info, session_key, dry_run=False):
    openclaw = lanying_openclaw
    normalized_node_info = _resolve_node_info(app_id, node_info)
    node_id = str((normalized_node_info or {}).get('node_id', '')).strip()
    normalized_session_key = str(session_key).strip()
    if node_id == '' or normalized_session_key == '':
        return {'result': 'error', 'message': 'bad node_id or session_key'}
    before_inspect = inspect_session_mapping_canonical_states_for_node(app_id, normalized_node_info)
    before_report = _find_canonical_mapping_report_by_session_key(before_inspect, normalized_session_key)
    if not isinstance(before_report, dict):
        return {'result': 'error', 'message': 'canonical session mapping inspect report not found'}
    before_html = render_inspect_session_mapping_canonical_html_for_node(app_id, node_id)
    if dry_run:
        return {
            'result': 'ok',
            'data': {
                'session_key': normalized_session_key,
                'dry_run': True,
                'before_report': before_report,
                'after_report': _predict_canonical_mapping_report_after_changes(before_report),
                'applied_changes': [],
                'before_html': before_html,
                'after_html': before_html,
            }
        }
    mapping = openclaw.get_session_mapping_by_session(app_id, node_id, normalized_session_key)
    if not isinstance(mapping, dict):
        return {'result': 'error', 'message': 'session mapping not found'}
    for change in list(before_report.get('proposed_changes', []) or []):
        if str(change.get('action', '')).strip() != 'mapping_field_update':
            return {'result': 'error', 'message': f'unsupported canonical mapping change: {change}'}
        field = str(change.get('field', '')).strip()
        mapping[field] = change.get('to', '')
    set_result = openclaw.rewrite_session_mapping_for_migration(
        app_id,
        node_id,
        mapping,
        change_source='canonical_session_mapping_migration',
    )
    if set_result.get('result') != 'ok':
        return {'result': 'error', 'message': 'set session mapping failed', 'data': {'set_result': set_result}}
    after_inspect = inspect_session_mapping_canonical_states_for_node(app_id, normalized_node_info)
    after_report = _find_canonical_mapping_report_by_session_key(after_inspect, normalized_session_key)
    after_html = render_inspect_session_mapping_canonical_html_for_node(app_id, node_id)
    return {
        'result': 'ok',
        'data': {
            'session_key': normalized_session_key,
            'dry_run': False,
            'before_report': before_report,
            'after_report': after_report,
            'applied_changes': list(before_report.get('proposed_changes', []) or []),
            'before_html': before_html,
            'after_html': after_html,
        }
    }


def migrate_inspected_session_mapping_canonical_states_for_node(app_id, node_info, dry_run=False):
    openclaw = lanying_openclaw
    normalized_node_info = _resolve_node_info(app_id, node_info)
    node_id = str((normalized_node_info or {}).get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'error', 'message': 'bad node id'}
    max_rounds = 5
    results_by_session_key = {}
    seen_signatures_by_session_key = {}
    inspect_result = inspect_session_mapping_canonical_states_for_node(app_id, normalized_node_info)
    initial_dirty_count = 0
    stop_reason = 'clean'
    rounds_run = 0
    after_inspect = inspect_result

    for round_number in range(1, max_rounds + 1):
        rounds_run = round_number
        reports = sorted(
            list(after_inspect.get('data', {}).get('mapping_reports', []) or []),
            key=lambda item: str((item or {}).get('session_key', '')).strip(),
        )
        dirty_reports = [report for report in reports if str(report.get('status', '')).strip() == 'dirty']
        if round_number == 1:
            initial_dirty_count = len(dirty_reports)
        if len(dirty_reports) == 0:
            stop_reason = 'clean'
            break
        if dry_run:
            for report in dirty_reports:
                session_key = str(report.get('session_key', '')).strip()
                if session_key == '':
                    continue
                if session_key not in results_by_session_key:
                    results_by_session_key[session_key] = {
                        'result': 'ok',
                        'data': {
                            'session_key': session_key,
                            'dry_run': True,
                            'before_report': report,
                            'after_report': _predict_canonical_mapping_report_after_changes(report),
                            'applied_changes': [],
                            'before_html': '',
                            'after_html': '',
                        }
                    }
            stop_reason = 'dry_run'
            break

        any_progress = False
        repeated_signature_detected = False
        round_had_error = False
        for report in dirty_reports:
            session_key = str(report.get('session_key', '')).strip()
            if session_key == '':
                continue
            if session_key not in results_by_session_key:
                results_by_session_key[session_key] = {
                    'result': 'ok',
                    'data': {
                        'session_key': session_key,
                        'dry_run': False,
                        'before_report': report,
                        'after_report': None,
                        'applied_changes': [],
                        'before_html': '',
                        'after_html': '',
                    }
                }
            signature = _canonical_mapping_change_signature(report)
            previous_signatures = seen_signatures_by_session_key.setdefault(session_key, set())
            if signature in previous_signatures:
                repeated_signature_detected = True
                continue
            previous_signatures.add(signature)

            mapping = openclaw.get_session_mapping_by_session(app_id, node_id, session_key)
            if not isinstance(mapping, dict):
                results_by_session_key[session_key] = {
                    'result': 'error',
                    'message': 'session mapping not found',
                    'data': {
                        'session_key': session_key,
                    }
                }
                round_had_error = True
                continue
            unsupported_change = None
            for change in list(report.get('proposed_changes', []) or []):
                if str(change.get('action', '')).strip() != 'mapping_field_update':
                    unsupported_change = change
                    break
                field = str(change.get('field', '')).strip()
                mapping[field] = change.get('to', '')
            if unsupported_change is not None:
                results_by_session_key[session_key] = {
                    'result': 'error',
                    'message': f'unsupported canonical mapping change: {unsupported_change}',
                    'data': {
                        'session_key': session_key,
                    }
                }
                round_had_error = True
                continue
            set_result = openclaw.rewrite_session_mapping_for_migration(
                app_id,
                node_id,
                mapping,
                change_source='canonical_session_mapping_migration',
            )
            if set_result.get('result') != 'ok':
                results_by_session_key[session_key] = {
                    'result': 'error',
                    'message': 'set session mapping failed',
                    'data': {
                        'session_key': session_key,
                        'set_result': set_result,
                    }
                }
                round_had_error = True
                continue
            any_progress = True
            results_by_session_key[session_key]['data']['applied_changes'].extend([
                dict(change, round=round_number)
                for change in list(report.get('proposed_changes', []) or [])
            ])

        after_inspect = inspect_session_mapping_canonical_states_for_node(app_id, normalized_node_info)
        if round_had_error:
            stop_reason = 'error'
            break
        if repeated_signature_detected and not any_progress:
            stop_reason = 'repeated_proposed_changes'
            break
        if not any_progress:
            stop_reason = 'no_progress'
            break
    else:
        stop_reason = 'max_rounds_reached'

    after_reports_by_session_key = {}
    for report in list(after_inspect.get('data', {}).get('mapping_reports', []) or []):
        session_key = str((report or {}).get('session_key', '')).strip()
        if session_key != '':
            after_reports_by_session_key[session_key] = report
    results = []
    for session_key in sorted(results_by_session_key.keys()):
        result = results_by_session_key[session_key]
        if result.get('result') != 'ok':
            results.append(result)
            continue
        result['data']['after_report'] = after_reports_by_session_key.get(session_key)
        results.append(result)
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'dry_run': bool(dry_run),
            'dirty_before_count': initial_dirty_count,
            'migration_results': results,
            'after_inspect': after_inspect,
            'rounds_run': rounds_run,
            'stop_reason': stop_reason,
        }
    }


def inspect_session_mapping_origin_identity_for_node(app_id, node_info):
    return inspect_session_mapping_canonical_states_for_node(app_id, node_info)


def render_inspect_session_mapping_origin_identity_html_for_node(app_id, node_id):
    return render_inspect_session_mapping_canonical_html_for_node(app_id, node_id)


def _find_origin_identity_report_by_session_key(inspect_result, session_key):
    return _find_canonical_mapping_report_by_session_key(inspect_result, session_key)


def _predict_origin_identity_report_after_changes(report):
    return _predict_canonical_mapping_report_after_changes(report)


def migrate_inspected_session_mapping_origin_identity_state(app_id, node_info, session_key, dry_run=False):
    return migrate_inspected_session_mapping_canonical_state(app_id, node_info, session_key, dry_run=dry_run)


def migrate_inspected_session_mapping_origin_identity_states_for_node(app_id, node_info, dry_run=False):
    return migrate_inspected_session_mapping_canonical_states_for_node(app_id, node_info, dry_run=dry_run)


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
        return openclaw.rewrite_session_mapping_for_migration(
            app_id,
            node_id,
            mapping,
            change_source='group_state_session_mapping_migration',
        )
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
