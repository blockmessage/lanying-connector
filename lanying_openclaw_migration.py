import html
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


def render_inspect_session_mapping_group_states_html_for_node(app_id, node_id):
    details = lanying_openclaw.list_session_mapping_details_for_node(app_id, node_id)
    header = (
        '<tr>'
        '<th>session_key</th>'
        '<th>mapping</th>'
        '<th>group_info</th>'
        '<th>member_summary</th>'
        '<th>key_user_status</th>'
        '<th>errors</th>'
        '</tr>'
    )
    rows = [header]
    for detail in details:
        mapping_summary = {
            'group_id': detail.get('group_id', ''),
            'openclaw_user_id': detail.get('openclaw_user_id', ''),
            'management_user_id': detail.get('management_user_id', ''),
            'origin_user_id': detail.get('origin_user_id', ''),
            'chatbot_user_id': detail.get('chatbot_user_id', ''),
            'parent_session_key': detail.get('parent_session_key', ''),
            'root_session_key': detail.get('root_session_key', ''),
            'effective_target_session_key': detail.get('effective_target_session_key', ''),
            'updated_at': detail.get('updated_at', ''),
        }
        group_info_html = _build_session_mapping_html_table(
            _build_session_mapping_html_key_value_rows(
                detail.get('group_info', {}),
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
        rows.append(
            '<tr>'
            f'<td valign="top">{_escape_session_mapping_html(detail.get("session_key", ""))}</td>'
            f'<td valign="top">{_build_session_mapping_html_table(_build_session_mapping_html_key_value_rows(mapping_summary))}</td>'
            f'<td valign="top">{group_info_html}</td>'
            f'<td valign="top">{member_summary_html}</td>'
            f'<td valign="top">{key_user_status_html}</td>'
            f'<td valign="top">{error_html}</td>'
            '</tr>'
        )
    return _build_session_mapping_html_table(rows)


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
    root_identity = openclaw.parse_clawchat_session_identity(normalized_detail.get('root_session_key', ''))
    root_mode = openclaw.resolve_root_session_sync_mode(root_identity)
    openclaw_user_id = str(normalized_detail.get('openclaw_user_id', '')).strip()
    management_user_id = str(normalized_detail.get('management_user_id', '')).strip()
    origin_user_id = str(normalized_detail.get('origin_user_id', '')).strip()
    chatbot_user_id = str(normalized_detail.get('chatbot_user_id', '')).strip()

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

    if openclaw.is_router_root_session(root_identity):
        expected_owner_user_id = chatbot_user_id or str(bound_chatbot_user_id).strip()
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
    else:
        expected_owner_user_id = openclaw_user_id or management_user_id

    required_members = []
    required_admins = []
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
    if management_user_id != '':
        required_admins.append(('management_user_id', management_user_id))

    key_user_status = dict(normalized_detail.get('key_user_status', {}))
    for role_key, user_id in required_members:
        status = dict(key_user_status.get(role_key, {}))
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

    for role_key, user_id in required_admins:
        status = dict(key_user_status.get(role_key, {}))
        if str(status.get('admin_status', '')).strip() == 'not_admin':
            _append_group_state_issue(
                issues,
                proposed_changes,
                'error',
                'missing_required_admin',
                f'{role_key} 不是群管理员',
                current={'role': role_key, 'user_id': user_id, 'admin_status': 'not_admin'},
                expected={'role': role_key, 'user_id': user_id, 'admin_status': 'admin'},
                proposal=_build_group_relation_change(
                    'group_admin_add',
                    group_id,
                    user_id,
                    'member',
                    'admin',
                    f'补齐 {role_key} 的群管理员关系',
                ),
            )

    if expected_owner_user_id != '' and owner_user_id != '' and owner_user_id != expected_owner_user_id:
        _append_group_state_issue(
            issues,
            proposed_changes,
            'warning',
            'unexpected_group_owner',
            '群主与当前 session mapping 推导结果不一致，需要人工审核是否转移群主',
            current=owner_user_id,
            expected=expected_owner_user_id,
            proposal={
                'action': 'group_owner_transfer_review',
                'target_type': 'group_relation',
                'group_id': group_id,
                'from': owner_user_id,
                'to': expected_owner_user_id,
                'reason': '群主变更风险较高，先人工审核，不自动执行',
                'risk': 'high',
            },
        )

    return {
        'session_key': session_key,
        'group_id': group_id,
        'root_mode': root_mode,
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
