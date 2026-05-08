import lanying_chatbot
import lanying_redis
import lanying_openclaw
import logging
import json
import lanying_ai_capsule
from lanying_grow_ai import GitBookSummary
import os
import re
import shutil
import requests

def info(format):
    print(format)
    logging.info(format)

def migrate_legacy_session_mappings_for_node(app_id, node_info, dry_run=False):
    openclaw = lanying_openclaw
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    redis = lanying_redis.get_redis_connection()
    index_key = openclaw.get_openclaw_session_mapping_index_key(app_id, node_id)
    try:
        indexed_session_keys = list(redis.smembers(index_key))
    except Exception:
        logging.exception("migrate_legacy_session_mappings_for_node read index failed")
        return {'result': 'error', 'message': 'read index failed'}
    total = 0
    migrated = 0
    conflicts = 0
    skipped = 0
    logging.info(
        f"migrate_legacy_session_mappings_for_node start | app_id:{app_id}, node_id:{node_id}, "
        f"indexed_count:{len(indexed_session_keys)}, dry_run:{dry_run}"
    )
    for indexed_session_key in indexed_session_keys:
        raw_session_key = openclaw.normalize_session_key_text(indexed_session_key)
        facts = openclaw.get_session_key_facts(raw_session_key)
        if raw_session_key == '' or not facts.get('is_legacy_alias'):
            continue
        total += 1
        raw = lanying_redis.redis_get(
            redis,
            openclaw.get_openclaw_session_mapping_by_session_storage_key(app_id, node_id, raw_session_key),
        )
        if not raw:
            skipped += 1
            logging.info(
                f"migrate_legacy_session_mappings_for_node skip missing record | app_id:{app_id}, "
                f"node_id:{node_id}, legacy_session_key:{raw_session_key}"
            )
            continue
        try:
            legacy_mapping = openclaw.normalize_session_mapping_record(json.loads(raw))
        except Exception:
            logging.exception("migrate_legacy_session_mappings_for_node parse failed")
            skipped += 1
            continue
        canonical_session_key = openclaw.normalize_optional_session_key(legacy_mapping.get('session_key', ''))
        if canonical_session_key == '':
            skipped += 1
            logging.info(
                f"migrate_legacy_session_mappings_for_node skip empty canonical session | app_id:{app_id}, "
                f"node_id:{node_id}, legacy_session_key:{raw_session_key}"
            )
            continue
        existing_canonical_mapping = None
        existing_canonical_raw = lanying_redis.redis_get(
            redis,
            openclaw.get_openclaw_session_mapping_by_session_key(app_id, node_id, canonical_session_key),
        )
        if existing_canonical_raw:
            try:
                existing_canonical_mapping = openclaw.normalize_session_mapping_record(json.loads(existing_canonical_raw))
            except Exception:
                logging.exception("migrate_legacy_session_mappings_for_node canonical parse failed")
                existing_canonical_mapping = None
        if (
            isinstance(existing_canonical_mapping, dict) and
            openclaw.session_mapping_conflicts(existing_canonical_mapping, legacy_mapping)
        ):
            conflicts += 1
            logging.warning(
                f"migrate_legacy_session_mappings_for_node conflict | app_id:{app_id}, node_id:{node_id}, "
                f"legacy_session_key:{raw_session_key}, canonical_session_key:{canonical_session_key}, "
                f"legacy_signature:{openclaw.session_mapping_signature(legacy_mapping)}, "
                f"canonical_signature:{openclaw.session_mapping_signature(existing_canonical_mapping)}"
            )
            continue
        logging.info(
            f"migrate_legacy_session_mappings_for_node {'dry_run ' if dry_run else ''}migrate | "
            f"app_id:{app_id}, node_id:{node_id}, legacy_session_key:{raw_session_key}, "
            f"canonical_session_key:{canonical_session_key}"
        )
        if not dry_run:
            openclaw.converge_session_mapping_record(
                redis,
                app_id,
                node_id,
                legacy_mapping,
                legacy_session_keys=[raw_session_key],
            )
        migrated += 1
    return {
        'result': 'ok',
        'data': {
            'total': total,
            'migrated': migrated,
            'conflicts': conflicts,
            'skipped': skipped,
            'dry_run': dry_run,
        }
    }

def migrate_session_mapping_group_admins_for_node(app_id, node_info, dry_run=False):
    openclaw = lanying_openclaw
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    if not openclaw.is_session_map_sync_enabled(node_info):
        return {'result': 'ignored', 'message': 'session_map_sync disabled'}
    app_user_result = openclaw.ensure_openclaw_app_manager_user(app_id)
    if app_user_result.get('result') != 'ok':
        return {'result': 'error', 'message': 'openclaw app manager user unavailable'}
    management_user_id = str(app_user_result.get('data', {}).get('user_id', '')).strip()
    if management_user_id == '':
        return {'result': 'error', 'message': 'bad management user id'}

    migrated_groups = set()
    total = 0
    success = 0
    mappings = openclaw.list_session_mappings_for_node(app_id, node_id)
    logging.info(
        f"migrate_session_mapping_group_admins_for_node start | app_id:{app_id}, node_id:{node_id}, "
        f"management_user_id:{management_user_id}, mapping_count:{len(mappings)}, dry_run:{dry_run}"
    )
    for mapping in mappings:
        group_id = str(mapping.get('group_id', '')).strip()
        if group_id == '':
            logging.info(
                f"migrate_session_mapping_group_admins_for_node skip mapping without group | "
                f"app_id:{app_id}, node_id:{node_id}, session_key:{str(mapping.get('session_key', '')).strip()}"
            )
            continue
        if group_id in migrated_groups:
            logging.info(
                f"migrate_session_mapping_group_admins_for_node skip duplicated group | "
                f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}"
            )
            continue
        migrated_groups.add(group_id)
        total += 1
        if dry_run:
            logging.info(
                f"migrate_session_mapping_group_admins_for_node dry_run candidate | "
                f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}, management_user_id:{management_user_id}"
            )
            success += 1
            continue
        repair_ok = openclaw.ensure_user_group_admin_sync(app_id, management_user_id, group_id)
        logging.info(
            f"migrate_session_mapping_group_admins_for_node repair result | "
            f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}, "
            f"management_user_id:{management_user_id}, success:{repair_ok}"
        )
        if repair_ok:
            success += 1
    logging.info(
        f"migrate_session_mapping_group_admins_for_node | app_id:{app_id}, node_id:{node_id}, "
        f"management_user_id:{management_user_id}, total_groups:{total}, success_groups:{success}, dry_run:{dry_run}"
    )
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'management_user_id': management_user_id,
            'total_groups': total,
            'success_groups': success,
            'dry_run': dry_run
        }
    }

def migrate_session_mapping_management_users_for_node(app_id, node_info, dry_run=False):
    openclaw = lanying_openclaw
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    if not openclaw.is_session_map_sync_enabled(node_info):
        return {'result': 'ignored', 'message': 'session_map_sync disabled'}

    migrated_pairs = set()
    total = 0
    success = 0
    mappings = openclaw.list_session_mappings_for_node(app_id, node_id)
    logging.info(
        f"migrate_session_mapping_management_users_for_node start | app_id:{app_id}, node_id:{node_id}, "
        f"mapping_count:{len(mappings)}, dry_run:{dry_run}"
    )
    for mapping in mappings:
        group_id = str(mapping.get('group_id', '')).strip()
        management_user_id = str(mapping.get('management_user_id', '')).strip()
        if group_id == '' or management_user_id == '':
            logging.info(
                f"migrate_session_mapping_management_users_for_node skip incomplete mapping | "
                f"app_id:{app_id}, node_id:{node_id}, session_key:{str(mapping.get('session_key', '')).strip()}, "
                f"group_id:{group_id}, management_user_id:{management_user_id}"
            )
            continue
        migrate_key = f"{group_id}:{management_user_id}"
        if migrate_key in migrated_pairs:
            logging.info(
                f"migrate_session_mapping_management_users_for_node skip duplicated pair | "
                f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}, management_user_id:{management_user_id}"
            )
            continue
        migrated_pairs.add(migrate_key)
        total += 1
        if dry_run:
            logging.info(
                f"migrate_session_mapping_management_users_for_node dry_run candidate | "
                f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}, management_user_id:{management_user_id}"
            )
            success += 1
            continue
        repair_ok = openclaw.ensure_user_group_admin_sync(app_id, management_user_id, group_id)
        logging.info(
            f"migrate_session_mapping_management_users_for_node repair result | "
            f"app_id:{app_id}, node_id:{node_id}, group_id:{group_id}, "
            f"management_user_id:{management_user_id}, success:{repair_ok}"
        )
        if repair_ok:
            success += 1
    logging.info(
        f"migrate_session_mapping_management_users_for_node | app_id:{app_id}, node_id:{node_id}, "
        f"total_pairs:{total}, success_pairs:{success}, dry_run:{dry_run}"
    )
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'total_groups': total,
            'success_groups': success,
            'dry_run': dry_run
        }
    }

def list_openclaw_node_list_app_ids():
    redis = lanying_redis.get_redis_connection()
    app_ids = set()
    prefix = "lanying_connector:openclaw:node_list:"
    try:
        for raw_key in redis.scan_iter(match=f"{prefix}*", count=100):
            key = raw_key.decode('utf-8') if isinstance(raw_key, bytes) else str(raw_key)
            app_id = key[len(prefix):].strip() if key.startswith(prefix) else ''
            if app_id != '':
                app_ids.add(app_id)
    except Exception:
        logging.exception("list_openclaw_node_list_app_ids scan failed")
        return []
    resolved_app_ids = sorted(app_ids)
    logging.info(
        f"list_openclaw_node_list_app_ids | app_count:{len(resolved_app_ids)}, app_ids:{resolved_app_ids}"
    )
    return resolved_app_ids

def migrate_session_mapping_group_admins(app_id, node_id='', dry_run=False):
    openclaw = lanying_openclaw
    app_id_text = str(app_id).strip()
    node_id_text = str(node_id).strip()
    app_ids = [app_id_text] if app_id_text != '' else list_openclaw_node_list_app_ids()
    if len(app_ids) == 0:
        return {'result': 'error', 'message': 'bad app id'}
    logging.info(
        f"migrate_session_mapping_group_admins start | app_id:{app_id_text}, node_id:{node_id_text}, "
        f"dry_run:{dry_run}, app_ids:{app_ids}"
    )

    app_results = []
    total_node_count = 0
    total_groups = 0
    success_groups = 0
    for current_app_id in app_ids:
        logging.info(
            f"migrate_session_mapping_group_admins load node list | app_id:{current_app_id}, "
            f"node_id_filter:{node_id_text}, dry_run:{dry_run}"
        )
        node_list_result = openclaw.get_node_list(current_app_id)
        if node_list_result.get('result') != 'ok':
            logging.info(
                f"migrate_session_mapping_group_admins node list failed | app_id:{current_app_id}, "
                f"node_id_filter:{node_id_text}, result:{node_list_result}"
            )
            app_result = {
                'result': 'error',
                'message': 'get node list failed',
                'data': {
                    'app_id': current_app_id,
                    'node_id': node_id_text,
                    'dry_run': bool(dry_run),
                    'node_count': 0,
                    'total_groups': 0,
                    'success_groups': 0,
                    'node_results': [],
                    'node_list_result': node_list_result,
                }
            }
            app_results.append(app_result)
            continue
        nodes = node_list_result.get('data', {}).get('list', [])
        if node_id_text != '':
            nodes = [node for node in nodes if str(node.get('node_id', '')).strip() == node_id_text]
        logging.info(
            f"migrate_session_mapping_group_admins process app nodes | app_id:{current_app_id}, "
            f"node_count:{len(nodes)}, node_id_filter:{node_id_text}"
        )

        node_results = []
        app_total_groups = 0
        app_success_groups = 0
        for node in nodes:
            result = migrate_session_mapping_group_admins_for_node(current_app_id, node, dry_run=dry_run)
            node_results.append(result)
            data = result.get('data', {}) if isinstance(result, dict) else {}
            app_total_groups += int(data.get('total_groups', 0) or 0)
            app_success_groups += int(data.get('success_groups', 0) or 0)

        app_result = {
            'result': 'ok',
            'data': {
                'app_id': current_app_id,
                'node_id': node_id_text,
                'dry_run': bool(dry_run),
                'node_count': len(nodes),
                'total_groups': app_total_groups,
                'success_groups': app_success_groups,
                'node_results': node_results
            }
        }
        logging.info(
            f"migrate_session_mapping_group_admins app summary | app_id:{current_app_id}, "
            f"node_count:{len(nodes)}, total_groups:{app_total_groups}, "
            f"success_groups:{app_success_groups}, dry_run:{dry_run}"
        )
        app_results.append(app_result)
        total_node_count += len(nodes)
        total_groups += app_total_groups
        success_groups += app_success_groups

    if app_id_text != '':
        logging.info(
            f"migrate_session_mapping_group_admins done single app | app_id:{app_id_text}, node_id:{node_id_text}, "
            f"total_groups:{total_groups}, success_groups:{success_groups}, dry_run:{dry_run}"
        )
        return app_results[0]
    logging.info(
        f"migrate_session_mapping_group_admins done all apps | app_count:{len(app_ids)}, node_count:{total_node_count}, "
        f"total_groups:{total_groups}, success_groups:{success_groups}, dry_run:{dry_run}"
    )
    return {
        'result': 'ok',
        'data': {
            'app_id': '',
            'node_id': node_id_text,
            'dry_run': bool(dry_run),
            'app_count': len(app_ids),
            'node_count': total_node_count,
            'total_groups': total_groups,
            'success_groups': success_groups,
            'app_results': app_results
        }
    }

def migrate_session_mapping_management_users(app_id, node_id='', dry_run=False):
    openclaw = lanying_openclaw
    app_id_text = str(app_id).strip()
    node_id_text = str(node_id).strip()
    app_ids = [app_id_text] if app_id_text != '' else list_openclaw_node_list_app_ids()
    if len(app_ids) == 0:
        return {'result': 'error', 'message': 'bad app id'}
    logging.info(
        f"migrate_session_mapping_management_users start | app_id:{app_id_text}, node_id:{node_id_text}, "
        f"dry_run:{dry_run}, app_ids:{app_ids}"
    )

    app_results = []
    total_node_count = 0
    total_groups = 0
    success_groups = 0
    for current_app_id in app_ids:
        logging.info(
            f"migrate_session_mapping_management_users load node list | app_id:{current_app_id}, "
            f"node_id_filter:{node_id_text}, dry_run:{dry_run}"
        )
        node_list_result = openclaw.get_node_list(current_app_id)
        if node_list_result.get('result') != 'ok':
            logging.info(
                f"migrate_session_mapping_management_users node list failed | app_id:{current_app_id}, "
                f"node_id_filter:{node_id_text}, result:{node_list_result}"
            )
            app_result = {
                'result': 'error',
                'message': 'get node list failed',
                'data': {
                    'app_id': current_app_id,
                    'node_id': node_id_text,
                    'dry_run': bool(dry_run),
                    'node_count': 0,
                    'total_groups': 0,
                    'success_groups': 0,
                    'node_results': [],
                    'node_list_result': node_list_result,
                }
            }
            app_results.append(app_result)
            continue
        nodes = node_list_result.get('data', {}).get('list', [])
        if node_id_text != '':
            nodes = [node for node in nodes if str(node.get('node_id', '')).strip() == node_id_text]
        logging.info(
            f"migrate_session_mapping_management_users process app nodes | app_id:{current_app_id}, "
            f"node_count:{len(nodes)}, node_id_filter:{node_id_text}"
        )

        node_results = []
        app_total_groups = 0
        app_success_groups = 0
        for node in nodes:
            result = migrate_session_mapping_management_users_for_node(current_app_id, node, dry_run=dry_run)
            node_results.append(result)
            data = result.get('data', {}) if isinstance(result, dict) else {}
            app_total_groups += int(data.get('total_groups', 0) or 0)
            app_success_groups += int(data.get('success_groups', 0) or 0)

        app_result = {
            'result': 'ok',
            'data': {
                'app_id': current_app_id,
                'node_id': node_id_text,
                'dry_run': bool(dry_run),
                'node_count': len(nodes),
                'total_groups': app_total_groups,
                'success_groups': app_success_groups,
                'node_results': node_results
            }
        }
        logging.info(
            f"migrate_session_mapping_management_users app summary | app_id:{current_app_id}, "
            f"node_count:{len(nodes)}, total_groups:{app_total_groups}, "
            f"success_groups:{app_success_groups}, dry_run:{dry_run}"
        )
        app_results.append(app_result)
        total_node_count += len(nodes)
        total_groups += app_total_groups
        success_groups += app_success_groups

    if app_id_text != '':
        logging.info(
            f"migrate_session_mapping_management_users done single app | app_id:{app_id_text}, node_id:{node_id_text}, "
            f"total_groups:{total_groups}, success_groups:{success_groups}, dry_run:{dry_run}"
        )
        return app_results[0]
    logging.info(
        f"migrate_session_mapping_management_users done all apps | app_count:{len(app_ids)}, node_count:{total_node_count}, "
        f"total_groups:{total_groups}, success_groups:{success_groups}, dry_run:{dry_run}"
    )
    return {
        'result': 'ok',
        'data': {
            'app_id': '',
            'node_id': node_id_text,
            'dry_run': bool(dry_run),
            'app_count': len(app_ids),
            'node_count': total_node_count,
            'total_groups': total_groups,
            'success_groups': success_groups,
            'app_results': app_results
        }
    }

def transform_chatbot_preset(dry_run):
    redis = lanying_redis.get_redis_connection()
    keys = lanying_redis.redis_keys(redis, 'lanying_connector:chatbot:*')
    for key in keys:
        fields = key.split(':')
        if len(fields) == 4:
            app_id = fields[2]
            chatbot_id = fields[3]
            transform_chatbot_preset_one(app_id, chatbot_id, dry_run)

def transform_chatbot_preset_one(app_id, chatbot_id, dry_run):
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot:
        preset = chatbot.get('preset')
        if preset:
            info(f"start check app_id:{app_id}, chatbot_id:{chatbot_id}")
            old_preset_str = json.dumps(preset, ensure_ascii=False)
            changed = False
            if 'stream' not in preset:
                info(f"transform stream to true")
                preset['stream'] = True
                changed = True
            else:
                if preset['stream'] == False:
                    info(f"app_id:{app_id}, chatbot_id:{chatbot_id} stream is False")
            if 'ext' not in preset:
                info(f"Add default ext")
                preset['ext'] = {
                    'debug': False,
                    'stream_interval': 3
                }
                changed = True
            else:
                ext = preset.get('ext')
                if 'stream_interval' not in ext:
                    info(f"add stream_interval")
                    ext['stream_interval'] = 3
                    preset['ext'] = ext
                    changed = True
            if changed:
                preset_str = json.dumps(preset, ensure_ascii=False)
                if dry_run:
                    info(f"finish transform with dry run: app_id:{app_id}, chatbot_id:{chatbot_id}, old_preset_str:{old_preset_str}, preset_str:{preset_str}")
                else:
                    lanying_chatbot.set_chatbot_field(app_id, chatbot_id, "preset", preset_str)
                    info(f"finish transform: app_id:{app_id}, chatbot_id:{chatbot_id}, old_preset_str:{old_preset_str}, preset_str:{preset_str}")

def transform_capsule_income_app_ids():
    redis = lanying_redis.get_redis_connection()
    keys = lanying_redis.redis_keys(redis, 'lanying:connector:statistics:capsule:everymonth:v2:*')
    for key in keys:
        fields = key.split(':')
        if len(fields) == 8:
            app_id = fields[6]
            date_month = fields[7]
            app_ids_key = f"lanying:connector:statistics:capsule_app_ids:everymonth:{date_month}"
            info(f"add app_id{app_id}, to {date_month}")
            redis.hincrby(app_ids_key, app_id, 0)

def transform_summary_remove_date_directory(base_dir):
    summary_file = os.path.join(base_dir, 'SUMMARY.md')
    with open(summary_file, 'r') as f:
        summary_text = f.read()
    gitbook_summary = GitBookSummary(summary_text=summary_text)
    new_summary_list = []
    for summary in gitbook_summary.summary_list:
        type = summary['type']
        if type == 'link':
            link = summary['link']
            pattern = re.compile(r'/(\d{8})/\d+_\d+')
            match = pattern.search(link)
            if match:
                new_link = re.sub(r'/\d{8}/', '/', link)
                print(f"move: {link}, {new_link}")
                old_path = os.path.join(base_dir, link)
                new_path = os.path.join(base_dir, new_link)
                shutil.move(old_path, new_path)
                summary['link'] = new_link
                new_summary_list.append(summary)
            else:
                new_summary_list.append(summary)
        else:
            new_summary_list.append(summary)
    with open(summary_file, 'w') as f:
        f.write(gitbook_summary.to_markdown())
    return gitbook_summary

def add_page_keywords_and_description(directory, max_process_count = 5):
    process_queue = []
    article_id = 0
    for root, dirs, files in os.walk(directory):
        if article_id >= max_process_count:
            break
        for file in files:
            if file.endswith(".md") and file not in ['SUMMARY.md']:
                full_file = os.path.join(root, file)
                with open(full_file) as f:
                    content = f.read()
                    lines = content.splitlines()
                    if len(lines) <= 2:
                        print(f'skip for line too less: {full_file}')
                        continue
                    if re.search(f'^description: .*', content, re.MULTILINE):
                        print(f'skip for description exist: {full_file}')
                        continue
                    match = re.search(r'^(#|title:) (.*)', content, re.MULTILINE)
                    if match:
                        title = match.group(2).strip('" ')
                    else:
                        title = ''
                    summary = extract_summary(content)
                    article_id += 1
                    process_info = {'file': file, 'full_file': full_file, 'content': content, 'summary': summary, 'article_id': article_id, 'title': title}
                    process_queue.append(process_info)
                    if article_id >= max_process_count:
                        break
                    if len(process_queue) >= 10:
                        process_queue_page_keywords_and_description(process_queue)
                        process_queue = []
    if len(process_queue) > 0:
        process_queue_page_keywords_and_description(process_queue)

def process_queue_page_keywords_and_description(process_queue):
    article_infos = []
    for process_info in process_queue:
        article_infos.append({'article_id': process_info['article_id'],'content': process_info['summary']})
    article_infos
    prompt_lines = ['我会给你多个网页的ID和主要内容，请根据输入的网页ID和网页主要内容， 生成每个网页的元数据。请直接返回JSON格式的列表，前后不要有额外内容。',
              '元数据包括：网页ID，网页的描述，网页的搜索引擎SEO关键词，网页的搜索引擎SEO额外关键词。',
              '网页的描述: 用于告知搜索引擎SEO优化的网页描述，默认使用中文，请根据我提供的网页主要内容来总结，150个字符以内，单段不换行，请保持语句通顺完整，结尾有标点表示句子结束。',
              '网页的搜索引擎SEO关键词: 2 个关键词, 字符串类型，多个关键词请以英文逗号和空格连接。',
              '网页的搜索引擎SEO额外关键词: 请从提供的可能的关键词列表中选出 2 个最适合作为本文关键词且不在[网页搜索引擎SEO关键词]里的关键词，可能的关键词列表为：IM SDK,即时通讯SDK,APP内聊天功能,IM开源,IM云服务,PUSH SDK,第三方推送,RTC SDK,实时音视频,Chat AI SDK,企业级AI,AI Agent,AI智能体。',
              '输出格式为:[{"article_id": 1, "description": "", "keywords": "", "extra_keywords":""},{"article_id": 2, "description": "", "keywords": "", "extra_keywords":""}, ...]',
              f'网页主要内容列表为：{json.dumps(article_infos, ensure_ascii=False)}']
    prompt = "\n".join(prompt_lines)
    # print(f'prompt: {prompt}\n')
    url = 'https://connector-preview.lanyingim.com/v1/chat/completions'
    api_key = os.getenv('openapi_key')
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    body = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 4096
    }
    print("request to openai")
    response = requests.request("POST", url, headers=headers, json=body)
    print(f'respose: {response.text}')
    res = response.json()
    response_message = res['choices'][0]['message']
    reply = response_message.get('content', "")
    metadata_list = json.loads(reply)
    for metadata in metadata_list:
        article_id = metadata.get('article_id',0)
        description = metadata.get('description', '')
        keywords = metadata.get('keywords', '')
        extra_keywords = metadata.get('extra_keywords', '')
        if not(article_id > 0 and description != '' and keywords != '' and extra_keywords != ''):
            print(f"bad metadata: {metadata}")
            continue
        process_info = next((item for item in process_queue if item["article_id"] == article_id), None)
        if process_info is None:
            print(f'bad article_id:{metadata}')
            continue
        full_file = process_info['full_file']
        content = process_info['content']
        title = process_info['title']
        markdown_header = f'---\ndescription: {description}\nkeywords: {keywords}, {extra_keywords}\n---\n'
        print(f'update file:{full_file}\ntitle:{title}\nheader:{markdown_header}')
        with open(full_file, 'w') as f:
            f.write(f'{markdown_header}{content}')

def extract_summary(content):
    lines = []
    # 提取1、2、3级标题
    headers = re.findall(r'^#{1,3}\s+(.*)$', content, flags=re.MULTILINE)
    count = 0
    max_count = 500
    for header in headers:
        header = header.strip()
        count += len(header)
        if count <= max_count or len(lines) == 0:
            lines.append(header)
    if len(lines) < 2:
        lines = [content[:300]]
    return '\n'.join(lines)

def add_readme_file_dir_prefix(base_dir):
    summary_file = os.path.join(base_dir, 'SUMMARY.md')
    with open(summary_file, 'r') as f:
        summary_text = f.read()
    gitbook_summary = GitBookSummary(summary_text=summary_text)
    for summary in gitbook_summary.summary_list:
        type = summary['type']
        if type == 'link':
            link = summary['link']
            pattern = r'/(\d{8}|latest)/README.md'
            match = re.search(pattern, link)
            if match:
                path = os.path.join(base_dir, link)
                print(f"found: {path}")
                with open(path, 'r') as f:
                    content = f.read()
                    lines = content.splitlines()
                    if len(lines) == 1 and '/' not in content:
                        prefix = link.split('/')[0].capitalize()
                        new_content = re.sub(r'# (.*)', r'# {}/\1'.format(prefix), content)
                        print(f"change content: path:{path}\ncontent:{content}\nnew_content:{new_content}")
                        with open(path, 'w') as ff:
                            ff.write(new_content)
