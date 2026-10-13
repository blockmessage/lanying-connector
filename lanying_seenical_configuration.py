"""Saved Seenical conversation content settings; drafts stay in the browser."""
import json
import logging

from sqlalchemy import text

import lanying_agent_tools_storage as storage


def validate_configuration(value):
    strings = {'name': 255, 'prompt': 20000, 'sessionPrompt': 5000,
               'keywords': 50000, 'contentLanguage': 16}
    numbers = {'articleCount': (1, 1000), 'wordMin': (1, 1000000), 'wordMax': (1, 1000000)}
    if not isinstance(value, dict) or set(value) - (strings.keys() | numbers.keys()):
        raise ValueError('Invalid conversation configuration fields')
    if not isinstance(value.get('prompt'), str) or not value['prompt'].strip():
        raise ValueError('Content topic is required')
    for key, item in value.items():
        if key in strings and (not isinstance(item, str) or len(item) > strings[key]):
            raise ValueError('Invalid conversation configuration text')
        if key in numbers and (type(item) is not int or not numbers[key][0] <= item <= numbers[key][1]):
            raise ValueError('Invalid conversation configuration number')
    if value.get('contentLanguage', 'zh-hans') not in ('zh-hans', 'en'):
        raise ValueError('Invalid conversation language')
    if value.get('wordMin', 1200) > value.get('wordMax', 2400):
        raise ValueError('Minimum word count exceeds maximum')
    return value


def handle(app_id, actor, data, save=False):
    import lanying_agent_tools as tools
    try:
        bindings = tools.list_seenical_conversations(app_id, actor)
        if bindings.get('result') != 'ok':
            raise ValueError('Conversation is unavailable')
        by_id = {item['seenical_session_id']: item for item in bindings['data']['list']}
        params = {'app_id': str(app_id), 'bound_im_user_id': str(actor.get('im_user_id', ''))}
        engine = storage.get_engine()
        if engine is None:
            raise RuntimeError('Configuration storage is disabled')
        if save:
            if set(data) - {'seenical_session_id', 'configuration', 'initial_configuration'}:
                raise ValueError('Invalid conversation configuration fields')
            session_id = str(data.get('seenical_session_id', ''))
            binding = by_id.get(session_id)
            if not binding or tools._validate_seenical_conversation(app_id, actor, binding).get('result') != 'ok':
                raise ValueError('Conversation is unavailable')
            changes = data.get('configuration')
            initial = data.get('initial_configuration', {})
            if not isinstance(changes, dict) or not changes or not isinstance(initial, dict):
                raise ValueError('Invalid conversation configuration fields')
            params['seenical_session_id'] = session_id
            with engine.begin() as conn:
                row = conn.execute(text('''SELECT configuration FROM seenical_conversation_configuration
                    WHERE app_id=:app_id AND bound_im_user_id=:bound_im_user_id
                    AND seenical_session_id=:seenical_session_id FOR UPDATE'''), params).mappings().first()
                # Summary replies update only sessionPrompt. Initial values are used
                # solely for a first save, never to overwrite newer saved settings.
                current = json.loads(row['configuration']) if row else initial
                configuration = validate_configuration(dict(current, **changes))
                params['configuration'] = json.dumps(configuration, ensure_ascii=False)
                conn.execute(text('''INSERT INTO seenical_conversation_configuration
                    (app_id, seenical_session_id, bound_im_user_id, configuration)
                    VALUES (:app_id, :seenical_session_id, :bound_im_user_id, :configuration)
                    ON DUPLICATE KEY UPDATE configuration=VALUES(configuration)'''), params)
            return {'result': 'ok', 'data': {'seenical_session_id': session_id, 'configuration': configuration}}
        session_id = data.get('seenical_session_id')
        if set(data) - {'seenical_session_id'} or (session_id is not None and not isinstance(session_id, str)):
            raise ValueError('Invalid conversation configuration fields')
        if session_id is not None:
            binding = by_id.get(session_id)
            if not binding or tools._validate_seenical_conversation(app_id, actor, binding).get('result') != 'ok':
                raise ValueError('Conversation is unavailable')
            params['seenical_session_id'] = session_id
        with engine.connect() as conn:
            rows = conn.execute(text('''SELECT seenical_session_id, configuration
                FROM seenical_conversation_configuration
                WHERE app_id=:app_id AND bound_im_user_id=:bound_im_user_id'''
                + (' AND seenical_session_id=:seenical_session_id' if session_id is not None else '')), params).mappings().all()
        values = []
        unavailable = []
        for row in rows:
            binding = by_id.get(row['seenical_session_id'])
            if not binding:
                continue
            try:
                valid = session_id is not None or tools._validate_seenical_conversation(app_id, actor, binding).get('result') == 'ok'
            except Exception as error:
                logging.warning('Seenical configuration access check failed | app_id:%s | session_id:%s | error_type:%s',
                                app_id, row['seenical_session_id'], type(error).__name__)
                valid = False
            if valid:
                values.append({'seenical_session_id': row['seenical_session_id'],
                               'configuration': json.loads(row['configuration'])})
            else:
                unavailable.append(row['seenical_session_id'])
        return {'result': 'ok', 'data': {'list': values, 'unavailable_session_ids': unavailable}}
    except ValueError as error:
        return {'result': 'error', 'message': str(error)}
    except Exception as error:
        # Database exceptions can contain prompt text in bound parameters.
        logging.error('Seenical configuration storage failed | app_id:%s | error_type:%s', app_id, type(error).__name__)
        return {'result': 'error', 'message': 'Conversation configuration storage is unavailable'}
