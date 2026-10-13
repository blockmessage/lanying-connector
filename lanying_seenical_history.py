"""Immutable, display-only text inherited when creating a Seenical conversation.

This is a user-supplied context snapshot, not an IM message or authorization
source. Never persist tool payloads, attachment URLs, or executable card state.
"""
import json
import logging

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import lanying_agent_tools_storage as storage

MAX_MESSAGES = 500
MAX_BYTES = 2 * 1024 * 1024


def _authorize(app_id, actor, session_id):
    import lanying_agent_tools as tools
    result = tools.list_seenical_conversations(app_id, actor)
    if result.get('result') != 'ok':
        raise ValueError('Conversation is unavailable')
    binding = next((item for item in result['data']['list']
                    if item['seenical_session_id'] == session_id), None)
    if not binding or tools._validate_seenical_conversation(
            app_id, actor, binding).get('result') != 'ok':
        raise ValueError('Conversation is unavailable')
    return binding


def _messages(value):
    if not isinstance(value, list) or len(value) > MAX_MESSAGES:
        raise ValueError('Inherited history exceeds the supported message count')
    result = []
    for item in value:
        if not isinstance(item, dict) or set(item) - {'role', 'text', 'timestamp', 'speaker_name'}:
            raise ValueError('Invalid inherited message fields')
        if item.get('role') not in ('user', 'assistant') or not isinstance(item.get('text'), str):
            raise ValueError('Invalid inherited message')
        timestamp = item.get('timestamp', 0)
        if type(timestamp) is not int or not 0 <= timestamp <= 8640000000000000:
            raise ValueError('Invalid inherited message timestamp')
        speaker = item.get('speaker_name', '')
        if not isinstance(speaker, str) or len(speaker) > 255:
            raise ValueError('Invalid inherited speaker name')
        if item['text'].strip():
            result.append({'role': item['role'], 'text': item['text'], 'timestamp': timestamp,
                           **({'speaker_name': speaker} if speaker else {})})
    encoded = json.dumps(result, ensure_ascii=False)
    if len(encoded.encode('utf-8')) > MAX_BYTES:
        raise ValueError('Inherited history exceeds the supported size')
    return encoded


def _read(conn, params):
    row = conn.execute(text('''SELECT source_session_id, messages, bound_im_user_id
        FROM seenical_conversation_history_snapshot
        WHERE app_id=:app_id AND seenical_session_id=:seenical_session_id'''), params).mappings().first()
    if row and str(row['bound_im_user_id']) != params['bound_im_user_id']:
        raise ValueError('Conversation owner has changed')
    return {'source_session_id': row['source_session_id'], 'messages': json.loads(row['messages'])} if row else {'source_session_id': '', 'messages': []}


def handle(app_id, actor, data, save=False):
    try:
        session_id = str(data.get('seenical_session_id', ''))
        target = _authorize(app_id, actor, session_id)
        if target['conversation_type'] != 'GROUPCHAT':
            raise ValueError('Only child conversations may inherit history')
        params = {'app_id': str(app_id), 'seenical_session_id': session_id,
                  'bound_im_user_id': str(actor.get('im_user_id', ''))}
        engine = storage.get_engine()
        if engine is None:
            raise RuntimeError('History storage is disabled')
        if save:
            source_id = str(data.get('source_session_id', ''))
            source = _authorize(app_id, actor, source_id)
            if source_id == session_id or source['chatbot_id'] != target['chatbot_id']:
                raise ValueError('Inherited history must come from the same Agent')
            params.update(source_session_id=source_id, messages=_messages(data.get('messages')))
            try:
                with engine.begin() as conn:
                    conn.execute(text('''INSERT INTO seenical_conversation_history_snapshot
                        (app_id, seenical_session_id, source_session_id, bound_im_user_id, messages)
                        VALUES (:app_id, :seenical_session_id, :source_session_id, :bound_im_user_id, :messages)'''), params)
            except IntegrityError:
                # A retried save reads the original immutable snapshot.
                # The primary key handles duplicate requests across nodes.
                pass
        with engine.connect() as conn:
            snapshot = _read(conn, params)
        if save and snapshot['source_session_id'] != params['source_session_id']:
            raise ValueError('Conversation already has a different history source')
        return {'result': 'ok', 'data': snapshot}
    except ValueError as error:
        return {'result': 'error', 'message': str(error)}
    except Exception as error:
        # SQLAlchemy exceptions can include bound SQL parameters (chat text).
        logging.error('Seenical inherited history storage failed | app_id:%s | error_type:%s',
                      app_id, type(error).__name__)
        return {'result': 'error', 'message': 'Conversation history storage is unavailable'}
