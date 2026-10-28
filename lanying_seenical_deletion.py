"""Durable conversation deletion, with cooperative cancellation of LOOP work.

The verified conversation binding is also the deletion job and tombstone.
DELETING/DELETED rows must never be registered again. MySQL advisory locks
protect only in-flight writers from cleanup; they are not job/authorization state.
No connection is held by the HTTP deletion request while generation finishes.
"""
from contextlib import contextmanager
from functools import wraps
import hashlib
import logging

from sqlalchemy import text
import lanying_agent_tools_storage as storage


class Cancelled(Exception):
    pass


@contextmanager
def operation_lock(app_id, kind, identity):
    """Nonblocking, connection-owned lock; process death releases it in MySQL."""
    engine = storage._get_engine()
    if engine is None:
        yield True  # Legacy deployments without Seenical persistence.
        return
    name = 'seenical-delete:' + hashlib.sha256(
        f'{app_id}:{kind}:{identity}'.encode()).hexdigest()[:48]
    with engine.connect() as conn:
        acquired = conn.execute(text('SELECT GET_LOCK(:name, 0)'), {'name': name}).scalar() == 1
        try:
            yield acquired
        finally:
            if acquired:
                try:
                    conn.execute(text('SELECT RELEASE_LOCK(:name)'), {'name': name})
                except Exception:
                    # Never return a connection with a named lock to the pool.
                    conn.invalidate()
                    raise


def deleting_task_ids(app_id):
    engine = storage._get_engine()
    if engine is None:
        return set()
    with engine.connect() as conn:
        return {str(row[0]) for row in conn.execute(text(
            "SELECT deletion_task_id FROM seenical_conversation_binding "
            "WHERE app_id=:a AND deletion_task_id IS NOT NULL"), {'a': app_id})}


def check_task(app_id, task_id):
    if str(task_id) in deleting_task_ids(app_id):
        raise Cancelled('conversation_deleting')


def check_run(app_id, run_id):
    import lanying_grow_ai as grow
    run = grow.get_task_run(app_id, run_id)
    if not run:
        raise Cancelled('task_run not exist')
    check_task(app_id, run['task_id'])
    return run


def guarded(kind):
    """Protect task creation or a run writer; distinct runs remain concurrent.

    Applied only at entry points, not nested helpers, to avoid recursive locks.
    A stale Celery delivery returns a terminal result instead of retrying.
    """
    def decorate(function):
        @wraps(function)
        def call(app_id, *args, **kwargs):
            identity_key = {'task': 'task_id', 'run': 'task_run_id', 'preview': 'preview_id'}[kind]
            identity = args[0] if args else kwargs[identity_key]
            import lanying_grow_ai as grow
            run_id = identity
            if kind == 'preview':
                preview = grow.get_preview(app_id, identity)
                if not preview:
                    return {'result': 'error', 'message': 'preview not found', 'retry': False}
                run_id = preview['task_run_id']
            lock_kind = 'task' if kind == 'task' else 'run'
            with operation_lock(app_id, lock_kind, run_id) as acquired:
                if not acquired:
                    # Existing Celery retries can handle a temporarily busy run.
                    raise RuntimeError('conversation operation in progress')
                try:
                    if kind == 'task':
                        check_task(app_id, identity)
                    else:
                        check_run(app_id, run_id)
                    return function(app_id, *args, **kwargs)
                except Cancelled:
                    return {'result': 'error', 'message': 'conversation_deleting', 'retry': False}
        return call
    return decorate


def tombstones(app_id, bound_user_id):
    engine = storage._get_engine()
    if engine is None:
        return []
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(text(
            "SELECT seenical_session_id,conversation_id,deletion_task_id AS task_id,status "
            "FROM seenical_conversation_binding WHERE app_id=:a "
            "AND bound_im_user_id=:u AND status IN ('DELETING','DELETED')"),
            {'a': app_id, 'u': bound_user_id}).mappings()]


def request(app_id, actor, data):
    import lanying_agent_tools as tools
    binding = tools.get_im_binding_projection(app_id)
    user_id = str(actor.get('im_user_id', ''))
    if not binding or binding.get('status') != 'BOUND' or str(binding.get('im_user_id')) != user_id:
        return {'result': 'error', 'message': 'Seenical IM binding is unavailable'}
    session_id = str(data.get('seenical_session_id', ''))
    group_id = str(data.get('conversation_id', ''))
    expected = data.get('task_ids')
    if (not session_id or not group_id.isdigit() or not isinstance(expected, list)
            or len(expected) > 100 or any(not isinstance(v, str) for v in expected)):
        return {'result': 'error', 'message': 'invalid conversation deletion'}
    engine = storage._get_engine()
    if engine is None:
        return {'result': 'error', 'message': 'Seenical conversation storage unavailable'}
    with engine.begin() as conn:
        # Match material writers' lock order: space, then conversation binding.
        conn.execute(text('SELECT app_id FROM seenical_material_space WHERE app_id=:a FOR UPDATE'),
                     {'a': app_id}).first()
        row = conn.execute(text(
            'SELECT * FROM seenical_conversation_binding '
            'WHERE app_id=:a AND seenical_session_id=:s FOR UPDATE'),
            {'a': app_id, 's': session_id}).mappings().first()
        if (not row or row['conversation_type'] != 'GROUPCHAT'
                or str(row['conversation_id']) != group_id
                or str(row['bound_im_user_id']) != user_id):
            return {'result': 'error', 'message': 'conversation not registered'}
        if row['status'] not in ('DELETING', 'DELETED'):
            task_id = str(row['task_id'] or '')
            if set(expected) != ({task_id} if task_id else set()):
                return {'result': 'error', 'message': 'conversation_loops_changed'}
            conn.execute(text(
                "UPDATE seenical_conversation_binding SET status='DELETING', "
                "deletion_task_id=task_id,deletion_error='',revision=revision+1 "
                'WHERE app_id=:a AND seenical_session_id=:s'), {'a': app_id, 's': session_id})
    # The minute sweep recovers a broker outage or a lost reply after COMMIT.
    try:
        from lanying_tasks import seenical_conversation_cleanup
        seenical_conversation_cleanup.delay(app_id, session_id)
    except Exception:
        logging.warning('conversation cleanup enqueue failed | app_id:%s session_id:%s', app_id, session_id)
    return {'result': 'ok', 'data': {'accepted': True, 'seenical_session_id': session_id}}


def _run_previews(app_id, run):
    """Include an earlier preview still active on a site or awaiting PR closure."""
    import lanying_grow_ai as grow
    import lanying_redis
    candidates = {run.get('preview_id', '')}
    for site in grow.get_site_list(app_id)['data']['list']:
        candidates.update([site.get('active_preview_id', ''), site.get('pending_preview_id', '')])
    for value in lanying_redis.get_redis_connection().smembers(grow.preview_pr_set_key()):
        value = value.decode() if isinstance(value, bytes) else value
        owner, _, preview_id = value.partition(':')
        if owner == str(app_id):
            candidates.add(preview_id)
    result = []
    for preview_id in candidates - {''}:
        preview = grow.get_preview(app_id, preview_id)
        if preview and preview['task_run_id'] == run['task_run_id']:
            result.append(preview_id)
    return result


def _cleanup_run(app_id, run):
    import lanying_grow_ai as grow
    run_id = run['task_run_id']
    with operation_lock(app_id, 'run', run_id) as acquired:
        if not acquired:
            return False
        run = grow.get_task_run(app_id, run_id)
        if not run:
            return True
        # No writer can resume this run: the durable task tombstone is already set.
        grow.update_task_run_field(app_id, run_id, 'status', 'cancelled')
        previews_pending = False
        for preview_id in _run_previews(app_id, run):
            result = grow.preview_discard(app_id, preview_id)
            previews_pending = result.get('result') != 'ok' or bool(grow.get_preview(app_id, preview_id)) or previews_pending
        if previews_pending:
            return False
        result = grow.delete_task_run(app_id, run_id)
        if result.get('result') != 'ok':
            raise RuntimeError('run_cleanup_failed')
        return True


def cleanup(app_id, session_id):
    """Retryable cleanup; successful steps are safe to repeat, never rolled back."""
    import lanying_grow_ai as grow
    import lanying_im_api
    import lanying_seenical_materials as materials
    engine = storage._get_engine()
    if engine is None:
        return
    with operation_lock(app_id, 'conversation', session_id) as acquired:
        if not acquired:
            return
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM seenical_conversation_binding WHERE app_id=:a "
                "AND seenical_session_id=:s AND status='DELETING'"),
                {'a': app_id, 's': session_id}).mappings().first()
        if not row:
            return
        error = ''
        try:
            # Remove the IM entry promptly, independently of slow model/preview work.
            if not lanying_im_api.destroy_seenical_group(app_id, row['conversation_id']):
                raise RuntimeError('group_cleanup_failed')
            materials.release_owner(app_id, 'session', session_id)
            task_id = row['deletion_task_id']
            if task_id:
                # Serialize against run creation, not against unrelated runs/LOOPs.
                with operation_lock(app_id, 'task', task_id) as acquired:
                    if not acquired:
                        return
                    materials.release_owner(app_id, 'plan', task_id)
                    task = grow.get_task(app_id, task_id)
                    if task and task.get('schedule') == 'on':
                        grow.set_task_schedule(app_id, task_id, 'off', 'conversation deleted')
                    complete = True
                    for run in grow.get_task_run_list(app_id, task_id)['data']['list']:
                        complete = _cleanup_run(app_id, run) and complete
                    if not complete:
                        error = 'waiting_for_run_or_preview'
                        return
                    if task:
                        result = grow.delete_task(app_id, task_id)
                        if result.get('result') != 'ok':
                            raise RuntimeError('loop_cleanup_failed')
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE seenical_conversation_binding SET status='DELETED',task_id=NULL, "
                    "deletion_error='',revision=revision+1 WHERE app_id=:a AND seenical_session_id=:s"),
                    {'a': app_id, 's': session_id})
            logging.info('conversation cleanup complete | app_id:%s session_id:%s', app_id, session_id)
        except Exception as exc:
            error = 'cleanup_failed'
            logging.warning('conversation cleanup pending | app_id:%s session_id:%s error_type:%s',
                            app_id, session_id, type(exc).__name__)
        finally:
            if error:
                with engine.begin() as conn:
                    conn.execute(text(
                        'UPDATE seenical_conversation_binding SET deletion_error=:e, '
                        'updated_at=CURRENT_TIMESTAMP(3) WHERE app_id=:a AND seenical_session_id=:s'),
                        {'a': app_id, 's': session_id, 'e': error})


def sweep():
    engine = storage._get_engine()
    if engine is None:
        return
    with engine.connect() as conn:
        jobs = conn.execute(text(
            "SELECT app_id,seenical_session_id FROM seenical_conversation_binding "
            "WHERE status='DELETING' ORDER BY updated_at LIMIT 100")).all()
    from lanying_tasks import seenical_conversation_cleanup
    for app_id, session_id in jobs:
        seenical_conversation_cleanup.delay(app_id, session_id)
