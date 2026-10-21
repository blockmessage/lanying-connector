"""Non-blocking notifications from existing material operations.

Only a durable dirty flag is written. No GitHub I/O or model call occurs here.
Workspace failures must not undo an already successful upload or generation.
"""
import logging


def materials_changed(app_id):
    try:
        import lanying_agent_tools_storage as storage
        if not storage.is_enabled():
            return
        from lanying_workspace_sync import request_check
        request_check(app_id)
    except Exception:
        logging.warning('workspace change notification failed | app_id:%s | use manual check', app_id)
