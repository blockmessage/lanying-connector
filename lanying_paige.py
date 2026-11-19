"""Identify the Grow AI Writer by the Capsule used by Butler's importer."""

WRITER_CAPSULE_ID = '949256'


def is_official_capsule(capsule_id):
    return str(capsule_id) == WRITER_CAPSULE_ID


def is_writer(chatbot):
    return any(is_official_capsule(chatbot.get(field)) for field in (
        'capsule_id', 'linked_capsule_id', 'linked_publish_capsule_id'))
