MASKED_VALUE = '__MASKED_SENSITIVE_VALUE__'
_MISSING = object()


def restore_masked_values(value, current=_MISSING):
    if value == MASKED_VALUE:
        if current is _MISSING:
            raise ValueError('masked plugin value has no existing value')
        return current
    if isinstance(value, dict):
        current_map = current if isinstance(current, dict) else {}
        return {
            key: restore_masked_values(item, current_map.get(key, _MISSING))
            for key, item in value.items()
        }
    if isinstance(value, list):
        current_list = current if isinstance(current, list) else []
        return [
            restore_masked_values(
                item,
                current_list[index] if index < len(current_list) else _MISSING)
            for index, item in enumerate(value)
        ]
    return value


def restore_masked_config_map(value, current):
    if not isinstance(value, dict):
        raise ValueError('plugin configuration field must be an object')
    current_map = current if isinstance(current, dict) else {}
    return restore_masked_values(value, current_map)
