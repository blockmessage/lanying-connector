import re

def find_command(content, app_id):
    if len(content) == 0 or content[0] != "/":
        return {"result":"not_found"}
    content = content[1:]
    fields = re.split("[ \t\n]{1,}", content)
    commands = all_commands()
    for command in commands:
        res = check_command(command, fields, app_id)
        if res["result"] == 'found':
            return res
    return {"result":"not_found"}

def check_command(command, fields, app_id):
    try:
        rules = command["rules"]
        name = command["name"]
        args = []
        for index, rule in enumerate(rules):
            result = check_rule(rule, fields, index)
            args.extend(result)
        return {"result":"found", "name":name, "args":args}
    except Exception as e:
        pass
    return {"result":"not_found"}

def check_rule(rule, fields, index):
    type = rule["type"]
    if type == "string_exact":
        if fields[index] == rule["value"]:
            return []
    elif type == "string":
        return [fields[index]]
    elif type == "string_rest":
        return [" ".join(fields[index:])]
    elif type == "embedding_name":
        return [fields[index]]
    raise Exception({"not_match", rule, fields, index})

def all_commands():
    return [
        {
            "name": "bluevector_add",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "add"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_status",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "status"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_delete",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "delete"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_help",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "help"}
            ]
        },
        {
            "name": "bluevector_error",
            "rules": [
                {"type": "string_exact", "value": "bluevector"}
            ]
        },
        {
            "name": "search_on_doc_by_default_preset",
            "rules": [
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "doc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "search_on_doc_by_preset",
            "rules": [
                {"type": "embedding_name"},
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "doc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        }
    ]
