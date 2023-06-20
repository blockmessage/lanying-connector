import re
import lanying_config
import logging

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
            result = check_rule(rule, fields, index, app_id)
            args.extend(result)
        return {"result":"found", "name":name, "args":args}
    except Exception as e:
        pass
    return {"result":"not_found"}

def check_rule(rule, fields, index, app_id):
    type = rule["type"]
    if type == "string_exact":
        if fields[index] == rule["value"]:
            return []
    elif type == "string":
        return [fields[index]]
    elif type == "string_rest":
        return [" ".join(fields[index:])]
    elif type == "preset_name":
        preset_names = calc_preset_names(app_id)
        if fields[index] in preset_names:
            logging.info("lanying_commad found preset_name | preset_name:{fields[index]}, preset_names:{preset_names}")
            return [fields[index]]
        else:
            logging.info("lanying_commad not found preset_name | preset_name:{fields[index]}, preset_names:{preset_names}")
    raise Exception({"not_match", rule, fields, index})

def calc_preset_names(app_id):
    preset_names = ["default"]
    config = lanying_config.get_lanying_connector(app_id)
    if "preset" in config and "presets" in config["preset"]:
        try:
            for k in config["preset"]["presets"].keys():
                preset_names.append(k)
        except Exception as e:
            logging.exception(e)
            pass
    return preset_names

def help(app_id, role):
    result = ['可以用命令如下:']
    index = 0
    for command in all_commands():
        if role not in command['roles']: 
            continue
        if 'desc' in command:
            index = index + 1
            result.append(f"{index}. {command['desc']}")
        elif 'desc_rule' in command:
            rule = command["desc_rule"]
            if rule["type"] == "preset_name":
                preset_names = calc_preset_names(app_id)
                for preset_name in preset_names:
                    desc = rule["format"].replace("{preset_name}", f"{preset_name}")
                    index = index + 1
                    result.append(f"{index}. {desc}")
    return "\n".join(result)

def all_commands():
    return [
        {
            "name": "bluevector_add",
            "roles": ["admin"],
            "desc": "将文件添加到知识库:\n/bluevector add <KNOWLEDGE_BASE_NAME> <FILE_ID>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "add"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_status",
            "roles": ["admin"],
            "desc": "查询知识库状态:\n/bluevector status <KNOWLEDGE_BASE_NAME>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "status"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_delete",
            "roles": ["admin"],
            "desc": "知识库删除文档:\n/bluevector delete <KNOWLEDGE_BASE_NAME> <DOC_ID>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "delete"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_help",
            "roles": ["admin","normal"],
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "help"}
            ]
        },
        {
            "name": "bluevector_error",
            "roles": ["admin","normal"],
            "rules": [
                {"type": "string_exact", "value": "bluevector"}
            ]
        },
        {
            "name": "search_on_doc_by_default_preset",
            "roles": ["admin","normal"],
            "desc": "默认预设下使用文档ID查询:\n/on doc <DOC_ID> <MESSAGE>",
            "rules": [
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "doc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "search_on_doc_by_preset",
            "roles": ["admin","normal"],
            "desc_rule": {
                "type":"preset_name",
                "format": "预设({preset_name})下使用文档ID查询:\n/{preset_name} on doc <DOC_ID> <MESSAGE>"
            },
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "doc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "search_by_preset",
            "roles": ["admin","normal"],
            "desc_rule": {
                "type":"preset_name",
                "format": "预设({preset_name})下查询:\n/{preset_name} <MESSAGE>"
            },
            "rules": [
                {"type": "preset_name"},
                {"type": "string_rest"}
            ]
        }
    ]
