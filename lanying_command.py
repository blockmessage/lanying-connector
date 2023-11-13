import re
import lanying_config
import logging
import lanying_chatbot

def find_command(content, app_id):
    if len(content) == 0 or content[0] != "/":
        return {"result":"not_found"}
    content = content[1:]
    fields = re.split("[ \t\n]{1,}", content)
    commands = all_commands()
    for command in commands:
        command_res = check_command(command, fields, app_id)
        if command_res["result"] == 'found':
            logging.info(f"found command: {command_res}")
            return command_res
        alias_res = check_alias(fields)
        if alias_res["result"] == 'found':
            new_fields = alias_res['new_fields']
            alias_comand_res = check_command(command, new_fields, app_id)
            if alias_comand_res["result"] == 'found':
                logging.info(f"found alias command: {alias_comand_res}")
                return alias_comand_res
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
            logging.info(f"lanying_commad found preset_name | preset_name:{fields[index]}, preset_names:{preset_names}")
            return [fields[index]]
    raise Exception({"not_match", rule, fields, index})

def calc_preset_names(app_id):
    if lanying_chatbot.is_chatbot_mode(app_id):
        return lanying_chatbot.get_chatbot_names(app_id)
    else:
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

def calc_preset_infos(app_id, user_id):
    if lanying_chatbot.is_chatbot_mode(app_id):
        preset_infos = []
        chatbot_id = lanying_chatbot.get_user_chatbot_id(app_id, user_id)
        chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot:
            default_desc = chatbot.get('desc', '')
            if default_desc != '':
                sep = " "
            preset_infos.append(("default", f"{default_desc}{sep}默认预设，也可使用别名 /bluebird 或 /bb 代替"))
            chatbot_ids = chatbot.get('chatbot_ids',[])
            for sub_chatbot_id in chatbot_ids:
                sub_chatbot = lanying_chatbot.get_chatbot(app_id, sub_chatbot_id)
                name = sub_chatbot['name']
                desc = sub_chatbot.get('desc', '暂无说明')
                preset_infos.append((name, desc))
        return preset_infos
    else:
        preset_infos = []
        config = lanying_config.get_lanying_connector(app_id)
        default_desc = ""
        sep = ""
        if "preset" in config:
            ext = config["preset"].get('ext', {})
            default_desc = ext.get('preset_desc', '')
        if default_desc != '':
            sep = " "
        preset_infos.append(("default", f"{default_desc}{sep}默认预设，也可使用别名 /bluebird 或 /bb 代替"))
        if "preset" in config and "presets" in config["preset"]:
            try:
                for k in config["preset"]["presets"].keys():
                    preset = config["preset"]["presets"][k]
                    ext = preset.get('ext',{})
                    preset_infos.append((k, ext.get('preset_desc', '暂无说明')))
            except Exception as e:
                logging.exception(e)
                pass
        return preset_infos

# def help(app_id, role):
#     result = ['可以用命令如下:']
#     index = 0
#     for command in all_commands():
#         if role not in command['roles']: 
#             continue
#         if 'desc' in command:
#             index = index + 1
#             result.append(f"{index}. {command['desc']}")
#         elif 'desc_rule' in command:
#             rule = command["desc_rule"]
#             if rule["type"] == "preset_name":
#                 preset_names = calc_preset_names(app_id)
#                 for preset_name in preset_names:
#                     desc = rule["format"].replace("{preset_name}", f"{preset_name}")
#                     index = index + 1
#                     result.append(f"{index}. {desc}")
#         if 'desc_extra' in command:
#             for desc in command['desc_extra']:
#                 index = index + 1
#                 result.append(f"{index}. {desc}")
#     return "\n".join(result)

def pretty_help(app_id, user_id):
    return f"""用法：/command [OPTION] [ARGS]
说明：使用命令操作企业知识库或限定参考知识范围。

可用命令如下:
1. 查询当前预设绑定的知识库信息：/bluevector info
2. 查看知识库状态及详情: /bluevector status <KNOWLEDGE_BASE_NAME> 
3. 添加文件到知识库: /bluevector add <KNOWLEDGE_BASE_NAME> <FILE_ID> 
4. 从知识库中删除文档: /bluevector delete <KNOWLEDGE_BASE_NAME> <DOC_ID> 
5. 限定AI指令参考范围为文档: /on doc <DOC_ID> <AI_MESSAGE> 
6. 限定AI指令参考文档全文: /on fulldoc <DOC_ID> <AI_MESSAGE> 
7. 设置默认知识库： /bluevector mode auto <KNOWLEDGE_BASE_NAME>

发送消息可以指定AI预设：
用法：/preset <AI_MESSAGE>
说明：指示AI将会根据特定预设来回答问题。

命令也支持指定预设：
用法：/[preset] command [OPTION] [ARGS]
说明：指示AI使用特定预设执行命令。

如果不指定预设，相当于使用默认预设：
即 /bluevector [OPTION] [ARGS] 相当于执行 /default bluevector [OPTION] [ARGS]

当前可用预设如下：
{pretty_help_preset_info(app_id, user_id)}

通过 /help 或者 /+空格 查看本说明。"""

def pretty_help_preset_info(app_id, user_id):
    lines = []
    for name,desc in calc_preset_infos(app_id, user_id):
        lines.append(f"/{name}：{desc}")
    return "\n".join(lines)

def check_alias(fields):
    if len(fields) > 0:
        command = fields[0]
        for alias_info in all_alias():
            if alias_info["alias"] == command:
                new_fields = [alias_info["for"]]
                new_fields.extend(fields[1:])
                return {'result':'found', 'new_fields':new_fields}
    return {'result':'not_found'}

def all_alias():
    return [
        {"alias": "bluebird", "for":"default"},
        {"alias": "blue", "for":"default"},
        {"alias": "bb", "for":"default"},
        {"alias": "b", "for":"default"},
        {"alias": "", "for":"help"}
    ]
def all_commands():
    return [
        {
            "name": "bluevector_mode",
            "desc": "设置默认知识库",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "mode"},
                {"type": "string_exact", "value": "auto"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_add",
            "desc": "将文件添加到知识库:\n/bluevector add <KNOWLEDGE_BASE_NAME> <FILE_ID>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "add"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_add_with_preset",
            "desc": "将文件添加到知识库:\n/bluevector add <KNOWLEDGE_BASE_NAME> <FILE_ID>",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "add"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_status",
            "desc": "查询知识库状态:\n/bluevector status <KNOWLEDGE_BASE_NAME>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "status"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_status_with_preset",
            "desc": "查询知识库状态:\n/bluevector status <KNOWLEDGE_BASE_NAME>",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "status"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_delete",
            "desc": "知识库删除文档:\n/bluevector delete <KNOWLEDGE_BASE_NAME> <DOC_ID>",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "delete"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_delete_with_preset",
            "desc": "知识库删除文档:\n/bluevector delete <KNOWLEDGE_BASE_NAME> <DOC_ID>",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "delete"},
                {"type": "string", "value": "string"},
                {"type": "string", "value": "string"}
            ]
        },
        {
            "name": "bluevector_info",
            "desc": "列出当前预设绑定的知识库名字和信息:\n/bluevector info",
            "rules": [
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "info"}
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
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "doc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "search_on_fulldoc_by_default_preset",
            "desc": "默认预设下使用文档ID全文查询:\n/on doc <DOC_ID> <MESSAGE>",
            "rules": [
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "fulldoc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "search_on_fulldoc_by_preset",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "on"},
                {"type": "string_exact", "value": "fulldoc"},
                {"type": "string"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "bluevector_info_by_preset",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_exact", "value": "bluevector"},
                {"type": "string_exact", "value": "info"}
            ]
        },
        {
            "name": "search_by_preset",
            "rules": [
                {"type": "preset_name"},
                {"type": "string_rest"}
            ]
        },
        {
            "name": "help",
            "rules": [
                {"type": "string_exact", "value": "help"}
            ]
        }
    ]
