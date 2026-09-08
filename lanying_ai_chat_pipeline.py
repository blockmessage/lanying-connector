import copy
import json
import logging
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PresetResolution:
    preset: dict
    lc_ext: dict
    preset_ext: dict
    preset_name: str
    vendor: str
    model_config: Optional[dict]


@dataclass(frozen=True)
class ChatHandlerResult:
    replies: list
    error_code: str
    error_message: str


@dataclass(frozen=True)
class Prompt:
    preset: dict
    is_force_stream: bool
    oper_msg_config: dict


@dataclass(frozen=True)
class ModelResponse:
    response: dict
    preset: dict
    error: Optional[dict]


@dataclass(frozen=True)
class ToolRunResult:
    response: dict
    reply_ext: dict
    stream_msg_id: int
    is_stream: bool
    stream_msg_last_send_time: float
    function_messages: list
    subsequent_messages: list


@dataclass(frozen=True)
class ReplyResult:
    reply: str
    audio_reply: Optional[dict]
    reply_ext: dict
    command: Optional[dict]
    stream_msg_id: int
    is_stream: bool
    stream_msg_last_send_time: float
    function_messages: list
    subsequent_messages: list


def resolve_chat_preset(config, app_id, preset, is_chatbot_mode, chatbot,
                        command_ext, from_user_id, to_user_id,
                        redis_provider, get_preset_name, get_chatbot_by_name,
                        add_debug_message, get_chat_model_config):
    lc_ext = {}
    try:
        ext = json.loads(config['ext'])
        if 'ai' in ext:
            lc_ext = ext['ai']
        elif 'lanying_connector' in ext:
            lc_ext = ext['lanying_connector']
    except Exception:
        pass

    redis = redis_provider()
    preset_name = ""
    try:
        if ('preset_name' in command_ext
                and command_ext['preset_name'] != "default"):
            requested_name = command_ext['preset_name']
            if is_chatbot_mode:
                sub_chatbot = get_chatbot_by_name(
                    app_id, chatbot['chatbot_ids'], requested_name)
                if sub_chatbot:
                    chatbot = sub_chatbot
                    preset = sub_chatbot['preset']
                    preset_name = requested_name
                    logging.info(
                        f"using preset_name from command:{preset_name}")
            else:
                preset = preset['presets'][requested_name]
                preset_name = requested_name
                logging.info(f"using preset_name from command:{preset_name}")
    except Exception as error:
        logging.exception(error)

    if preset_name == "":
        try:
            if ('preset_name' in lc_ext
                    and lc_ext['preset_name'] != "default"):
                requested_name = lc_ext['preset_name']
                if is_chatbot_mode:
                    sub_chatbot = get_chatbot_by_name(
                        app_id, chatbot['chatbot_ids'], requested_name)
                    if sub_chatbot:
                        chatbot = sub_chatbot
                        preset = sub_chatbot['preset']
                        preset_name = requested_name
                        logging.info(
                            f"using preset_name from lc_ext:{preset_name}")
                else:
                    preset = preset['presets'][requested_name]
                    preset_name = requested_name
                    logging.info(
                        f"using preset_name from lc_ext:{preset_name}")
        except Exception as error:
            logging.exception(error)

    if preset_name == "":
        last_choose_preset_name = get_preset_name(
            redis, from_user_id, to_user_id)
        logging.info(f"lastChoosePresetName:{last_choose_preset_name}")
        if last_choose_preset_name:
            try:
                if last_choose_preset_name != "default":
                    if is_chatbot_mode:
                        sub_chatbot = get_chatbot_by_name(
                            app_id, chatbot['chatbot_ids'],
                            last_choose_preset_name)
                        if sub_chatbot:
                            chatbot = sub_chatbot
                            preset = json.loads(sub_chatbot['preset'])
                            preset_name = last_choose_preset_name
                            logging.info(
                                "using preset_name from last_choose_preset:"
                                f"{preset_name}")
                    else:
                        preset = preset['presets'][last_choose_preset_name]
                        preset_name = last_choose_preset_name
                        logging.info(
                            "using preset_name from last_choose_preset:"
                            f"{preset_name}")
            except Exception as error:
                logging.exception(error)

    if preset_name == "":
        preset_name = chatbot['name'] if is_chatbot_mode else "default"
    preset.pop('presets', None)
    preset_ext = copy.deepcopy(preset.pop('ext', {}))
    add_debug_message(config, f"当前预设为: {preset_name}")
    logging.info(
        f"lanying-connector:ext={json.dumps(lc_ext, ensure_ascii=False)},"
        f"presetExt:{preset_ext}")
    vendor = preset.get('vendor', config.get('vendor', 'openai'))
    model_config = get_chat_model_config(app_id, vendor, preset['model'])
    return PresetResolution(
        preset, lc_ext, preset_ext, preset_name, vendor, model_config)


def build_chat_prompt(config, msg_type, preset, messages, system_functions,
                      user_functions, lc_ext, functions_to_tools):
    preset['messages'] = messages
    if msg_type == 'GROUPCHAT':
        preset['user'] = config['send_from']
    functions = system_functions
    functions.extend(user_functions)
    if functions:
        preset['functions'] = functions
        preset['tools'] = functions_to_tools(functions)
    else:
        preset.pop('functions', None)
        preset.pop('tools', None)
    preset_message_lines = "\n".join([
        f"{message.get('role','')}:{message.get('content','')}"
        for message in messages
    ])
    logging.info(
        f"==========final preset messages/functions============\n"
        f"{preset_message_lines}\n{functions}")
    is_force_stream = lc_ext.get('force_stream') == True
    if is_force_stream:
        logging.info("force use stream")
        preset['stream'] = True
    return Prompt(preset, is_force_stream, {'force_callback': True})


def invoke_chat_model(app_id, config, vendor, prepare_info, model_config,
                      prompt, transform_preset, chat_call,
                      normalize_vendor_response):
    model_preset = transform_preset(
        config, app_id, model_config, prompt.preset)
    response = chat_call(app_id, config, vendor, prepare_info, model_preset)
    response = normalize_vendor_response(response)
    logging.info(f"vendor response | vendor:{vendor}, response:{response}")
    if response.get('result') != 'error':
        return ModelResponse(response, model_preset, None)
    error_code = response.get('code', response.get('reason', ''))
    error_message = (
        response.get('msg', '') or response.get('message', '')
        or response.get('reason', ''))
    if error_code in ['engine_overloaded_error', 'rate_limit_reached_error']:
        error_message = '请求过快，请稍后再试。'
    return ModelResponse(response, model_preset, {
        'result': 'error', 'code': error_code, 'msg': error_message,
    })


def build_reply_result(config, vendor, model, tool_result,
                       notify_empty_reply, add_debug_message):
    response = tool_result.response
    reply = response['reply']
    if reply == '' and vendor == 'deepseek':
        notify_empty_reply(
            f'【蓝莺Connector】AI Chat 返回空白内容, vendor:{vendor}, model:{model}',
            f'ai_chat_resp_failed_{vendor}')
        reply = '抱歉，我暂时无法回答你的问题。'
    tool_result.reply_ext['ai']['finish_reason'] = response.get(
        'finish_reason', '')
    command = None
    try:
        command = json.loads(reply)['ai']
    except Exception:
        pass
    if command is None:
        try:
            command = json.loads(reply)['lanying-connector']
        except Exception:
            pass
    if command:
        add_debug_message(
            config, f"收到如下JSON:\n{reply}", {'need_antispam_check': True})
        if 'preset_welcome' in command:
            reply = command['preset_welcome']
    return ReplyResult(
        reply, response.get('audio'), tool_result.reply_ext, command,
        tool_result.stream_msg_id, tool_result.is_stream,
        tool_result.stream_msg_last_send_time, tool_result.function_messages,
        tool_result.subsequent_messages)
