import copy
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BotTask:
    chatbot_user_id: str
    config: dict


@dataclass(frozen=True)
class GroupChatbotRouteResult:
    handled: bool
    chatbot_user_id: Optional[str]
    mention_all: bool


@dataclass(frozen=True)
class GroupTargetResolution:
    chatbot_user_ids: list
    mention_all: bool


class TargetResolver:
    def __init__(self, resolve_user_ids, init_chatbot_config, safe_json_loads):
        self.resolve_user_ids = resolve_user_ids
        self.init_chatbot_config = init_chatbot_config
        self.safe_json_loads = safe_json_loads

    def resolve_group(self, config, msg):
        msg_config = self.safe_json_loads(msg.get('config')) or {}
        mention_all = msg_config.get(
            'mentionAll', msg_config.get('mention_all', False)) is True
        return GroupTargetResolution(
            chatbot_user_ids=self.resolve_user_ids(config, msg),
            mention_all=mention_all,
        )

    def build_bot_tasks(self, config, msg, chatbot_user_ids):
        tasks = []
        for chatbot_user_id in chatbot_user_ids:
            target_config = copy.deepcopy(config)
            self.init_chatbot_config(target_config, msg, chatbot_user_id)
            tasks.append(BotTask(
                chatbot_user_id=str(chatbot_user_id),
                config=target_config,
            ))
        return tasks
