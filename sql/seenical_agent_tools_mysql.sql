CREATE TABLE IF NOT EXISTS `agent_tool_audit_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `app_id` varchar(100) NOT NULL DEFAULT '',
  `request_id` varchar(128) NOT NULL DEFAULT '',
  `event` varchar(100) NOT NULL DEFAULT '',
  `chatbot_id` varchar(100) NOT NULL DEFAULT '',
  `conversation_type` varchar(32) NOT NULL DEFAULT '',
  `conversation_id` varchar(100) NOT NULL DEFAULT '',
  `actor_subject_id` varchar(100) NOT NULL DEFAULT '',
  `tool_id` varchar(255) NOT NULL DEFAULT '',
  `tool_version` int NOT NULL DEFAULT 0,
  `skill_versions` longtext NOT NULL,
  `arguments_hash` varchar(128) NOT NULL DEFAULT '',
  `result_status` varchar(100) NOT NULL DEFAULT '',
  `diff_summary` longtext NOT NULL,
  `extra_metadata` longtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_agent_tool_audit_request_created` (`request_id`,`created_at`),
  KEY `idx_agent_tool_audit_app_created` (`app_id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `public_skill_catalog_revision` (
  `revision` varchar(128) NOT NULL,
  `source_commit` varchar(64) NOT NULL,
  `manifest_sha` varchar(128) NOT NULL,
  `catalog` longtext NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`revision`),
  KEY `idx_public_skill_catalog_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `public_skill_catalog_state` (
  `state_key` varchar(32) NOT NULL,
  `active_revision` varchar(128) NOT NULL,
  `source_commit` varchar(64) NOT NULL,
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`state_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `public_skill_revision` (
  `skill_id` varchar(100) NOT NULL,
  `revision` varchar(128) NOT NULL,
  `source_commit` varchar(64) NOT NULL,
  `skill` longtext NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`skill_id`,`revision`),
  KEY `idx_public_skill_revision_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `seenical_config_revision` (
  `app_id` varchar(100) NOT NULL,
  `resource_type` varchar(32) NOT NULL,
  `resource_id` varchar(128) NOT NULL,
  `revision` bigint NOT NULL,
  `snapshot` longtext NOT NULL,
  `request_id` varchar(128) NOT NULL DEFAULT '',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_id`,`resource_type`,`resource_id`,`revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `seenical_conversation_binding` (
  `app_id` varchar(100) NOT NULL,
  `seenical_session_id` varchar(128) NOT NULL,
  `chatbot_id` varchar(100) NOT NULL,
  `agent_user_id` varchar(100) NOT NULL,
  `conversation_type` varchar(32) NOT NULL,
  `conversation_id` varchar(100) NOT NULL,
  `conversation_name` varchar(255) NOT NULL DEFAULT '',
  `task_id` varchar(128) DEFAULT NULL,
  `bound_im_user_id` varchar(100) NOT NULL,
  `status` varchar(32) NOT NULL DEFAULT 'ACTIVE',
  `revision` bigint NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_id`,`seenical_session_id`),
  UNIQUE KEY `uk_seenical_conversation_target`
    (`app_id`,`conversation_type`,`conversation_id`),
  UNIQUE KEY `uk_seenical_conversation_task` (`app_id`,`task_id`),
  KEY `idx_seenical_conversation_chatbot` (`app_id`,`chatbot_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `message_quota_usage_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `app_id` varchar(100) NOT NULL DEFAULT '',
  `quota` decimal(20,6) NOT NULL DEFAULT 0,
  `model_type` varchar(100) NOT NULL DEFAULT '',
  `vendor` varchar(100) NOT NULL DEFAULT '',
  `model` varchar(255) NOT NULL DEFAULT '',
  `api_key_type` varchar(100) NOT NULL DEFAULT '',
  `message_count` int NOT NULL DEFAULT 1,
  `total_tokens` int NOT NULL DEFAULT 0,
  `prompt_tokens` int NOT NULL DEFAULT 0,
  `completion_tokens` int NOT NULL DEFAULT 0,
  `text_size` int NOT NULL DEFAULT 0,
  `content_security` varchar(100) NOT NULL DEFAULT '',
  `product_id` bigint NOT NULL DEFAULT 0,
  `extra_metadata` longtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_message_quota_app_created` (`app_id`,`created_at`),
  KEY `idx_message_quota_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `openclaw_session_map_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `app_id` varchar(100) NOT NULL DEFAULT '',
  `node_id` varchar(100) NOT NULL DEFAULT '',
  `session_key` text NOT NULL,
  `group_id` varchar(100) NOT NULL DEFAULT '',
  `openclaw_user_id` varchar(100) NOT NULL DEFAULT '',
  `change_source` varchar(100) NOT NULL DEFAULT '',
  `previous_signature` longtext NOT NULL,
  `new_signature` longtext NOT NULL,
  `previous_mapping` longtext NOT NULL,
  `new_mapping` longtext NOT NULL,
  `legacy_session_keys` longtext NOT NULL,
  `extra_metadata` longtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_openclaw_map_app_node_created` (`app_id`,`node_id`,`created_at`),
  KEY `idx_openclaw_map_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
