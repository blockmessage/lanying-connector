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
