-- Run once before upgrading Connector Web, Workers and Beat.
-- Existing ACTIVE/INACTIVE bindings and material references are preserved.
ALTER TABLE `seenical_conversation_binding`
  ADD COLUMN `deletion_task_id` varchar(128) DEFAULT NULL,
  ADD COLUMN `deletion_error` varchar(128) NOT NULL DEFAULT '',
  ADD UNIQUE KEY `uk_seenical_conversation_deleted_task` (`app_id`,`deletion_task_id`),
  ADD KEY `idx_seenical_conversation_cleanup` (`status`,`updated_at`);
