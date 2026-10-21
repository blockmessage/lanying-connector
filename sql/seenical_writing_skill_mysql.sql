-- Additive upgrade; execute in Connector Seenical MySQL before deployment.
-- IDs are permanent identities, not credentials; never truncate/reseed this table.
CREATE TABLE IF NOT EXISTS `seenical_writing_skill` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `app_id` varchar(100) NOT NULL,
  `seenical_session_id` varchar(128) NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_writing_skill_session` (`app_id`,`seenical_session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
