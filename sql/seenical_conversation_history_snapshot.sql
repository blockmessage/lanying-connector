-- Additive upgrade; run before deploying inherited conversation history.
CREATE TABLE IF NOT EXISTS `seenical_conversation_history_snapshot` (
  `app_id` varchar(100) NOT NULL,
  `seenical_session_id` varchar(128) NOT NULL,
  `source_session_id` varchar(128) NOT NULL,
  `bound_im_user_id` varchar(100) NOT NULL,
  `messages` longtext NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_id`,`seenical_session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
