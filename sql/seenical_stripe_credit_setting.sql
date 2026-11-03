
CREATE TABLE IF NOT EXISTS seenical_stripe_credit_setting (
  app_id varchar(64) NOT NULL,
  environment varchar(16) NOT NULL,
  revision bigint NOT NULL,
  auto_recharge tinyint(1) NOT NULL DEFAULT 0,
  created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
