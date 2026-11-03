-- Apply to Connector Seenical MySQL before upgrading all Web/Embedding/Workers.
CREATE TABLE IF NOT EXISTS seenical_stripe_entitlement (
  app_id varchar(64) NOT NULL,
  environment varchar(16) NOT NULL,
  revision bigint NOT NULL,
  value_json mediumtext NOT NULL,
  created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
