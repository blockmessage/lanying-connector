-- Additional tables for package-first accounting and durable preference projection.
-- Run together with 20261014_message_credits.sql before enabling purchases.
CREATE TABLE IF NOT EXISTS message_quota_package_usage (
  statistic_key VARCHAR(255) NOT NULL PRIMARY KEY,
  used DECIMAL(20,4) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS message_quota_settings (
  app_id VARCHAR(64) NOT NULL PRIMARY KEY,
  auto_recharge TINYINT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
