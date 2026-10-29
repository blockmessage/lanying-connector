-- Connector Seenical MySQL (LANYING_AGENT_TOOLS_MYSQL_*), not PostgreSQL.
-- Existing grants belong to the legacy live balance/invitation flow.
-- Run once before upgrading Connector Web, Embedding and quota workers.
-- Fresh installations using seenical_gift_quota_mysql.sql already have this column.
-- If interrupted, SHOW COLUMNS FROM message_quota_gift LIKE 'payment_environment';
-- skip this ALTER if the column already exists. Do not rewrite historical grants.
ALTER TABLE message_quota_gift
  ADD COLUMN payment_environment VARCHAR(8) NOT NULL DEFAULT 'LIVE';
