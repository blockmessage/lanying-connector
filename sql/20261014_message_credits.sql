-- Run once before upgrading every quota-checking/settlement process.
-- If interrupted, check SHOW COLUMNS and skip any column already added.
ALTER TABLE message_quota_gift ADD COLUMN credit_source VARCHAR(16) NOT NULL DEFAULT 'invitation';
ALTER TABLE message_quota_gift_usage ADD COLUMN purchase_used DECIMAL(20,4) NOT NULL DEFAULT 0;
ALTER TABLE message_quota_gift_offset ADD COLUMN purchase_used DECIMAL(20,4) NOT NULL DEFAULT 0;
