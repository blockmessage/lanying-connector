-- Connector Seenical MySQL, not PostgreSQL. Apply before enabling workspace sync.
-- Existing tables are preserved. No original files, references or usage are removed.
-- Existing installations first apply seenical_workspace_sync_migration.sql.
-- Fresh installations use seenical_materials_mysql.sql (already includes its columns).

CREATE TABLE IF NOT EXISTS seenical_workspace (
  app_id varchar(100) NOT NULL,
  workspace_id varchar(64) NOT NULL PRIMARY KEY,
  layout_version int NOT NULL DEFAULT 2,
  repository varchar(255) NOT NULL,
  repository_id varchar(64) NOT NULL,
  branch varchar(128) NOT NULL,
  directory varchar(512) NOT NULL,
  location_hash char(64) NOT NULL,
  github_token text NOT NULL,
  actor_json text NOT NULL,
  scopes_json mediumtext NOT NULL,
  paused tinyint NOT NULL DEFAULT 0,
  status varchar(32) NOT NULL DEFAULT 'pending',
  error_code varchar(64) NOT NULL DEFAULT '',
  manifest_sha varchar(40) NOT NULL DEFAULT '',
  config_sha varchar(40) NOT NULL DEFAULT '',
  warnings_json text DEFAULT NULL,
  last_synced_at datetime DEFAULT NULL,
  next_check_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  requested_generation bigint NOT NULL DEFAULT 1,
  completed_generation bigint NOT NULL DEFAULT 0,
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_workspace_location (location_hash),
  KEY idx_workspace_app (app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS seenical_workspace_file (
  app_id varchar(100) NOT NULL,
  workspace_id varchar(64) NOT NULL,
  skill_id varchar(64) NOT NULL,
  path_hash char(64) NOT NULL,
  path varchar(768) NOT NULL,
  source_type varchar(32) NOT NULL,
  source_id varchar(128) NOT NULL,
  doc_id varchar(100) NOT NULL,
  base_sha varchar(40) NOT NULL DEFAULT '',
  source_fingerprint varchar(128) NOT NULL DEFAULT '',
  server_sha varchar(40) NOT NULL DEFAULT '',
  remote_sha varchar(40) NOT NULL DEFAULT '',
  status varchar(32) NOT NULL DEFAULT 'pending',
  error_code varchar(64) NOT NULL DEFAULT '',
  decision_json text DEFAULT NULL,
  removed tinyint NOT NULL DEFAULT 0,
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (workspace_id, skill_id, path_hash),
  UNIQUE KEY uq_workspace_document (workspace_id, skill_id, source_type, doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS seenical_workspace_partition (
  app_id varchar(100) NOT NULL,
  skill_id varchar(64) NOT NULL,
  workspace_id varchar(64) NOT NULL,
  seenical_session_id varchar(128) NOT NULL,
  knowledge_base_ids mediumtext NOT NULL,
  warnings_json text DEFAULT NULL,
  active tinyint NOT NULL DEFAULT 1,
  paused tinyint NOT NULL DEFAULT 0,
  manifest_sha varchar(40) NOT NULL DEFAULT '',
  status varchar(32) NOT NULL DEFAULT 'pending',
  error_code varchar(64) NOT NULL DEFAULT '',
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (workspace_id, skill_id),
  KEY idx_partition_skill (app_id, skill_id, active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One recoverable candidate per document, not a user-facing version history.
CREATE TABLE IF NOT EXISTS seenical_material_replacement (
  app_id varchar(100) NOT NULL,
  doc_id varchar(100) NOT NULL,
  replacement_id varchar(64) NOT NULL,
  physical_id varchar(100) NOT NULL,
  previous_physical_id varchar(100) NOT NULL,
  object_name varchar(512) NOT NULL,
  filename varchar(255) NOT NULL,
  file_size bigint NOT NULL,
  old_usage bigint NOT NULL,
  new_usage bigint NOT NULL DEFAULT 0,
  content_sha varchar(40) NOT NULL,
  status varchar(32) NOT NULL DEFAULT 'pending',
  error_code varchar(64) NOT NULL DEFAULT '',
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (app_id, doc_id),
  UNIQUE KEY uq_replacement_id (replacement_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
