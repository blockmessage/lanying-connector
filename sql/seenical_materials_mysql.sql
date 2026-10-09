-- Apply to the Connector MySQL database before deploying any new workers.
-- Additive only: original files are retained after index reclamation.
CREATE TABLE IF NOT EXISTS seenical_material_space (
  app_id varchar(100) NOT NULL PRIMARY KEY,
  embedding_name varchar(255) NOT NULL,
  embedding_uuid varchar(100) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS seenical_material (
  app_id varchar(100) NOT NULL,
  doc_id varchar(100) NOT NULL,
  message_id varchar(128) NOT NULL,
  filename varchar(255) NOT NULL,
  object_name varchar(512) NOT NULL DEFAULT '',
  file_size bigint NOT NULL DEFAULT 0,
  source_url text NOT NULL,
  status varchar(32) NOT NULL DEFAULT 'pending',
  error_code varchar(64) NOT NULL DEFAULT '',
  worker_id varchar(128) NOT NULL DEFAULT '',
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (app_id, doc_id),
  UNIQUE KEY uq_material_message (app_id, message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS seenical_material_reference (
  app_id varchar(100) NOT NULL,
  owner_type varchar(16) NOT NULL,
  owner_id varchar(128) NOT NULL,
  doc_id varchar(100) NOT NULL,
  active tinyint NOT NULL DEFAULT 1,
  PRIMARY KEY (app_id, owner_type, owner_id, doc_id),
  KEY idx_material_references (app_id, doc_id, active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
