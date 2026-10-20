-- Existing Connector Seenical MySQL only, once. This changes metadata, not originals or vectors.
-- Fresh seenical_materials_mysql.sql already contains these columns: do not run twice.
ALTER TABLE seenical_material
  ADD COLUMN index_doc_id varchar(100) NOT NULL DEFAULT '',
  ADD COLUMN replacement_id varchar(64) NOT NULL DEFAULT '';
-- Next execute seenical_workspace_sync_mysql.sql for the three new tables.
