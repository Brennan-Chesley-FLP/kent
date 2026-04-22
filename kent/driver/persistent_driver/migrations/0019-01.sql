-- v18 → v19: Drop unused warc_record_id column (WARC export feature removed)
ALTER TABLE requests DROP COLUMN warc_record_id;
