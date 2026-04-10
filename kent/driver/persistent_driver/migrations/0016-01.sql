-- v15 → v16: Deduplicated storage table for incidental request content
CREATE TABLE IF NOT EXISTS incidental_request_storage (
    id INTEGER PRIMARY KEY,
    resource_type TEXT NOT NULL,
    url TEXT NOT NULL,
    method TEXT NOT NULL,
    body BLOB,
    status_code INTEGER,
    response_headers_json TEXT,
    content_compressed BLOB,
    content_size_original INTEGER,
    content_size_compressed INTEGER,
    compression_dict_id INTEGER REFERENCES compression_dicts(id),
    failure_reason TEXT,
    content_md5 TEXT
);

CREATE INDEX IF NOT EXISTS idx_irs_content_md5 ON incidental_request_storage(content_md5);

ALTER TABLE incidental_requests ADD COLUMN storage_id INTEGER REFERENCES incidental_request_storage(id);

CREATE INDEX IF NOT EXISTS idx_incidental_requests_storage ON incidental_requests(storage_id);
