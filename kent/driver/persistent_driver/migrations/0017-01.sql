-- v16 → v17: Speculative protocol — add param_index and template_json to speculation_tracking
ALTER TABLE speculation_tracking ADD COLUMN param_index INTEGER DEFAULT 0;
ALTER TABLE speculation_tracking ADD COLUMN template_json TEXT;
