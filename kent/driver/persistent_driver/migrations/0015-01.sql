-- v14 → v15: Add browser cookies JSON to run metadata
ALTER TABLE run_metadata ADD COLUMN browser_cookies_json TEXT;
