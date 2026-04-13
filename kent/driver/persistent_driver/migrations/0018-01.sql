-- v17 → v18: Add resolution_type to errors for distinguishing resolution reasons
ALTER TABLE errors ADD COLUMN resolution_type TEXT;
