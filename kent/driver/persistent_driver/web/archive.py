"""Archive handler for the persistent driver web interface.

Provides UuidAsyncArchiveHandler which saves downloaded files using
SHA-256 content-hash filenames while preserving the original file extension.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlparse

from kent.data_types import ArchiveDecision


def get_storage_dir_for_run(runs_dir: Path, run_id: str) -> Path:
    """Get the storage directory for archived files for a specific run."""
    return runs_dir / run_id / "files"


class UuidAsyncArchiveHandler:
    """Archive handler using SHA-256 content-hash filenames.

    Used by the persistent driver web interface. Always downloads
    (no skip logic), and names files by their content hash.
    """

    def __init__(self, storage_dir: Path) -> None:
        self.storage_dir = storage_dir

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        return ArchiveDecision(download=True)

    async def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str:
        content_hash = hashlib.sha256(content).hexdigest()

        # Try to extract extension from URL
        parsed_url = urlparse(url)
        url_path = Path(parsed_url.path)
        extension = url_path.suffix.lower() if url_path.suffix else ""

        # If no extension from URL, try to infer from expected_type
        if not extension and expected_type:
            type_to_extension = {
                "pdf": ".pdf",
                "audio": ".mp3",
                "mp3": ".mp3",
                "wav": ".wav",
                "image": ".jpg",
                "jpg": ".jpg",
                "jpeg": ".jpg",
                "png": ".png",
                "gif": ".gif",
                "html": ".html",
                "json": ".json",
                "xml": ".xml",
                "text": ".txt",
                "csv": ".csv",
            }
            extension = type_to_extension.get(expected_type.lower(), "")

        filename = f"{content_hash}{extension}"
        file_path = self.storage_dir / filename

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)

        return str(file_path)
