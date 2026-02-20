"""Archive handler for LocalDevDriver web interface.

This module provides a custom archive callback that saves downloaded files
using UUID4 filenames while preserving the original file extension.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4


async def uuid_archive_callback(
    content: bytes,
    url: str,
    expected_type: str | None,
    storage_dir: Path,
) -> str:
    """Archive callback that uses UUID4 filenames with preserved extensions.

    This callback generates a unique UUID4 filename for each downloaded file,
    preserving the file extension from the URL if available, or inferring it
    from the expected_type hint.

    Args:
        content: The binary file content.
        url: The URL the file was downloaded from.
        expected_type: Optional hint about the file type ("pdf", "audio", etc.).
        storage_dir: Directory where files should be saved.

    Returns:
        The local file path where the file was saved.
    """
    # Generate UUID4 for the filename
    file_uuid = uuid4()

    # Try to extract extension from URL
    parsed_url = urlparse(url)
    url_path = Path(parsed_url.path)
    extension = url_path.suffix.lower() if url_path.suffix else ""

    # If no extension from URL, try to infer from expected_type
    if not extension and expected_type:
        # Map expected_type hints to extensions
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

    # Construct filename
    filename = f"{file_uuid}{extension}"
    file_path = storage_dir / filename

    # Ensure storage directory exists
    storage_dir.mkdir(parents=True, exist_ok=True)

    # Write the file
    file_path.write_bytes(content)

    return str(file_path)


def get_storage_dir_for_run(runs_dir: Path, run_id: str) -> Path:
    """Get the storage directory for archived files for a specific run.

    Args:
        runs_dir: The base runs directory.
        run_id: The run identifier.

    Returns:
        Path to the files directory for this run: runs/{run_id}/files/
    """
    return runs_dir / run_id / "files"
