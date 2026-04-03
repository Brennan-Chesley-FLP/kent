"""Archive handler protocols and implementations.

Provides the ArchiveHandler protocol (sync and async variants) and concrete
implementations for different archive strategies:

- NoDownloads: skip all downloads (replaces skip_archive=True)
- Local: save files to a local directory (replaces default_archive_callback)
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from kent.data_types import ArchiveDecision


class SyncArchiveHandler(Protocol):
    """Protocol for synchronous archive handlers."""

    def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision: ...

    def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str: ...


class AsyncArchiveHandler(Protocol):
    """Protocol for asynchronous archive handlers."""

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision: ...

    async def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str: ...


def _filename_from_url(url: str, expected_type: str | None) -> str:
    """Extract a filename from a URL, or generate one from expected_type."""
    parsed_url = urlparse(url)
    path_parts = Path(parsed_url.path).parts
    valid_parts = [p for p in path_parts if p and p not in (".", "/")]

    if valid_parts:
        return valid_parts[-1]

    ext = {"pdf": ".pdf", "audio": ".mp3"}.get(expected_type or "", "")
    return f"download_{hash(url)}{ext}"


class NoDownloadsSyncArchiveHandler:
    """Always skips downloads. Replaces skip_archive=True for SyncDriver."""

    def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        return ArchiveDecision(download=False, file_url="skipped")

    def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str:
        return "skipped"


class NoDownloadsAsyncArchiveHandler:
    """Always skips downloads. Replaces skip_archive=True for AsyncDriver."""

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        return ArchiveDecision(download=False, file_url="skipped")

    async def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str:
        return "skipped"


class LocalSyncArchiveHandler:
    """Saves files to a local directory. Replaces default_archive_callback."""

    def __init__(self, storage_dir: Path) -> None:
        self.storage_dir = storage_dir

    def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        if deduplication_key:
            dedup_dir = self.storage_dir / deduplication_key
            if dedup_dir.is_dir() and any(dedup_dir.iterdir()):
                existing = next(dedup_dir.iterdir())
                return ArchiveDecision(download=False, file_url=str(existing))
        return ArchiveDecision(download=True)

    def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str:
        filename = _filename_from_url(url, expected_type)
        if deduplication_key:
            target_dir = self.storage_dir / deduplication_key
            target_dir.mkdir(parents=True, exist_ok=True)
            file_path = target_dir / filename
        else:
            file_path = self.storage_dir / filename
        file_path.write_bytes(content)
        return str(file_path)


class LocalAsyncArchiveHandler:
    """Saves files to a local directory. Async variant for AsyncDriver."""

    def __init__(self, storage_dir: Path) -> None:
        self.storage_dir = storage_dir

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        if deduplication_key:
            dedup_dir = self.storage_dir / deduplication_key
            if dedup_dir.is_dir() and any(dedup_dir.iterdir()):
                existing = next(dedup_dir.iterdir())
                return ArchiveDecision(download=False, file_url=str(existing))
        return ArchiveDecision(download=True)

    async def save(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        content: bytes,
    ) -> str:
        filename = _filename_from_url(url, expected_type)
        if deduplication_key:
            target_dir = self.storage_dir / deduplication_key
            target_dir.mkdir(parents=True, exist_ok=True)
            file_path = target_dir / filename
        else:
            file_path = self.storage_dir / filename
        file_path.write_bytes(content)
        return str(file_path)
