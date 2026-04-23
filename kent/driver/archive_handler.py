"""Archive handler protocols and implementations.

Provides the ArchiveHandler protocol (sync and async variants) and concrete
implementations for different archive strategies:

- NoDownloads: skip all downloads (replaces skip_archive=True)
- Local: save files to a local directory (replaces default_archive_callback)
- LocalStreaming: like Local, but writes incoming chunks straight to disk
  without buffering the full file in memory.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from kent.data_types import ArchiveDecision


def _streaming_target_path(
    storage_dir: Path,
    deduplication_key: str | None,
    sha_hex: str,
    expected_type: str | None,
) -> Path:
    """Assemble the final target path for a streamed download.

    Layout: ``{storage_dir}/{deduplication_key}/{shasum}.{expected_type}``
    (the ``deduplication_key`` segment is omitted when it's ``None`` and
    the ``.{expected_type}`` suffix is omitted when ``expected_type`` is
    ``None``). The parent directory is created as a side effect.
    """
    target_dir = (
        storage_dir / deduplication_key if deduplication_key else storage_dir
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{sha_hex}.{expected_type}" if expected_type else sha_hex
    return target_dir / filename


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


def _existing_dedup_file(dedup_dir: Path) -> Path | None:
    """Return the first file in ``dedup_dir`` if it exists and is non-empty."""
    if not dedup_dir.is_dir():
        return None
    try:
        return next(iter(dedup_dir.iterdir()))
    except StopIteration:
        return None


def _write_local_file(
    storage_dir: Path,
    deduplication_key: str | None,
    filename: str,
    content: bytes,
) -> Path:
    """Write ``content`` under the local storage directory and return the path."""
    if deduplication_key:
        target_dir = storage_dir / deduplication_key
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename
    else:
        file_path = storage_dir / filename
    file_path.write_bytes(content)
    return file_path


def _hash_and_write(sha: Any, tmp: Any, chunk: bytes) -> None:
    """Update a running hash and write ``chunk`` in a single thread dispatch."""
    sha.update(chunk)
    tmp.write(chunk)


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
            existing = _existing_dedup_file(dedup_dir)
            if existing is not None:
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
        file_path = _write_local_file(
            self.storage_dir, deduplication_key, filename, content
        )
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
            existing = await asyncio.to_thread(_existing_dedup_file, dedup_dir)
            if existing is not None:
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
        file_path = await asyncio.to_thread(
            _write_local_file,
            self.storage_dir,
            deduplication_key,
            filename,
            content,
        )
        return str(file_path)


class SyncStreamingArchiveHandler(Protocol):
    """Protocol for synchronous streaming archive handlers.

    Unlike :class:`SyncArchiveHandler`, this variant receives the downloaded
    bytes as an iterator of chunks so the handler can persist them without
    ever buffering the whole file in memory.
    """

    def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision: ...

    def save_stream(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        chunks: Iterator[bytes],
    ) -> str: ...


class AsyncStreamingArchiveHandler(Protocol):
    """Protocol for asynchronous streaming archive handlers."""

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision: ...

    async def save_stream(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        chunks: AsyncIterator[bytes],
    ) -> str: ...


class LocalSyncStreamingArchiveHandler:
    """Streams downloaded bytes to a local directory.

    Behaves like :class:`LocalSyncArchiveHandler` but writes chunks straight
    to disk instead of accepting a fully-buffered ``bytes`` payload. The
    default filename is content-addressed:
    ``{storage_dir}/{deduplication_key}/{sha256}.{expected_type}``. Bytes
    stream into a temp file alongside the final destination so the rename
    is atomic once the full SHA-256 is known.
    """

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
            existing = _existing_dedup_file(dedup_dir)
            if existing is not None:
                return ArchiveDecision(download=False, file_url=str(existing))
        return ArchiveDecision(download=True)

    def save_stream(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        chunks: Iterator[bytes],
    ) -> str:
        target_dir = (
            self.storage_dir / deduplication_key
            if deduplication_key
            else self.storage_dir
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        sha = hashlib.sha256()
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115 (rename-then-close)
            dir=target_dir, delete=False, prefix=".stream-", suffix=".tmp"
        )
        try:
            with tmp as f:
                for chunk in chunks:
                    sha.update(chunk)
                    f.write(chunk)
            final_path = _streaming_target_path(
                self.storage_dir,
                deduplication_key,
                sha.hexdigest(),
                expected_type,
            )
            os.replace(tmp.name, final_path)
        except BaseException:
            # Best-effort cleanup on any error so we don't leave .tmp
            # files behind.
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise
        return str(final_path)


class LocalAsyncStreamingArchiveHandler:
    """Async counterpart of :class:`LocalSyncStreamingArchiveHandler`.

    Same content-addressed filename scheme:
    ``{storage_dir}/{deduplication_key}/{sha256}.{expected_type}``.
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
        if deduplication_key:
            dedup_dir = self.storage_dir / deduplication_key
            existing = await asyncio.to_thread(_existing_dedup_file, dedup_dir)
            if existing is not None:
                return ArchiveDecision(download=False, file_url=str(existing))
        return ArchiveDecision(download=True)

    async def save_stream(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        chunks: AsyncIterator[bytes],
    ) -> str:
        target_dir = (
            self.storage_dir / deduplication_key
            if deduplication_key
            else self.storage_dir
        )
        await asyncio.to_thread(target_dir.mkdir, parents=True, exist_ok=True)

        sha = hashlib.sha256()
        tmp = await asyncio.to_thread(
            tempfile.NamedTemporaryFile,  # noqa: SIM115 (rename-then-close)
            dir=target_dir,
            delete=False,
            prefix=".stream-",
            suffix=".tmp",
        )
        try:
            try:
                async for chunk in chunks:
                    await asyncio.to_thread(_hash_and_write, sha, tmp, chunk)
            finally:
                await asyncio.to_thread(tmp.close)
            final_path = await asyncio.to_thread(
                _streaming_target_path,
                self.storage_dir,
                deduplication_key,
                sha.hexdigest(),
                expected_type,
            )
            await asyncio.to_thread(os.replace, tmp.name, final_path)
        except BaseException:
            # Best-effort cleanup — use sync os.unlink so this stays safe
            # under cancellation (an await here could itself be cancelled).
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise
        return str(final_path)
