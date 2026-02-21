Step 11: Archive Event Hook
=============================

In Step 4, we introduced the archive request pattern (``Request`` with ``archive=True``) for downloading and saving
files locally. The driver included a ``save_file`` method that handled the file
archival logic. In Step 7, we introduced the ``on_data`` callback pattern to allow
customization of data handling.

Now in Step 13, we extend the callback pattern to file archival by introducing the
**on_archive callback hook**. This allows users to customize how files are saved,
where they're stored, and what file paths are used.


Overview
--------

In this step, we introduce:

1. **default_archive_callback** - Default implementation for saving files
2. **on_archive parameter** - Optional callback in SyncDriver.__init__()
3. **Callback signature** - Receives content, url, expected_type, and storage_dir
4. **Custom archival logic** - Users can implement custom file saving strategies


Why an Archive Callback?
-------------------------

Different use cases require different file archival strategies:

**Custom Storage Backends**:

- Save to S3 or cloud storage instead of local filesystem
- Store files in a database as BLOBs
- Upload to external archival systems

**Custom Naming Strategies**:

- Use content hashes for deduplication
- Implement hierarchical directory structures
- Add timestamps or metadata to filenames

**Validation and Processing**:

- Compute checksums before saving
- Verify file integrity
- Extract metadata or thumbnails

**No-Op Archival**:

- Skip saving files during testing
- Record file metadata without storing content
- Implement dry-run mode


Default Archive Callback
-------------------------

The ``default_archive_callback`` function provides a standard file saving behavior:

.. code-block:: python

    from kent.driver.sync_driver import (
        default_archive_callback,
    )

    # Default behavior: extract filename from URL, save to storage_dir
    file_url = default_archive_callback(
        content=b"file content",
        url="http://example.com/files/document.pdf",
        expected_type="pdf",
        storage_dir=Path("/tmp/downloads"),
    )

**Behavior:**

- **Extracts filename from URL** - Uses last path component as filename
- **Generates filename** - If URL has no path, generates ``download_{hash(url)}.{ext}``
- **Uses expected_type** - Maps "pdf" to .pdf, "audio" to .mp3
- **Saves to storage_dir** - Writes bytes to file in storage directory
- **Returns file path** - Returns absolute path as string


Implementation
--------------

The archive callback is called in ``resolve_archive_request``:

.. code-block:: python

    def resolve_archive_request(
        self, request: Request
    ) -> ArchiveResponse:
        """Fetch an archive request and save the file using on_archive callback."""
        http_response = self.resolve_request(request)

        # Step 13: Use on_archive callback to save the file
        file_url = self.on_archive(
            http_response.content,
            request.request.url,
            request.expected_type,
            self.storage_dir,
        )

        return ArchiveResponse(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=request.request.url,
            request=request,
            file_url=file_url,
        )

**Callback Signature:**

.. code-block:: python

    def on_archive(
        content: bytes,
        url: str,
        expected_type: str | None,
    ) -> str:
        """Archive a downloaded file.

        Args:
            content: The binary file content.
            url: The URL the file was downloaded from.
            expected_type: Optional hint about the file type ("pdf", "audio").
            storage_dir: Directory configured for file storage.

        Returns:
            The file path (local or remote) where the file was saved.
        """
        ...


Next Steps
----------

In :doc:`12_lifecycle_hooks`, we introduce lifecycle hooks (on_run_start and
on_run_complete) for tracking scraper runs. These callbacks fire at the
beginning and end of each run, providing visibility into scraper lifecycle
for monitoring and metrics.
