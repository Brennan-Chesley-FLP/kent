"""REST API endpoints for scraper discovery and information.

This module provides endpoints for:
- Listing available scrapers
- Getting scraper details and parameter schema
- Rescanning for scrapers
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from kent.driver.dev_driver.web.scraper_registry import (
    ScraperInfo,
    ScraperRegistry,
    get_registry,
)

router = APIRouter(prefix="/api/scrapers", tags=["scrapers"])


class FieldSchemaResponse(BaseModel):
    """Response model for a searchable field."""

    name: str
    filter_type: str
    description: str | None = None


class ModelSchemaResponse(BaseModel):
    """Response model for a data model's schema."""

    name: str
    fields: list[FieldSchemaResponse]


class SpeculativeStepResponse(BaseModel):
    """Response model for a speculative step."""

    name: str
    default_starting_id: int


class ScraperResponse(BaseModel):
    """Response model for scraper information."""

    module_path: str
    class_name: str
    full_path: str
    court_ids: list[str]
    court_url: str
    data_types: list[str]
    status: str
    version: str
    requires_auth: bool
    rate_limit_ms: int | None
    models: list[ModelSchemaResponse]
    speculative_steps: list[SpeculativeStepResponse]
    entry_schema: dict[str, Any] | None = None


class ScraperListResponse(BaseModel):
    """Response model for listing scrapers."""

    scrapers: list[ScraperResponse]
    total: int


class RescanResponse(BaseModel):
    """Response model for rescan operation."""

    discovered: int
    total: int


def _scraper_info_to_response(info: ScraperInfo) -> ScraperResponse:
    """Convert ScraperInfo to API response model."""
    return ScraperResponse(
        module_path=info.module_path,
        class_name=info.class_name,
        full_path=info.full_path,
        court_ids=list(info.court_ids),
        court_url=info.court_url,
        data_types=list(info.data_types),
        status=info.status,
        version=info.version,
        requires_auth=info.requires_auth,
        rate_limit_ms=info.rate_limit_ms,
        models=[],
        speculative_steps=[],
        entry_schema=info.entry_schema,
    )


@router.get("", response_model=ScraperListResponse)
async def list_scrapers(
    registry: Annotated[ScraperRegistry, Depends(get_registry)],
    status_filter: str | None = None,
) -> ScraperListResponse:
    """List all available scrapers.

    Args:
        status_filter: Optional filter by scraper status.

    Returns:
        List of scrapers with their metadata and parameter schemas.
    """
    scrapers = registry.list_scrapers()

    if status_filter:
        scrapers = [s for s in scrapers if s.status == status_filter]

    return ScraperListResponse(
        scrapers=[_scraper_info_to_response(s) for s in scrapers],
        total=len(scrapers),
    )


@router.get("/{scraper_path:path}", response_model=ScraperResponse)
async def get_scraper(
    scraper_path: str,
    registry: Annotated[ScraperRegistry, Depends(get_registry)],
) -> ScraperResponse:
    """Get details for a specific scraper.

    Args:
        scraper_path: Full scraper path (module.path:ClassName).

    Returns:
        Scraper details including parameter schema.

    Raises:
        HTTPException: 404 if scraper not found.
    """
    info = registry.get_scraper(scraper_path)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scraper '{scraper_path}' not found",
        )
    return _scraper_info_to_response(info)


@router.post("/rescan", response_model=RescanResponse)
async def rescan_scrapers(
    registry: Annotated[ScraperRegistry, Depends(get_registry)],
) -> RescanResponse:
    """Rescan for new scrapers.

    Scans the juriscraper/sd directory for BaseScraper subclasses.

    Returns:
        Number of scrapers discovered and total count.
    """
    from pathlib import Path

    # Get sd directory
    from kent.driver.dev_driver.web import (
        scraper_registry,
    )

    this_file = Path(scraper_registry.__file__)
    sd_directory = this_file.parent.parent.parent.parent.parent / "sd"

    discovered = 0
    if sd_directory.exists():
        discovered = registry.scan_directory(sd_directory, "juriscraper.sd")

    return RescanResponse(
        discovered=discovered,
        total=len(registry.list_scrapers()),
    )
