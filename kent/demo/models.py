"""Pydantic data models for the Bug Civil Court demo scraper."""

from __future__ import annotations

from datetime import date

from pydantic import Field

from kent.common.data_models import ScrapedData


class CaseData(ScrapedData):
    """A case filed in the Bug Civil Court."""

    docket: str = Field(..., description="Docket number, e.g. BCC-2024-001")
    case_name: str = Field(..., description="Full case name")
    plaintiff: str = Field(..., description="Plaintiff name")
    defendant: str = Field(..., description="Defendant name")
    date_filed: date = Field(..., description="Date the case was filed")
    case_type: str = Field(..., description="Type of case")
    status: str = Field(..., description="Current status")
    judge: str = Field(..., description="Presiding judge")
    summary: str = Field(..., description="Case summary")


class JusticeData(ScrapedData):
    """A justice of the Bug Civil Court."""

    name: str = Field(..., description="Full name with honorific")
    insect_species: str = Field(..., description="Species name")
    title: str = Field(..., description="Judicial title")
    appointed_date: date = Field(..., description="Date appointed")
    bio: str = Field(..., description="Biographical text")
    image_url: str = Field(..., description="Portrait image URL")


class OralArgumentData(ScrapedData):
    """An oral argument recording from the Bug Civil Court."""

    docket: str = Field(..., description="Docket number")
    case_name: str = Field(..., description="Case name")
    audio_url: str = Field(..., description="URL of the audio recording")
    local_path: str | None = Field(
        None, description="Local archived file path"
    )


class OpinionData(ScrapedData):
    """An opinion document (insect illustration) from the Bug Civil Court."""

    docket: str = Field(..., description="Docket number")
    case_name: str = Field(..., description="Case name")
    image_url: str = Field(..., description="URL of the opinion image")
    local_path: str | None = Field(
        None, description="Local archived file path"
    )
