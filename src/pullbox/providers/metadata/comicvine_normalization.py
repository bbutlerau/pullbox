"""Normalize legacy ComicVine reads without inventing source identities."""

from datetime import date

from pullbox.core.issue_numbers import normalize_issue_number_text
from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.providers.base import IssueMetadata, SeriesMetadata
from pullbox.schemas.metadata_credits import parse_credits
from pullbox.schemas.metadata_sources import MetadataPage, ProviderIssueRead, ProviderSeriesRead
from pullbox.services.catalog.reader import CatalogIssueMetadata, CatalogSeriesMetadata
from pullbox.services.metadata_source_reads import source_id


def _text(value: str | None, limit: int = 500) -> str | None:
    if value is not None and (not isinstance(value, str) or len(value) > limit):
        raise ValueError("Invalid ComicVine text")
    return value


def _date(value: str | None) -> date | None:
    if not value:
        return None
    if not isinstance(value, str) or len(value) != 10:
        raise ValueError("Invalid ComicVine date")
    return date.fromisoformat(value)


def series(source: MetadataSource, row: SeriesMetadata, identifier: str) -> ProviderSeriesRead:
    from pullbox.providers.metadata.sources import _image_url

    identity = source_id(source, MetadataEntityKind.SERIES, row.provider_id)
    title = _text(row.title)
    if identity != identifier or not title or not title.strip():
        raise ValueError("Different ComicVine series")
    return ProviderSeriesRead(
        source=source,
        identity_namespace=source.identity_namespace,
        external_id=identity,
        title=title,
        sort_title=_text(row.sort_title),
        year_start=row.year_start,
        year_end=row.year_end,
        publisher=_text(row.publisher),
        issue_count=row.issue_count,
        description=_text(row.description, 20000),
        image_url=_image_url(row.cover_url),
        resource_url=f"https://comicvine.gamespot.com/volume/4050-{identity}/",
        status=row.status,
        source_updated_at=row.source_cutoff_at if isinstance(row, CatalogSeriesMetadata) else None,
    )


def issue(source: MetadataSource, row: IssueMetadata) -> ProviderIssueRead:
    from pullbox.providers.metadata.sources import _image_url

    identity = source_id(source, MetadataEntityKind.ISSUE, row.provider_id)
    parent = source_id(source, MetadataEntityKind.SERIES, row.series_provider_id)
    number = _text(row.issue_number_text, 320)
    if not number or not number.strip():
        raise ValueError("Missing ComicVine issue designation")
    try:
        key = normalize_issue_number_text(number)
    except ValueError:
        key = None
    return ProviderIssueRead(
        credits=parse_credits(
            [{"name": credit.get("name"), "role": credit.get("role")} for credit in row.creators]
        )
        if row.creators
        else None,
        source=source,
        identity_namespace=source.identity_namespace,
        external_id=identity,
        series_external_id=parent,
        issue_number_text=number,
        issue_number_key=key,
        title=_text(row.title),
        description=_text(row.description, 20000),
        cover_date=_date(row.release_date),
        store_date=_date(row.store_date),
        page_count=row.page_count,
        image_url=_image_url(row.cover_url),
        resource_url=f"https://comicvine.gamespot.com/issue/4000-{identity}/",
        source_updated_at=row.source_cutoff_at if isinstance(row, CatalogIssueMetadata) else None,
    )


def issue_page(
    source: MetadataSource, rows: list[IssueMetadata], total: int, page: int
) -> MetadataPage[ProviderIssueRead]:
    more = page * 100 < total
    return MetadataPage(
        results=[issue(source, row) for row in rows],
        total=total,
        next_page=page + 1 if more and page < 10000 else None,
        truncated=more and page == 10000,
    )
