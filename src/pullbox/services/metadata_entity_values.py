"""Shared descriptive projection, separate from identity and catalog lifecycle."""

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models import Issue, Series
from pullbox.models.series import SeriesStatus, SeriesType
from pullbox.schemas.metadata_snapshot import MetadataValues
from pullbox.services.metadata_service import MetadataService


async def apply_series_metadata_values(
    session: AsyncSession, series: Series, values: MetadataValues
) -> None:
    series.title = values.title or series.title
    series.sort_title = values.sort_title or series.sort_title
    series.description, series.year_start, series.year_end = (
        values.description,
        values.year_start,
        values.year_end,
    )
    series.cover_url = values.image_url
    series.publisher_id = (
        await MetadataService._ensure_publisher(session, values.publisher)
        if values.publisher
        else None
    )
    if values.series_type is not None:
        series.series_type = SeriesType(values.series_type)
    if values.status is not None:
        series.status = SeriesStatus(values.status)


def apply_issue_metadata_values(issue: Issue, values: MetadataValues) -> None:
    issue.title, issue.description = values.title, values.description
    issue.release_date, issue.store_date = values.cover_date, values.store_date
    issue.page_count, issue.cover_url = values.page_count, values.image_url
