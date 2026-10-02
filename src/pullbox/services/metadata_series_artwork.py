"""Representative Metron series artwork from an already-read first issue page."""

from collections.abc import Sequence

from pullbox.core.metadata_identity import MetadataSource
from pullbox.schemas.metadata_sources import ProviderIssueRead, ProviderSeriesRead
from pullbox.services.provider_artwork import allowed_artwork_url


def representative_series_cover(
    source: MetadataSource, external_id: str, issues: Sequence[ProviderIssueRead]
) -> str | None:
    if source is not MetadataSource.METRON_API or len(issues) > 100:
        return None
    if any(
        issue.source is not source
        or issue.identity_namespace is not source.identity_namespace
        or issue.series_external_id != external_id
        for issue in issues
    ):
        return None
    return next(
        (
            issue.image_url
            for issue in issues
            if issue.image_url and allowed_artwork_url(issue.image_url)
        ),
        None,
    )


def with_representative_cover(
    profile: ProviderSeriesRead, issues: Sequence[ProviderIssueRead]
) -> ProviderSeriesRead:
    if profile.image_url:
        return profile
    cover = representative_series_cover(profile.source, profile.external_id, issues)
    return profile.model_copy(update={"image_url": cover}) if cover else profile
