"""Explicit Add exclusions for unsupported GCD designations, not identity guessing."""

import hashlib
import json
from dataclasses import replace

from pullbox.core.issue_numbers import parse_issue_number_text
from pullbox.core.metadata_identity import MetadataSource
from pullbox.schemas.metadata_sources import (
    CatalogExcludedIssue,
    CatalogReviewRead,
    ProviderIssueRead,
)
from pullbox.services.metadata_series_adoption import (
    SeriesAdoptionError,
    SourceIssueBatch,
    SourceSeriesBundle,
    _validate_bundle,
    _validate_issue_batch,
)


def _entry(issue: ProviderIssueRead) -> CatalogExcludedIssue:
    return CatalogExcludedIssue(
        source=MetadataSource.GCD_LOCAL,
        series_external_id=issue.series_external_id,
        external_id=issue.external_id,
        issue_number_text=issue.issue_number_text,
    )


def catalog_review(bundle: SourceSeriesBundle) -> CatalogReviewRead | None:
    if bundle.series.source is not MetadataSource.GCD_LOCAL:
        return None
    supported, excluded = [], []
    for issue in bundle.issues:
        try:
            parse_issue_number_text(issue.issue_number_text)
        except ValueError:
            excluded.append(issue)
        else:
            supported.append(issue)
    if not excluded:
        _validate_bundle(bundle)
        return None
    # Counts, identity, parent and duplicate checks still apply to every entry.
    if supported:
        _validate_bundle(replace(bundle, issues=tuple(supported), excluded_issues=tuple(excluded)))
    else:
        _validate_issue_batch(
            SourceIssueBatch(
                bundle.series.source,
                bundle.series.external_id,
                bundle.issues,
                bundle.source_revision,
            ),
            allow_unsupported=True,
        )
    payload = {
        "series": bundle.series.model_dump(mode="json"),
        "source_revision": bundle.source_revision,
        "issues": [item.model_dump(mode="json") for item in bundle.issues],
    }
    # Content fingerprint binds consent to the fetched catalog, not authentication.
    token = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return CatalogReviewRead(
        token=token,
        total=bundle.catalog_total,
        supported_count=len(supported),
        excluded=tuple(_entry(item) for item in excluded),
    )


def approve_catalog_review(bundle: SourceSeriesBundle, token: str | None) -> SourceSeriesBundle:
    review = catalog_review(bundle)
    if review is None:
        if token is not None:
            raise SeriesAdoptionError("The catalog changed. Preview the series again.")
        return bundle
    if token != review.token or not review.supported_count:
        raise SeriesAdoptionError(
            "Review the unsupported catalog entries before adding this series."
        )
    return apply_catalog_exclusions(bundle, review.excluded)


def apply_catalog_exclusions(
    bundle: SourceSeriesBundle, saved: tuple[CatalogExcludedIssue, ...]
) -> SourceSeriesBundle:
    relevant = tuple(
        item
        for item in saved
        if item.source is bundle.series.source
        and item.series_external_id == bundle.series.external_id
    )
    if not relevant:
        return bundle
    by_id = {item.external_id: item for item in relevant}
    excluded = tuple(item for item in bundle.issues if item.external_id in by_id)
    if len(excluded) != len(relevant) or any(
        _entry(item) != by_id[item.external_id] for item in excluded
    ):
        raise SeriesAdoptionError(
            "Previously reviewed GCD entries changed. Review the catalog before retrying."
        )
    result = replace(
        bundle,
        issues=tuple(item for item in bundle.issues if item.external_id not in by_id),
        excluded_issues=excluded,
    )
    _validate_bundle(result)
    return result
