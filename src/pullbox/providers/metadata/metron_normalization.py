"""Validate Metron serializer shapes before exposing shared metadata DTOs."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast
from urllib.parse import unquote, urlsplit

from pullbox.core.html_sanitizer import sanitize_rich_html
from pullbox.core.issue_numbers import normalize_issue_number_text
from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.schemas.metadata_sources import (
    ProviderIssueRead,
    ProviderSeriesRead,
    ProviderStoryArcRead,
)


def object_row(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("Expected a metadata object")
    return cast("dict[str, object]", value)


def external_id(value: object) -> str:
    if type(value) not in {int, str}:
        raise ValueError("Expected a positive provider identity")
    return ExternalIdentityRef(
        IdentityNamespace.METRON, MetadataEntityKind.SERIES, str(value)
    ).external_id


def _text(value: object, *, required: bool = False, limit: int = 500) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or len(value) > limit or (required and not value.strip()):
        raise ValueError("Expected bounded metadata text")
    return value.strip() or None


def _integer(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= 1_000_000_000:
        raise ValueError("Expected a nonnegative metadata integer")
    return value


def _date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 10:
        raise ValueError("Expected an ISO calendar date")
    return date.fromisoformat(value)


def _updated(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 48:
        raise ValueError("Expected a source timestamp")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Source timestamp must include its timezone")
    return result.astimezone(UTC)


def _url(value: object, path: str, *, image: bool = False) -> str | None:
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        url = urlsplit(value)
        decoded = unquote(url.path)
        if (
            url.scheme == "https"
            and url.hostname
            in ({"metron.cloud", "static.metron.cloud"} if image else {"metron.cloud"})
            and url.port in {None, 443}
            and url.username is None
            and url.password is None
            and not url.query
            and not url.fragment
            and decoded.startswith(path)
            and ".." not in decoded.split("/")
            and "\\" not in decoded
            and not any(char.isspace() or ord(char) < 32 for char in value + decoded)
        ):
            return value
    except ValueError:
        pass
    return None


def _crosswalks(row: dict[str, object], kind: MetadataEntityKind) -> list[ExternalIdentityRef]:
    identities = []
    for field, namespace in (
        ("cv_id", IdentityNamespace.COMICVINE),
        ("gcd_id", IdentityNamespace.GCD),
    ):
        if row.get(field) is not None:
            identities.append(ExternalIdentityRef(namespace, kind, external_id(row[field])))
    return identities


def _description(row: dict[str, object]) -> str | None:
    value = _text(row.get("desc"), limit=20000)
    return sanitize_rich_html(value) if value else None


def _named(value: object) -> str | None:
    return _text(object_row(value).get("name"), required=True) if value is not None else None


def series(value: object, *, detail: bool = False) -> ProviderSeriesRead:
    row = object_row(value)
    title = _text(row.get("name" if detail else "series"), required=True)
    assert title is not None
    year = _integer(row.get("year_began"))
    if not detail and year is not None:
        kind = object_row(row["series_type"]).get("id") if row.get("series_type") else None
        # These suffixes come from Series.__str__, not from the stored name.
        suffix = {
            12: f" ({year}) Digital",
            10: f" TPB ({year})",
            8: f" HC ({year})",
            9: f" GN ({year})",
        }.get(_integer(kind) or 0, f" ({year})")
        title = title.removesuffix(suffix)
    if not title.strip():
        raise ValueError("Expected a series title")
    volume = _integer(row.get("volume"))
    status = _text(row.get("status"))
    return ProviderSeriesRead(
        source=MetadataSource.METRON_API,
        identity_namespace=IdentityNamespace.METRON,
        external_id=external_id(row.get("id")),
        title=title,
        year_start=year,
        year_end=_integer(row.get("year_end")),
        publisher=_named(row.get("publisher")),
        issue_count=_integer(row.get("issue_count")),
        description=_description(row),
        resource_url=_url(row.get("resource_url"), "/series/"),
        cross_identities=_crosswalks(row, MetadataEntityKind.SERIES),
        source_updated_at=_updated(row.get("modified")),
        sort_title=_text(row.get("sort_name")),
        volume=str(volume) if volume is not None else None,
        series_type=_named(row.get("series_type")),
        status=status.lower() if status else None,
        language=_text(row.get("language")),
    )


def issue(value: object) -> ProviderIssueRead:
    row = object_row(value)
    number = _text(row.get("number"), required=True, limit=100)
    assert number is not None
    try:
        key = normalize_issue_number_text(number)
    except ValueError:
        key = None
    return ProviderIssueRead(
        source=MetadataSource.METRON_API,
        identity_namespace=IdentityNamespace.METRON,
        external_id=external_id(row.get("id")),
        series_external_id=external_id(object_row(row.get("series")).get("id")),
        issue_number_text=number,
        issue_number_key=key,
        title=_text(row.get("title")),
        description=_description(row),
        cover_date=_date(row.get("cover_date")),
        store_date=_date(row.get("store_date")),
        page_count=_integer(row.get("page")),
        resource_url=_url(row.get("resource_url"), "/issue/"),
        image_url=_url(row.get("image"), "/media/", image=True),
        cross_identities=_crosswalks(row, MetadataEntityKind.ISSUE),
        source_updated_at=_updated(row.get("modified")),
    )


def story_arc(value: object) -> ProviderStoryArcRead:
    row = object_row(value)
    title = _text(row.get("name"), required=True)
    assert title is not None
    return ProviderStoryArcRead(
        source=MetadataSource.METRON_API,
        identity_namespace=IdentityNamespace.METRON,
        external_id=external_id(row.get("id")),
        title=title,
        description=_description(row),
        resource_url=_url(row.get("resource_url"), "/arc/"),
        image_url=_url(row.get("image"), "/media/", image=True),
        cross_identities=_crosswalks(row, MetadataEntityKind.STORY_ARC),
        source_updated_at=_updated(row.get("modified")),
    )
