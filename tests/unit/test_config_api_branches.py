"""Direct branch coverage for configuration API helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from sqlalchemy import select

from pullbox.api.v1 import config as config_api
from pullbox.core.exceptions import ValidationError
from pullbox.models.config import DEFAULT_SYSTEM_CONFIG, SystemConfig
from pullbox.models.library import LibraryRoot
from pullbox.models.user import User
from pullbox.schemas.config import ConfigUpdate
from pullbox.services.auth_service import AuthService

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncSession


def _request() -> SimpleNamespace:
    return SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))


def _user() -> SimpleNamespace:
    return SimpleNamespace(id=1, username="admin")


async def _call_update(
    session: AsyncSession,
    values: dict[str, str],
    *,
    user: SimpleNamespace | None = None,
) -> dict[str, object]:
    return await config_api.update_config(
        _request(),
        ConfigUpdate(values=values),
        user or _user(),
        session,
    )


@pytest.mark.asyncio
async def test_get_config_obfuscates_invalid_secret(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "pullbox.core.encryption.decrypt_secret",
        lambda _value: (_ for _ in ()).throw(ValueError("invalid ciphertext")),
    )
    db_session.add(
        SystemConfig(
            key="comicvine_api_key",
            value="not-valid-ciphertext",
            value_type="secret",
            description="ComicVine key",
        )
    )

    response = await config_api.get_config(object(), db_session)

    api_key = next(item for item in response if item.key == "comicvine_api_key")
    assert api_key.value == "••••••••"
    assert api_key.description == "ComicVine key"


@pytest.mark.asyncio
async def test_get_config_obfuscates_decrypted_secret(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pullbox.core.encryption.decrypt_secret", lambda _value: "abc12345")
    monkeypatch.setattr("pullbox.core.comicvine_key.obfuscate_api_key", lambda _value: "••••2345")
    db_session.add(
        SystemConfig(
            key="comicvine_api_key",
            value="encrypted-value",
            value_type="secret",
        )
    )

    response = await config_api.get_config(object(), db_session)

    api_key = next(item for item in response if item.key == "comicvine_api_key")
    assert api_key.value == "••••2345"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("comicvine_api_key", "secret", "dedicated endpoint"),
        ("does_not_exist", "1", "Unknown configuration key"),
        ("search_interval_hours", "soon", "must be an integer"),
        ("rename_on_import", "maybe", "must be a boolean"),
        ("post_processing_method", "teleport", "post_processing_method"),
        ("torrent_import_strategy", "chaos", "torrent_import_strategy"),
        ("preferred_format", "exe", "preferred_format"),
        ("colon_replacement", "emoji", "colon_replacement"),
        ("allowed_import_extensions", ".cbz,exe", "blocked for security"),
        ("session_lifetime_hours", "0", "between 1 and 720"),
        ("process_completed_interval_seconds", "120", "between 300 and 600"),
        ("health_scheduler_interval_minutes", "0", "between 1 and 1440"),
        ("health_indexers_interval_hours", "169", "between 1 and 168"),
        ("health_history_retention_days", "31", "between 1 and 30"),
        ("instance_name", "   ", "Instance name cannot be empty"),
        ("base_url", "ftp://example.test", "http:// or https://"),
        ("library_permissions_folder_mode", "nope", "chmod mode"),
        ("library_permissions_file_mode", "nope", "chmod mode"),
        ("library_permissions_hardlink_behavior", "follow", "hardlink behavior"),
        ("library_permissions_symlink_behavior", "follow", "symlink behavior"),
        ("local_auth_bypass_addresses", "not a cidr", "Invalid local bypass address"),
    ],
)
async def test_update_config_rejects_invalid_values(
    db_session: AsyncSession,
    key: str,
    value: str,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        await _call_update(db_session, {key: value})


@pytest.mark.asyncio
async def test_update_config_rejects_runtime_managed_keys(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_defaults = dict(DEFAULT_SYSTEM_CONFIG)
    managed_defaults["logs_dir"] = ("/logs", "string")
    monkeypatch.setattr(config_api, "DEFAULT_SYSTEM_CONFIG", managed_defaults)

    with pytest.raises(ValidationError, match="runtime-managed"):
        await _call_update(db_session, {"logs_dir": "/tmp/logs"})


@pytest.mark.asyncio
async def test_update_config_rejects_runtime_managed_https(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        config_api,
        "https_runtime_config_values",
        lambda: {"https_enabled": "true"},
    )

    with pytest.raises(ValidationError, match="runtime-managed"):
        await _call_update(db_session, {"https_enabled": "true"})


@pytest.mark.asyncio
async def test_update_config_rejects_invalid_https_values(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config_api, "https_runtime_config_values", lambda: {})
    monkeypatch.setattr(
        config_api,
        "validate_https_config_values",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("bad certificate pair")),
    )

    with pytest.raises(ValidationError, match="bad certificate pair"):
        await _call_update(db_session, {"https_enabled": "true"})


@pytest.mark.asyncio
async def test_update_config_rejects_local_bypass_without_safe_target(
    db_session: AsyncSession,
) -> None:
    with pytest.raises(ValidationError, match="trusted local address"):
        await _call_update(
            db_session,
            {
                "local_auth_bypass_enabled": "true",
                "local_auth_bypass_addresses": "",
            },
        )
    await db_session.rollback()

    with pytest.raises(ValidationError, match="active username"):
        await _call_update(
            db_session,
            {
                "local_auth_bypass_enabled": "true",
                "local_auth_bypass_addresses": "127.0.0.1",
                "local_auth_bypass_username": "missing",
            },
        )
    await db_session.rollback()

    db_session.add_all(
        [
            User(username="first", password_hash=AuthService.hash_password("Test@1234")),
            User(username="second", password_hash=AuthService.hash_password("Test@1234")),
        ]
    )
    await db_session.flush()

    with pytest.raises(ValidationError, match="more than one active account"):
        await _call_update(
            db_session,
            {
                "local_auth_bypass_enabled": "true",
                "local_auth_bypass_addresses": "127.0.0.1",
            },
        )


@pytest.mark.asyncio
async def test_update_config_enables_local_bypass_with_default_addresses(
    db_session: AsyncSession,
) -> None:
    """Fresh installs should honor default trusted addresses when toggling bypass."""
    db_session.add(User(username="admin", password_hash=AuthService.hash_password("Test@1234")))
    await db_session.flush()

    response = await _call_update(
        db_session,
        {
            "local_auth_bypass_enabled": "true",
            "local_auth_bypass_username": "admin",
        },
    )

    configs = {item["key"]: item["value"] for item in response["configs"]}
    assert configs["local_auth_bypass_enabled"] == "true"
    assert configs["local_auth_bypass_username"] == "admin"
    assert configs["local_auth_bypass_addresses"] == "127.0.0.1, ::1"


@pytest.mark.asyncio
async def test_update_config_syncs_comics_directory_to_library_root(
    db_session: AsyncSession,
    tmp_path: Path,
) -> None:
    comics_dir = tmp_path / "comics"
    comics_dir.mkdir()

    response = await _call_update(db_session, {"comics_directory": str(comics_dir)})

    configs = {item["key"]: item["value"] for item in response["configs"]}
    assert configs["comics_directory"] == str(comics_dir.resolve())

    root = (
        await db_session.execute(
            select(LibraryRoot).where(LibraryRoot.path == str(comics_dir.resolve()))
        )
    ).scalar_one()
    assert root.name == "Comics Directory"
    assert root.enabled is True


@pytest.mark.asyncio
async def test_update_config_applies_runtime_side_effects_and_restart_response(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pullbox.ui import routes as ui_routes

    monkeypatch.setattr(ui_routes, "_cached_instance_name", ui_routes._cached_instance_name)
    db_session.add(User(username="admin", password_hash=AuthService.hash_password("Test@1234")))
    await db_session.flush()

    reconfigure_logging = Mock()
    utility_logging_calls: list[tuple[object, str]] = []
    audit_events: list[object] = []
    display_invalidated = False

    def fake_reconfigure_logging_runtime(**_kwargs: object) -> None:
        reconfigure_logging()

    def fake_configure_utility_logging_runtime(*, log_dir: object, level: str) -> None:
        utility_logging_calls.append((log_dir, level))
        raise OSError("log dir unavailable")

    def fake_validate_https_config_values(**_kwargs: object) -> None:
        return None

    async def fake_record_audit_event(*_args: object, **kwargs: object) -> None:
        audit_events.append(kwargs["detail"])

    def fake_invalidate_display_cache() -> None:
        nonlocal display_invalidated
        display_invalidated = True

    monkeypatch.setattr(config_api, "https_runtime_config_values", lambda: {})
    monkeypatch.setattr(
        config_api,
        "validate_https_config_values",
        fake_validate_https_config_values,
    )
    monkeypatch.setattr(
        "pullbox.logging.reconfigure_logging_runtime",
        fake_reconfigure_logging_runtime,
    )
    monkeypatch.setattr(
        "pullbox.utilities.logging_config.configure_utility_logging_runtime",
        fake_configure_utility_logging_runtime,
    )
    monkeypatch.setattr(
        "pullbox.services.audit_service.record_audit_event",
        fake_record_audit_event,
    )
    monkeypatch.setattr(
        "pullbox.core.display_time.invalidate_display_cache",
        fake_invalidate_display_cache,
    )

    response = await _call_update(
        db_session,
        {
            "instance_name": " Pullbox Lab ",
            "base_url": "https://pullbox.test/",
            "https_enabled": "true",
            "https_cert_path": "/config/certs/fullchain.pem",
            "https_key_path": "/config/certs/privkey.pem",
            "local_auth_bypass_enabled": "true",
            "local_auth_bypass_addresses": "127.0.0.1",
            "local_auth_bypass_username": "admin",
            "display.date_format": "YYYY-MM-DD",
            "log_level": "debug",
            "log_size_limit_mb": "2",
            "log_backup_count": "7",
            "utility_log_level": "warning",
            "post_processing_method": "copy",
            "convert_to_preferred_format_on_import": "true",
            "update_embedded_comicinfo_from_match_on_import": "true",
            "health_scheduler_interval_minutes": "60",
        },
    )

    assert ui_routes.get_instance_name() == "Pullbox Lab"
    assert response["restart_required"] is True
    assert response["restart_required_keys"] == [
        "https_cert_path",
        "https_enabled",
        "https_key_path",
    ]
    assert reconfigure_logging.call_count == 1
    assert utility_logging_calls
    assert display_invalidated is True
    assert any("Security config updated" in str(detail) for detail in audit_events)
    assert any("Local auth bypass enabled" in str(detail) for detail in audit_events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "values",
    [
        {
            "post_processing_method": "hardlink",
            "convert_to_preferred_format_on_import": "true",
        },
        {
            "post_processing_method": "symlink",
            "update_embedded_comicinfo_from_match_on_import": "true",
        },
    ],
)
async def test_update_config_rejects_import_policy_combinations(
    db_session: AsyncSession,
    values: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        await _call_update(db_session, values)


@pytest.mark.asyncio
async def test_update_config_rejects_convert_with_existing_hardlink_method(
    db_session: AsyncSession,
) -> None:
    db_session.add(
        SystemConfig(
            key="post_processing_method",
            value="hardlink",
            value_type="string",
        )
    )

    with pytest.raises(ValidationError, match="Move or Copy"):
        await _call_update(
            db_session,
            {"convert_to_preferred_format_on_import": "true"},
        )


@pytest.mark.asyncio
async def test_comicvine_key_test_and_save_branches(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls = 0

    class FakeComicVineProvider:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        async def test_connection(self) -> SimpleNamespace:
            if self.api_key == "boom":
                raise RuntimeError("provider down")
            return SimpleNamespace(healthy=True, message="ok", response_time_ms=12)

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    saved_keys: list[str] = []

    async def fake_save_key(_session: AsyncSession, api_key: str) -> None:
        saved_keys.append(api_key)

    monkeypatch.setattr(
        "pullbox.providers.metadata.comicvine.ComicVineProvider",
        FakeComicVineProvider,
    )
    monkeypatch.setattr(
        "pullbox.services.metadata_sources.save_comicvine_api_key",
        fake_save_key,
    )
    monkeypatch.setattr(
        "pullbox.core.comicvine_key.obfuscate_api_key",
        lambda key: f"***{key[-4:]}",
    )

    assert await config_api.test_comicvine_key(object(), db_session, {}) == {
        "healthy": False,
        "message": "No API key provided.",
    }
    assert await config_api.test_comicvine_key(object(), db_session, {"api_key": "good"}) == {
        "healthy": True,
        "message": "ok",
        "response_time_ms": 12,
    }
    failed = await config_api.test_comicvine_key(object(), db_session, {"api_key": "boom"})
    assert failed["healthy"] is False
    assert close_calls == 2

    assert await config_api.save_comicvine_key(object(), db_session, {}) == {
        "saved": False,
        "message": "No API key provided.",
    }
    saved = await config_api.save_comicvine_key(
        object(),
        db_session,
        {"api_key": "comicvine-secret"},
    )
    assert saved == {
        "saved": True,
        "message": "API key saved.",
        "obfuscated": "***cret",
    }
    assert saved_keys == ["comicvine-secret"]


@pytest.mark.asyncio
async def test_naming_preview_endpoints_return_examples() -> None:
    legacy = await config_api.naming_preview(object(), template="{series} #{issue:03d}")
    assert legacy.examples[0] == "Batman #001.cbz"

    grouped = await config_api.naming_preview_grouped(
        object(),
        template="{Series} ({Year}) #{Issue:03d}",
        template_type="standard",
    )
    assert grouped.template_type == "standard"
    assert grouped.examples
