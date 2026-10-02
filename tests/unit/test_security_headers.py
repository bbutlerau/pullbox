"""Unit tests for SecurityHeadersMiddleware — security response headers.

Tests verify that X-Content-Type-Options, X-Frame-Options, Referrer-Policy,
Permissions-Policy, Content-Security-Policy, and HSTS headers are correctly
applied to all responses including errors.

Run:
    pytest tests/unit/test_security_headers.py -v
    pytest tests/unit/test_security_headers.py -v --cov=pullbox.api.middleware
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from pullbox.api.middleware import SecurityHeadersMiddleware

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

os.environ.setdefault("PULLBOX_SECRET_KEY", "test-secret-key-for-security-headers")


def _build_app(*, debug: bool) -> FastAPI:
    """Build a minimal FastAPI app with SecurityHeadersMiddleware."""
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware, debug=debug)

    @app.get("/ping")
    async def ping() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/json-api")
    async def json_api() -> dict[str, str]:
        return {"data": "value"}

    return app


@pytest.fixture
async def production_client() -> AsyncGenerator[AsyncClient, None]:
    """Client for production mode (debug=False, HSTS enabled)."""
    app = _build_app(debug=False)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


@pytest.fixture
async def debug_client() -> AsyncGenerator[AsyncClient, None]:
    """Client for debug mode (debug=True, HSTS disabled)."""
    app = _build_app(debug=True)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


# ── Individual Header Tests ───────────────────────────────────────


class TestSecurityHeaders:
    """Tests for individual security headers on normal responses."""

    @pytest.mark.asyncio
    async def test_x_content_type_options_present(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        assert resp.headers["x-content-type-options"] == "nosniff"

    @pytest.mark.asyncio
    async def test_x_frame_options_present(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        assert resp.headers["x-frame-options"] == "DENY"

    @pytest.mark.asyncio
    async def test_referrer_policy_present(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"

    @pytest.mark.asyncio
    async def test_permissions_policy_present(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        policy = resp.headers["permissions-policy"]
        assert "camera=()" in policy
        assert "microphone=()" in policy
        assert "geolocation=()" in policy

    @pytest.mark.asyncio
    async def test_csp_present(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        csp = resp.headers["content-security-policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "comicvine.gamespot.com" in csp

    @pytest.mark.asyncio
    async def test_application_csp_is_pinned_exactly(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")

        assert resp.headers["content-security-policy"] == (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https://comicvine.gamespot.com https://*.gamespot.com "
            "https://s3.amazonaws.com https://metron.cloud/media/ "
            "https://static.metron.cloud/media/; "
            "connect-src 'self'; "
            "font-src 'self'; "
            "frame-ancestors 'none'"
        )

    @pytest.mark.asyncio
    async def test_docs_csp_is_pinned_exactly(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/docs")

        assert resp.headers["content-security-policy"] == (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "img-src 'self' data: https://fastapi.tiangolo.com; "
            "connect-src 'self'; "
            "font-src 'self'; "
            "frame-ancestors 'none'"
        )

    @pytest.mark.asyncio
    async def test_xss_protection_disabled(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        assert resp.headers["x-xss-protection"] == "0"


# ── HSTS Conditional Logic ────────────────────────────────────────


class TestHSTS:
    """Tests for HSTS header presence based on debug mode."""

    @pytest.mark.asyncio
    async def test_hsts_present_in_production(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/ping")
        hsts = resp.headers["strict-transport-security"]
        assert "max-age=31536000" in hsts
        assert "includeSubDomains" in hsts

    @pytest.mark.asyncio
    async def test_hsts_absent_in_debug(self, debug_client: AsyncClient) -> None:
        resp = await debug_client.get("/ping")
        assert "strict-transport-security" not in resp.headers


# ── Headers on Error and API Responses ────────────────────────────


class TestHeadersOnAllResponses:
    """Verify security headers are present on error, API, and HTML responses."""

    @pytest.mark.asyncio
    async def test_headers_on_error_responses(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/nonexistent-route")
        assert resp.status_code in (404, 405)
        assert resp.headers["x-content-type-options"] == "nosniff"
        assert resp.headers["x-frame-options"] == "DENY"
        assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "content-security-policy" in resp.headers

    @pytest.mark.asyncio
    async def test_headers_on_api_responses(self, production_client: AsyncClient) -> None:
        resp = await production_client.get("/json-api")
        assert resp.status_code == 200
        assert resp.headers["x-content-type-options"] == "nosniff"
        assert resp.headers["x-frame-options"] == "DENY"
        assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "content-security-policy" in resp.headers
        assert "strict-transport-security" in resp.headers

    @pytest.mark.asyncio
    async def test_non_static_responses_disable_browser_caching(
        self, production_client: AsyncClient
    ) -> None:
        resp = await production_client.get("/ping")

        assert resp.headers["cache-control"] == "no-store, no-cache, max-age=0, must-revalidate"
        assert resp.headers["pragma"] == "no-cache"
        assert resp.headers["expires"] == "0"


# ── Server Header Suppression ────────────────────────────────────


class TestServerHeaderSuppression:
    """Verify the server header is suppressed to avoid information disclosure."""

    @pytest.mark.asyncio
    async def test_no_server_header_on_normal_response(
        self, production_client: AsyncClient
    ) -> None:
        """Normal responses should not reveal the server software."""
        resp = await production_client.get("/ping")
        assert "server" not in resp.headers

    @pytest.mark.asyncio
    async def test_no_server_header_on_api_response(self, production_client: AsyncClient) -> None:
        """API responses should not reveal the server software."""
        resp = await production_client.get("/json-api")
        assert "server" not in resp.headers

    @pytest.mark.asyncio
    async def test_no_server_header_on_error_response(self, production_client: AsyncClient) -> None:
        """Error responses should not reveal the server software."""
        resp = await production_client.get("/nonexistent-route")
        assert "server" not in resp.headers

    @pytest.mark.asyncio
    async def test_no_server_header_in_debug_mode(self, debug_client: AsyncClient) -> None:
        """Debug mode should also suppress the server header."""
        resp = await debug_client.get("/ping")
        assert "server" not in resp.headers

    @pytest.mark.asyncio
    async def test_other_security_headers_still_present_after_server_removal(
        self, production_client: AsyncClient
    ) -> None:
        """Removing server header should not affect other security headers."""
        resp = await production_client.get("/ping")
        assert "server" not in resp.headers
        assert resp.headers["x-content-type-options"] == "nosniff"
        assert resp.headers["x-frame-options"] == "DENY"
        assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "content-security-policy" in resp.headers
