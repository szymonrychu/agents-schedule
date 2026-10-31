"""The FastAPI app: error shape, source allowlist and probes (design D11)."""

from __future__ import annotations

import ipaddress
import logging

from agents_common.orchestrator import OrchestratorClient
from agents_common.service_gate import ServiceTokenGate
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Receive, Scope, Send

from agents_schedule.api import runs, schedules
from agents_schedule.api.errors import ApiError, error_response
from agents_schedule.settings import Settings

log = logging.getLogger(__name__)

PREFIX = "/schedules/api/v1"
EXEMPT_PATHS = frozenset({"/healthz", "/readyz"})


class SourceAllowlist:
    """Refuses peers outside the allowlist. Only the socket peer is read: Forwarded,
    X-Forwarded-For and X-Real-IP are never consulted."""

    def __init__(self, app: ASGIApp, networks: list) -> None:
        self.app = app
        self.networks = list(networks)

    def allowed(self, scope: Scope) -> bool:
        client = scope.get("client")
        if not client:
            return False
        try:
            addr = ipaddress.ip_address(client[0])
        except ValueError:
            return False
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
            addr = addr.ipv4_mapped
        return any(addr.version == n.version and addr in n for n in self.networks)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        if (scope["type"] == "http" and scope.get("path") in EXEMPT_PATHS) or self.allowed(scope):
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        response = error_response(403, "forbidden", "source address is not allowed")
        await response(scope, receive, send)


def _build_app(
    settings: Settings, pool: AsyncConnectionPool, client: OrchestratorClient
) -> FastAPI:
    app = FastAPI(title="agents-schedule", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.settings = settings
    app.state.pool = pool
    app.state.client = client

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return error_response(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0] if exc.errors() else {}
        where = ".".join(str(p) for p in first.get("loc", ()) if p != "body")
        message = f"{where}: {first.get('msg', 'invalid request')}" if where else "invalid request"
        return error_response(422, "invalid_request", message)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "error")
        return error_response(exc.status_code, code, str(exc.detail))

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        try:
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")
        except Exception:
            log.exception("readiness check failed")
            return error_response(503, "not_ready", "database is not reachable")
        return JSONResponse({"status": "ok"})

    app.include_router(schedules.router, prefix=PREFIX)
    app.include_router(runs.router, prefix=PREFIX)
    return app


def create_app(
    settings: Settings, pool: AsyncConnectionPool, client: OrchestratorClient
) -> FastAPI:
    """The ui app: every route behind the source allowlist."""
    app = _build_app(settings, pool, client)
    app.add_middleware(SourceAllowlist, networks=settings.api_networks())
    return app


def create_admin_app(
    settings: Settings,
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    tokens: list[str],
) -> FastAPI:
    """The admin app: the same routes behind bearer service tokens instead of the allowlist."""
    app = _build_app(settings, pool, client)
    app.add_middleware(ServiceTokenGate, tokens=tokens)
    return app
