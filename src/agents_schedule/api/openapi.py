"""Deterministic OpenAPI export: `python -m agents_schedule.api.openapi > openapi.json`.

FastAPI documents a validation error as its own `HTTPValidationError`; this service answers
every error with `{"error": {"code", "message"}}` (see `errors.py`), so the export replaces the
generated error responses with that shape and the statuses each route really returns.
"""

from __future__ import annotations

import json
from typing import Any

from agents_schedule.api.app import PREFIX, create_app
from agents_schedule.settings import Settings

ERROR_REF = {"$ref": "#/components/schemas/ErrorResponse"}

ERROR_SCHEMAS: dict[str, Any] = {
    "ErrorBody": {
        "type": "object",
        "title": "ErrorBody",
        "required": ["code", "message"],
        "properties": {
            "code": {"type": "string", "title": "Code"},
            "message": {"type": "string", "title": "Message"},
        },
    },
    "ErrorResponse": {
        "type": "object",
        "title": "ErrorResponse",
        "required": ["error"],
        "properties": {"error": {"$ref": "#/components/schemas/ErrorBody"}},
    },
}

ERRORS = {
    403: "source address is not allowed (forbidden)",
    404: "not found (not_found)",
    409: "a schedule with this name already exists (conflict)",
    422: "invalid request (invalid_request, invalid_cron, invalid_timezone, unknown_preset)",
    503: "orchestrator unreachable or rejecting the token (orchestrator_unavailable, "
    "orchestrator_auth_failed)",
}

ROUTE_ERRORS: dict[tuple[str, str], tuple[int, ...]] = {
    ("get", "/schedules"): (403,),
    ("post", "/schedules"): (403, 409, 422, 503),
    ("post", "/schedules/preview"): (403, 422),
    ("get", "/schedules/{schedule_id}"): (403, 404),
    ("put", "/schedules/{schedule_id}"): (403, 404, 409, 422, 503),
    ("delete", "/schedules/{schedule_id}"): (403, 404),
    ("post", "/schedules/{schedule_id}/run"): (403, 404),
    ("get", "/schedules/{schedule_id}/runs"): (403, 404, 422),
    ("get", "/runs"): (403, 422),
    ("get", "/runs/{run_id}"): (403, 404),
}


def _document() -> dict[str, Any]:
    settings = Settings(
        database_url="postgresql://openapi/export",
        orchestrator_url="http://orchestrator.invalid",
        orchestrator_token="openapi-export",
    )
    app = create_app(settings, pool=None, client=None)  # type: ignore[arg-type]
    doc = app.openapi()
    for path, methods in doc["paths"].items():
        if not path.startswith(PREFIX):
            continue
        for method, operation in methods.items():
            responses = operation["responses"]
            for status in [s for s in responses if not s.startswith("2")]:
                del responses[status]
            for status in ROUTE_ERRORS[(method, path.removeprefix(PREFIX))]:
                responses[str(status)] = {
                    "description": ERRORS[status],
                    "content": {"application/json": {"schema": ERROR_REF}},
                }
    schemas = doc["components"]["schemas"]
    for name in ("HTTPValidationError", "ValidationError"):
        schemas.pop(name, None)
    schemas.update(ERROR_SCHEMAS)
    return doc


def export() -> str:
    """The app's OpenAPI document as JSON with sorted keys and a trailing newline."""
    return json.dumps(_document(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    print(export(), end="")
