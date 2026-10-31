# agents-schedule

Part of the claude-code-tatara workbench (python).

## What this is

A Python microservice with its own Postgres database that maps 5-field cron schedules with an IANA timezone to orchestrator presets and a prompt. On each fire it starts a cluster from the preset through the orchestrator API and sends the prompt.

## Development

Run from this directory with mise:

- `mise run test` - unit tests
- `mise run lint` - ruff check and format check
- `mise run fmt` - ruff format
- `mise run test:e2e` - kind end-to-end test (`scripts/e2e.sh`)
- `mise run test:chart` - chart render tests
- `mise run lint:chart` - `helm lint` of the chart

## Commands

The `sched` entry point (`uv run sched <command>`):

- `sched migrate` - apply the schema migrations to the service's own database and exit.
- `sched api` - serve the HTTP api (schedule management, run history, probes).
- `sched worker` - run the scheduler, dispatcher and tracker loops.

## Configuration

Every process reads one validated `SCHED_*` environment. A bad or missing variable makes the process exit at startup with one error naming each of them. The token is never logged.

| Variable | Default | Meaning |
|---|---|---|
| `SCHED_DATABASE_URL` | required | Postgres DSN of the service's own database |
| `SCHED_ORCHESTRATOR_URL` | required | orchestrator service port, e.g. `http://agents-orchestrator-services:8002` |
| `SCHED_ORCHESTRATOR_TOKEN` | - | orchestrator service token; exactly one of this and the `_FILE` variable is required |
| `SCHED_ORCHESTRATOR_TOKEN_FILE` | - | path of a file holding the token |
| `SCHED_ALLOWED_CIDRS` | `127.0.0.1/32` | comma-separated CIDRs allowed to reach the api (the oauth2-proxy sidecar or a port-forward) |
| `SCHED_HTTP_HOST` | `0.0.0.0` | api bind address (from agents-common) |
| `SCHED_HTTP_PORT` | `8000` | api port (from agents-common) |
| `SCHED_LOG_LEVEL` | `INFO` | log level (from agents-common) |
| `SCHED_SHUTDOWN_GRACE_SECONDS` | `20` | graceful shutdown wait (from agents-common) |
| `SCHED_MAX_IN_PROGRESS` | `3` | clusters running at once across all schedules (1..50) |
| `SCHED_MAX_PER_SCHEDULE` | `1` | clusters running at once per schedule (1..10) |
| `SCHED_DEFAULT_TIMEZONE` | `Europe/Warsaw` | IANA zone for schedules that name none |
| `SCHED_TICK_SECONDS` | `15` | scheduler loop interval (1..300) |
| `SCHED_POLL_SECONDS` | `10` | interval for polling running sessions (1..300) |
| `SCHED_DISPATCH_MAX_ATTEMPTS` | `5` | dispatch attempts before a run fails (1..20) |
| `SCHED_RUN_TIMEOUT_SECONDS` | `3600` | a run still going after this is failed so a stuck turn cannot hold a slot (60..86400) |

## API

Base path `/schedules/api/v1`. JSON in and out. The api is meant for the oauth2-proxy sidecar or a port-forward: it refuses any socket peer outside `SCHED_ALLOWED_CIDRS` with `403 forbidden` (forwarding headers are never read). `GET /healthz` and `GET /readyz` (`503 not_ready` when the database is unreachable) are outside the base path and exempt from the allowlist. The machine-readable contract is `openapi.json`, regenerated with `uv run python -m agents_schedule.api.openapi > openapi.json`; a test fails when it drifts from the app or when a route is missing from this section.

Every error has the shape `{"error": {"code": "<code>", "message": "<text>"}}`. Codes: `forbidden` (403), `not_found` (404; also a malformed id), `method_not_allowed` (405), `conflict` (409), `invalid_request` (422; body or query fails validation, unknown body fields included), `invalid_cron` and `invalid_timezone` (422), `unknown_preset` (422; the orchestrator has no such preset), `orchestrator_unavailable` (503; the orchestrator cannot be reached) and `orchestrator_auth_failed` (503; it rejected `SCHED_ORCHESTRATOR_TOKEN`).

A schedule is `{id, name, cron, timezone, preset_id, prompt, enabled, next_fires, last_run, created_at, updated_at}`. `next_fires` holds the next three fire times (UTC, ISO 8601), empty when disabled; `last_run` is the newest run or `null`. `cron` is a 5-field expression (macros such as `@daily` are refused). A write body is `{name (1..100), cron, timezone (IANA, optional, default SCHED_DEFAULT_TIMEZONE), preset_id (uuid), prompt (1..16384), enabled (default true)}`; unknown fields are refused. Writes check the preset against the orchestrator.

### `GET /schedules/api/v1/schedules`

All schedules. Response `200`: list of schedules. Errors: `403`.

### `POST /schedules/api/v1/schedules`

Create a schedule from the write body. Response `201`: the schedule. Errors: `403`, `409 conflict` (name taken), `422 invalid_request|invalid_cron|invalid_timezone|unknown_preset`, `503 orchestrator_unavailable|orchestrator_auth_failed`.

### `POST /schedules/api/v1/schedules/preview`

Body `{cron, timezone?}`. Response `200`: `{"next_fires": [three ISO 8601 times]}`. Errors: `403`, `422 invalid_request|invalid_cron|invalid_timezone`.

### `GET /schedules/api/v1/schedules/{schedule_id}`

One schedule. Response `200`: the schedule. Errors: `403`, `404 not_found`.

### `PUT /schedules/api/v1/schedules/{schedule_id}`

Replace a schedule with the write body (set `enabled` to pause or resume it). Response `200`: the schedule. Errors: `403`, `404 not_found`, `409 conflict`, `422 invalid_request|invalid_cron|invalid_timezone|unknown_preset`, `503 orchestrator_unavailable|orchestrator_auth_failed`.

### `DELETE /schedules/api/v1/schedules/{schedule_id}`

Delete a schedule and its runs. Response `204`, no body. Errors: `403`, `404 not_found`.

### `POST /schedules/api/v1/schedules/{schedule_id}/run`

Run now, with no body. Allowed on a disabled schedule. Response `202`: the run, `new` (picked up by the dispatcher) or already `ignored` with reason `previous_running` when the schedule already has `SCHED_MAX_PER_SCHEDULE` runs in flight. Errors: `403`, `404 not_found`.

### `GET /schedules/api/v1/schedules/{schedule_id}/runs`

Runs of one schedule, newest first. Query `limit` (1..100, default 20). Response `200`: list of runs. Errors: `403`, `404 not_found`, `422 invalid_request`.

### `GET /schedules/api/v1/runs`

Runs of all schedules, newest first. Query `status` (one of the run statuses below) and `limit` (1..200, default 50). Response `200`: list of runs. Errors: `403`, `422 invalid_request`.

### `GET /schedules/api/v1/runs/{run_id}`

One run. Response `200`: the run. Errors: `403`, `404 not_found`.

### Runs

A run is `{id, schedule_id, trigger, fire_time, status, reason, session_id, session_slug, message_id, reuse, created_at, updated_at, started_at, finished_at}`. `trigger` is `scheduled` or `manual`; `fire_time` is the cron time (scheduled) or the request time (manual).

Statuses: `new` (waiting for a slot under `SCHED_MAX_IN_PROGRESS` and `SCHED_MAX_PER_SCHEDULE`), `in-progress` (session created or prompt delivered, turn running), `handled` (the turn completed), `failed` (see reason) and `ignored` (never sent). `reason` is null unless the run is `failed` or `ignored`:

| Status | Reason | Meaning |
|---|---|---|
| `ignored` | `previous_running` | the schedule already had the maximum runs in flight when this one fired |
| `ignored` | `schedule_disabled` | a scheduled run was claimed while its schedule was disabled (a manual run on a disabled schedule is sent) |
| `failed` | `preset_not_found` | the orchestrator has no such preset any more |
| `failed` | `session_not_found` | the run's session disappeared before the turn ended |
| `failed` | `orchestrator_refused: <code>` | the orchestrator answered a request with a refusal, `<code>` is its error code |
| `failed` | `orchestrator_unavailable` | the orchestrator stayed unreachable for `SCHED_DISPATCH_MAX_ATTEMPTS` attempts |
| `failed` | `message_failed` | the orchestrator marked the prompt message failed |
| `failed` | `message_cancelled` | the prompt message was cancelled |
| `failed` | `turn_failed` | the agent's turn failed |
| `failed` | `timeout` | the run was still going after `SCHED_RUN_TIMEOUT_SECONDS` |

`session_slug` is the slug the run's session is created with (`<schedule name slug>-<8 hex of the run id>`), fixed at first send. `session_id` and `message_id` are set as the run progresses.

Reuse: when the preset sets `reuse_running_agent` and the schedule's previous run has a session that is still running, the run posts its prompt to that session instead of creating one, and `reuse` is `delivered`. When the preset opts in but that session is gone, not running or refuses the prompt, `reuse` is `fallback` and the run creates a new session as usual. `reuse` is null when the preset does not opt in or no earlier run has a session.

Prompts are sandboxed Jinja templates rendered when the run is sent. Variables: `{{ schedule_name }}`, `{{ fired_at }}` (ISO 8601, in the schedule's timezone), `{{ preset }}` (the preset id) and `{{ now }}` (ISO 8601, UTC). An undefined variable or a syntax error fails the run with reason `prompt_invalid`; `POST /schedules/preview` with a `prompt` returns `rendered` and `variables`, or 422 `invalid_prompt` with the line.

### Versioning

`/schedules/api/v1` is additive-only: new routes, fields, query parameters and error codes may appear, and existing ones are never removed, renamed or retyped. A breaking change would be a new `/schedules/api/v2`. Clients must ignore fields they do not know. agents-web vendors `openapi.json`.

## Releases

Every green push to `main` ends with the `release` job in `.github/workflows/ci.yaml`, which needs every other job of the run, so releases after v0.1.0 are automatic:

- `scripts/next_version.py` computes the version from the conventional commits since the last `vX.Y.Z` git tag (every commit, branch commits behind merge commits included): a `type!:` subject or a `BREAKING CHANGE:` footer bumps major (also on 0.x), `feat` bumps minor, anything else bumps patch. It bumps from v0.1.0 below and never restarts at 0.1.0.
- The job copies the run's image `harbor.szymonrichert.pl/containers/agents-schedule:<sha7>` to the tags `X.Y.Z`, `X.Y` and `X` with `regctl image copy` (manifest only, no layer upload), then pushes git tag `vX.Y.Z` with the job token.
- When `HEAD` is already in a `vX.Y.Z` tag (a re-run, or a newer commit already released) the job skips everything after the version step. Release jobs run one at a time (concurrency group `release`); retry a failed one with "Re-run failed jobs".
- agents-helmfile must pin `X.Y.Z`, never `<sha7>`: Harbor retention prunes `<sha7>` tags within hours, semver tags survive it.
- The chart is published only from `main`, as `<version>-main.<sha7>` (`scripts/chart-version.sh`); a tag publishes no chart.

History:

- v0.1.0 (2026-10-25): tag v0.1.0 on commit 6cfa955; image `harbor.szymonrichert.pl/containers/agents-schedule:6cfa955` (sha7 6cfa955); chart `oci://harbor.szymonrichert.pl/charts/agents-schedule` version 0.1.0. Published chart versions are immutable.
