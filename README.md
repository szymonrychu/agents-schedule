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
