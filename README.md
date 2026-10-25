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
