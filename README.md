# agents-schedule

Part of the claude-code-tatara workbench (python).

## What this is

A Python microservice with its own Postgres database that maps 5-field cron schedules with an IANA timezone to orchestrator presets and a prompt. On each fire it starts a cluster from the preset through the orchestrator API and sends the prompt.
