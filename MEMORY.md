# agents-schedule memory

Lessons that outlive a session. Newest first.

## 2026-10-25 - why this component exists

agents-schedule is the time-driven sibling of agents-webhook: it turns cron schedules into agent cluster runs through the orchestrator API, keeping its own database. It consumes the agents-common library (settings, Postgres pool, migrator, guarded queue transitions, orchestrator client, run tracker) and the orchestrator service tokens issued by agents-webhook-microservice.
