# agents-schedule memory

Lessons that outlive a session. Newest first.

## 2026-10-25 - dispatcher rules (run-dispatcher)

- `schedule_disabled` ignores only `trigger = 'scheduled'` runs claimed while their schedule is
  disabled. A manual run on a disabled schedule dispatches, because design D11 allows run-now
  on a disabled schedule; ignoring it would make that call pointless (coordinator ruling).
- The dispatcher wakes on NOTIFY channel `runs_new` (`dispatcher.CHANNEL`), besides its
  `SCHED_POLL_SECONDS` sleep. The task named no channel; whoever inserts a `new` run (scheduler,
  run-now route) should notify it, else the run waits up to one poll interval.
- `session_slug`: a name with no usable `[a-z0-9]` character gets the base `schedule`, and a
  base of `orchestrator` or `orchestrator-*` is prefixed `s-`, because the orchestrator's
  `validate_slug` rejects those slugs (mirrors its `generated_slug` `p-` prefix). The slug is
  stored on the run at first send, so renaming the schedule never changes it.

## 2026-10-25 - why this component exists

agents-schedule is the time-driven sibling of agents-webhook: it turns cron schedules into agent cluster runs through the orchestrator API, keeping its own database. It consumes the agents-common library (settings, Postgres pool, migrator, guarded queue transitions, orchestrator client, run tracker) and the orchestrator service tokens issued by agents-webhook-microservice.
