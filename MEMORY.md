# agents-schedule memory

Lessons that outlive a session. Newest first.

## 2026-11-01 - usage quota hold (claude-usage-monitor E3)

- A 409 `usage_quota` on session create (agents-common v0.7.0 `UsageQuotaRefused`) puts the run back to `new` through `RunsStore.hold`: guarded `in-progress -> new` with `not_before = exc.retry_at(now)`, reason `usage_quota`, `attempts=0`, and `started_at`, `session_id`, `reuse` cleared (the run is unsent again; `session_slug` stays, it is fixed at first send). Why `new` and not a new status: the CHECK on `runs.status` and the api contract stay unchanged, and `in_flight` already counts `new`, so a fire during the hold is `ignored previous_running` with no extra code.
- `_PICK` skips `not_before > now` but its NOT EXISTS subquery deliberately does not look at `not_before`: a held run still blocks the newer `new` runs of its schedule (FIFO per schedule), other schedules go on. The catch sits before `OrchestratorError` and never touches `attempts`, so a refusal can neither fail the run `orchestrator_refused` nor reach `orchestrator_unavailable`.
- No timer: the dispatcher already wakes every `SCHED_POLL_SECONDS`, so a run is retried within one poll after `not_before`. `not_before` is not in the api views or `openapi.json`.

## 2026-10-31 - token-gated admin listener (superleader-crud SC1)

- The api process serves a second uvicorn app on `SCHED_ADMIN_PORT` when `SCHED_ADMIN_TOKENS_FILE` is set: the same routers as the ui app (one `_build_app`), wrapped in agents-common v0.6.0 `ServiceTokenGate` instead of `SourceAllowlist`. Off by default, so the ui deploy is unchanged. Why a second port, not a header on the first: the ui port is protected by peer CIDR behind oauth2-proxy and must keep that contract; the orchestrator relay needs bearer auth from other pods.
- Chart: `admin.enabled` mounts the tokens Secret into the api pod only and adds an `admin` container and Service port; enabling it without `admin.tokensSecret.name` fails the render.

## 2026-10-28 - release job (semver-image-tags-at-the-end)

- Every green main build ends with `release` (needs every other job, `contents: write`,
  concurrency group `release`, full history and tags). Why: Harbor retention pruned the deployed
  sha7 tags within hours (incident 2026-10-28; this repo's `6cfa955` among them); semver tags
  survive it. `scripts/next_version.py` and `tests/test_next_version.py` are byte-identical
  copies of agents-orchestrator's: never edit them here, change them there and copy.
- Versions bump from the hand-made v0.1.0 (on 6cfa955), never restart at 0.1.0: the base is the
  numerically highest `vX.Y.Z` reachable from HEAD. `!` or a BREAKING CHANGE footer is major
  (also on 0.x), `feat` minor, else patch, over every commit since the base (not first-parent).
  Nothing is printed when HEAD is already contained in a release tag, because concurrency
  serializes release jobs but not their order.
- Retag with `regctl image copy` (manifest only) first, lightweight git tag (checkout token,
  never `secrets.GITHUB_TOKEN`) last, so a failed run leaves no tag and a re-run repeats the
  idempotent copies.
- Retired: the `push: tags: [v*]` trigger, `chart-publish-tag` and chart-version.sh's tag mode.
  The release job owns `v*` tags, a GITHUB_TOKEN tag push starts no workflow, and the tag job's
  Chart.yaml equality check would have refused every release. Main `chart-publish` stays.

## 2026-10-25 - kind e2e two-worker failure on 814ba20 (e2e-two-worker-failure)

- Root cause: a race in the test, present since 5653502, not a product regression. The
  dispatcher claims a run (`new -> in-progress`) and only then fetches the preset, creates the
  session and posts the prompt, so an in-progress run has `message_id` null for a few ms. The
  two-worker probe completed every in-progress run, so a poll landing in that window posted
  `/_messages/None/complete` (fake log: preset GET, then the None complete, then the session
  POST of the same worker). fc99d8c passed by timing. d4f6a92 (fault isolation, proxy_headers)
  did not change the order or the api; the api log shows only 200s.
- The follow-on `Server disconnected` in the next test was the fake, not the api: control.py
  raised on `int("None")`, and uvicorn closes the connection after an exception once the 500 has
  started, so the test's next request on that keep-alive connection (port-forward 18091) got EOF.
- Rule: a test completes a turn only once the run has a `message_id`
  (`complete_sent_turns`, pinned by tests/test_e2e_helpers.py). The e2e diagnostics print every
  worker pod by label; `kubectl logs deployment/x` shows one pod only.

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

## 2026-10-25 - released v0.1.0

Tagged merged main commit 6cfa955 as annotated tag v0.1.0 (design D14, approval A6). CI run 40234530135: `chart-publish-tag` published chart 0.1.0 (appVersion 6cfa955); the main `images` job had published image `containers/agents-schedule:6cfa955`. Both verified from the registry. The deployment pins chart 0.1.0 and image 6cfa955.

## 2026-10-31 - prompts are Jinja templates (agents-ui-improvements B2)

- Prompts render through `agents_common.templating` (v0.5.0, sandbox, StrictUndefined). Context: `schedule_name`, `fired_at` (ISO, schedule tz), `preset` (the preset id; the dispatcher knows no preset name), `now` (ISO, UTC). No `{x}` compatibility: migration `0002_prompt_jinja.sql` rewrites stored `{schedule_name}`/`{fired_at}` to the `{{ }}` form, skipping already doubled braces so a rerun changes nothing.
- A prompt that does not render at dispatch fails the run `prompt_invalid: <message>` (no retry: it would never succeed). Create/PUT do not validate the prompt; preview does (422 `invalid_prompt`, message prefixed `line N:`).
- `POST /schedules/preview` takes optional `prompt`, `schedule_name`, `preset_id` and returns `rendered` and `variables` besides `next_fires`; render runs in a thread because common renders in a child process.
- 2026-10-31 fix: create/PUT validate the prompt (syntax and size) and answer 422 `invalid_prompt`, as agents-webhook does; undefined variables still only fail at fire time. Migration 0002 first escapes stored literal `{{`, `{%`, `{#` as `{{ '{{' }}` etc. (skipping already escaped tokens and converted placeholders, so it is idempotent), then converts the old placeholders. Escaping lengthens a prompt, so a stored prompt within a few characters of 16384 could trip the length CHECK.
