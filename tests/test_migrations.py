import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from agents_schedule.db.migrate import migrate


def add_schedule(conn, name="nightly") -> uuid.UUID:
    sid = uuid.uuid4()
    conn.execute(
        "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, armed_at) "
        "VALUES (%s, %s, '0 3 * * *', 'UTC', %s, 'go', now())",
        (sid, name, uuid.uuid4()),
    )
    return sid


def add_run(conn, schedule_id, fire="2026-01-01T00:00:00Z", **cols) -> None:
    values = {"trigger": "scheduled", "status": "new", **cols}
    names = ", ".join(["id", "schedule_id", "fire_time", *values])
    marks = ", ".join(["%s"] * (3 + len(values)))
    conn.execute(
        f"INSERT INTO runs ({names}) VALUES ({marks})",
        (uuid.uuid4(), schedule_id, fire, *values.values()),
    )


def test_migrate_idempotent(empty_db_url, settings_for):
    settings = settings_for(empty_db_url)
    assert migrate(settings) == [
        "0001_schedules.sql",
        "0002_prompt_jinja.sql",
        "0003_run_not_before.sql",
        "0004_chain.sql",
    ]
    assert migrate(settings) == []
    with psycopg.connect(empty_db_url) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
        }
    assert {"schedules", "runs"} <= tables


def test_concurrent_migrate_applies_once(empty_db_url, settings_for):
    settings = settings_for(empty_db_url)
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(lambda _: migrate(settings), range(4)))
    assert sorted(name for applied in results for name in applied) == [
        "0001_schedules.sql",
        "0002_prompt_jinja.sql",
        "0003_run_not_before.sql",
        "0004_chain.sql",
    ]


def test_runs_unique_schedule_fire_time(conn):
    sid = add_schedule(conn)
    add_run(conn, sid)
    with pytest.raises(psycopg.errors.UniqueViolation):
        add_run(conn, sid)
    add_run(conn, sid, trigger="manual")
    add_run(conn, sid, trigger="manual")
    add_run(conn, sid, fire="2026-01-02T00:00:00Z")


@pytest.mark.parametrize(
    "cols", [{"status": "done"}, {"trigger": "cron"}, {"status": None}, {"trigger": None}]
)
def test_runs_checks(conn, cols):
    sid = add_schedule(conn)
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation)):
        add_run(conn, sid, **cols)


@pytest.mark.parametrize("status", ["new", "in-progress", "handled", "failed", "ignored"])
def test_runs_status_values_accepted(conn, status):
    add_run(conn, add_schedule(conn), status=status)


def test_schedule_delete_cascades(conn):
    sid = add_schedule(conn)
    add_run(conn, sid)
    conn.execute("DELETE FROM schedules WHERE id = %s", (sid,))
    assert conn.execute("SELECT count(*) FROM runs").fetchone()[0] == 0


def test_runs_reuse_check(conn):
    sid = add_schedule(conn)
    add_run(conn, sid, fire="2026-01-01T00:00:00Z")
    add_run(conn, sid, fire="2026-01-02T00:00:00Z", reuse="delivered")
    add_run(conn, sid, fire="2026-01-03T00:00:00Z", reuse="fallback")
    with pytest.raises(psycopg.errors.CheckViolation):
        add_run(conn, sid, fire="2026-01-04T00:00:00Z", reuse="other")


def test_schedule_name_and_prompt_bounds(conn):
    add_schedule(conn, "a")
    with pytest.raises(psycopg.errors.UniqueViolation):
        add_schedule(conn, "a")
    with pytest.raises(psycopg.errors.CheckViolation):
        add_schedule(conn, "")
    with pytest.raises(psycopg.errors.CheckViolation):
        add_schedule(conn, "x" * 101)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, armed_at) "
            "VALUES (%s, 'p', '* * * * *', 'UTC', %s, '', now())",
            (uuid.uuid4(), uuid.uuid4()),
        )


def test_prompt_jinja_migration_converts_placeholders(empty_db_url, settings_for):
    from importlib import resources

    sql_text = (
        resources.files("agents_schedule") / "migrations" / "0002_prompt_jinja.sql"
    ).read_text()
    migrate(settings_for(empty_db_url))
    cases = {
        "old": "Run {schedule_name} at {fired_at}; keep {other} {} {schedule_name",
        "done": "Run {{ schedule_name }} at {{ fired_at }}",
        "tight": "{{schedule_name}} {{fired_at}}",
        "plain": "no placeholders",
        "literal": "a {{x}} b {% c %} d {# e #} {schedule_name}",
    }
    expected = {
        "old": "Run {{ schedule_name }} at {{ fired_at }}; keep {other} {} {schedule_name",
        "done": "Run {{ schedule_name }} at {{ fired_at }}",
        "tight": "{{ '{{' }}schedule_name}} {{ '{{' }}fired_at}}",
        "plain": "no placeholders",
        "literal": ("a {{ '{{' }}x}} b {{ '{%' }} c %} d {{ '{#' }} e #} {{ schedule_name }}"),
    }
    with psycopg.connect(empty_db_url, autocommit=True) as conn:
        for name, prompt in cases.items():
            conn.execute(
                "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, armed_at) "
                "VALUES (%s, %s, '0 3 * * *', 'UTC', %s, %s, now())",
                (uuid.uuid4(), name, uuid.uuid4(), prompt),
            )
        for _ in range(2):  # the second run is a no-op
            conn.execute(sql_text)
            got = dict(conn.execute("SELECT name, prompt FROM schedules").fetchall())
            assert got == expected


def test_runs_not_before_column(conn):
    sid = add_schedule(conn)
    add_run(conn, sid)
    assert conn.execute("SELECT not_before FROM runs").fetchone() == (None,)
    conn.execute("UPDATE runs SET not_before = now()")
    row = conn.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'runs' AND column_name = 'not_before'"
    ).fetchone()
    assert row == ("timestamp with time zone", "YES")


def test_chained_trigger_and_once_guard(conn):
    sid = add_schedule(conn)
    add_run(conn, sid, trigger="chained", chain_source="schedule", chain_ref="r1")
    add_run(
        conn,
        sid,
        fire="2026-01-02T00:00:00Z",
        trigger="chained",
        chain_source="schedule",
        chain_ref="r2",
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        add_run(
            conn,
            sid,
            fire="2026-01-03T00:00:00Z",
            trigger="chained",
            chain_source="schedule",
            chain_ref="r1",
        )
    other = add_schedule(conn, "other")
    add_run(conn, other, trigger="chained", chain_source="schedule", chain_ref="r1")
    add_run(
        conn,
        other,
        fire="2026-01-04T00:00:00Z",
        trigger="chained",
        chain_source="webhook",
        chain_ref="r1",
    )


@pytest.mark.parametrize(
    "cols", [{"chain_status": "done"}, {"chain_source": "cron"}, {"chain_depth": None}]
)
def test_chain_columns_checked(conn, cols):
    sid = add_schedule(conn)
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation)):
        add_run(conn, sid, **cols)


def test_chain_columns_defaults_and_accepted(conn):
    sid = add_schedule(conn)
    for i, status in enumerate(("pending", "fired", "skipped", "expired")):
        add_run(conn, sid, fire=f"2026-01-0{i + 1}T00:00:00Z", chain_status=status)
    row = conn.execute(
        "SELECT chain_depth, then_schedule_ids, result_text FROM runs LIMIT 1"
    ).fetchone()
    assert row == (0, None, None)


def test_schedule_chains_constraints_and_cascade(conn):
    a, b, c = add_schedule(conn, "a"), add_schedule(conn, "b"), add_schedule(conn, "c")
    ins = (
        "INSERT INTO schedule_chains (schedule_id, next_schedule_id, position) VALUES (%s, %s, %s)"
    )
    conn.execute(ins, (a, b, 0))
    conn.execute(ins, (a, c, 1))
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(ins, (a, b, 2))
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(ins, (a, a, 0))
    conn.execute("DELETE FROM schedules WHERE id = %s", (b,))
    assert conn.execute("SELECT next_schedule_id FROM schedule_chains").fetchall() == [(c,)]
    conn.execute("DELETE FROM schedules WHERE id = %s", (a,))
    assert conn.execute("SELECT count(*) FROM schedule_chains").fetchone()[0] == 0
