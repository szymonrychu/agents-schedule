"""Apply the forward-only schema migrations of agents-schedule."""

from agents_common.db.migrate import run_migrations

from agents_schedule.settings import Settings


def migrate(settings: Settings) -> list[str]:
    """Apply pending migration files under the advisory lock; return the names applied."""
    return run_migrations(settings.database_url.get_secret_value(), "agents_schedule")
