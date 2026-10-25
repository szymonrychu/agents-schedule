import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest
from psycopg import sql

from agents_schedule.settings import Settings

PACKAGE = "agents_schedule"


@pytest.fixture
def db_url(migrated_db: Callable[[str], str]) -> str:
    """A fresh database cloned from the migrated template."""
    return migrated_db(PACKAGE)


@pytest.fixture
def conn(db_url: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(db_url, autocommit=True) as connection:
        yield connection


@pytest.fixture
def empty_db_url(pg_url: str) -> Iterator[str]:
    """A fresh database with no schema at all."""
    name = f"e_{uuid.uuid4().hex}"
    with psycopg.connect(pg_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        yield f"{pg_url.rsplit('/', 1)[0]}/{name}"
        admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )


@pytest.fixture
def settings_for() -> Callable[[str], Settings]:
    def make(url: str) -> Settings:
        return Settings(
            database_url=url,
            orchestrator_url="http://orchestrator.invalid",
            orchestrator_token="t",
        )

    return make
