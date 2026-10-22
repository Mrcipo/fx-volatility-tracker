import os
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]

# Defaults match the CI service; override with POSTGRES_* to use another server.
TEST_DB = {
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "postgres"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    "POSTGRES_DB": os.environ.get("POSTGRES_TEST_DB", "fx_test"),
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
}


@pytest.fixture
def raw_schema(monkeypatch):
    """Fresh raw.* tables built from sql/*.sql; yields an open connection."""
    for key, value in TEST_DB.items():
        monkeypatch.setenv(key, value)

    conn = psycopg2.connect(
        user=TEST_DB["POSTGRES_USER"], password=TEST_DB["POSTGRES_PASSWORD"],
        dbname=TEST_DB["POSTGRES_DB"], host=TEST_DB["POSTGRES_HOST"], port=TEST_DB["POSTGRES_PORT"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS staging CASCADE;")
        for sql_file in sorted((ROOT / "sql").glob("*.sql")):
            cur.execute(sql_file.read_text(encoding="utf-8"))
    yield conn
    conn.close()
