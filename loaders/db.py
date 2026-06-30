import os
from pathlib import Path

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

_REQUIRED = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
)


def get_connection() -> psycopg2.extensions.connection:
    missing = [var for var in _REQUIRED if not os.environ.get(var)]
    if missing:
        raise ValueError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            "Check your .env file."
        )

    return psycopg2.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
    )


if __name__ == "__main__":
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
        print(f"Connected. {version}")
    finally:
        conn.close()
