import logging
import sys
from pathlib import Path

from loaders.db import get_connection

logger = logging.getLogger(__name__)

_SOURCE_TABLE: dict[str, str] = {
    "bcra_variables":       "raw.bcra_variables",
    "bcra_tipo_cambio":     "raw.bcra_tipo_cambio",
    "dolarapi":             "raw.dolarapi_cotizaciones",
    "banxico":              "raw.banxico_series",
}

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_file(filepath: Path, table: str) -> None:
    raw_text = filepath.read_text(encoding="utf-8")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {table} (source_file, raw_payload) "
                "VALUES (%s, %s::jsonb) "
                "ON CONFLICT (source_file) DO NOTHING",
                (filepath.name, raw_text),
            )
            conn.commit()
            if cur.rowcount == 0:
                logger.info("SKIPPED  %s → %s (ya existía)", filepath.name, table)
            else:
                logger.info("INSERTED %s → %s", filepath.name, table)
    except Exception:
        conn.rollback()
        logger.error("ERROR loading %s → %s", filepath.name, table, exc_info=True)
    finally:
        conn.close()


def load_directory(source: str) -> None:
    table = _SOURCE_TABLE[source]
    directory = _PROJECT_ROOT / "data" / "raw" / source
    json_files = sorted(directory.glob("*.json"))

    if not json_files:
        logger.warning("No JSON files found in %s", directory)
        return

    for filepath in json_files:
        load_file(filepath, table)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    if len(sys.argv) < 2 or sys.argv[1] not in _SOURCE_TABLE:
        print(f"Uso: python -m loaders.load_raw <source>")
        print(f"Fuentes válidas: {', '.join(_SOURCE_TABLE)}")
        sys.exit(1)

    load_directory(sys.argv[1])
