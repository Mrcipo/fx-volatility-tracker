import json

import pytest

from loaders import load_raw
from loaders.db import get_connection
from tests.conftest import TEST_DB


def _count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def test_get_connection_reports_missing_variables(monkeypatch):
    for key, value in TEST_DB.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    with pytest.raises(ValueError, match=r"variable\(s\): POSTGRES_PASSWORD, POSTGRES_HOST\."):
        get_connection()


def test_load_file_inserts_payload_as_jsonb(raw_schema, tmp_path):
    payload = [{"casa": "blue", "compra": 1180, "venta": 1200}]
    path = tmp_path / "cotizaciones_20260101_120000.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    load_raw.load_file(path, "raw.dolarapi_cotizaciones")

    with raw_schema.cursor() as cur:
        cur.execute("SELECT source_file, raw_payload FROM raw.dolarapi_cotizaciones")
        assert cur.fetchall() == [(path.name, payload)]


def test_load_file_is_idempotent(raw_schema, tmp_path):
    path = tmp_path / "variables_20260101_120000.json"
    path.write_text('{"results": []}', encoding="utf-8")

    load_raw.load_file(path, "raw.bcra_variables")
    load_raw.load_file(path, "raw.bcra_variables")

    assert _count(raw_schema, "raw.bcra_variables") == 1


def test_invalid_json_is_rolled_back_and_logged(raw_schema, tmp_path, caplog):
    path = tmp_path / "tipo_cambio_20260101_120000.json"
    path.write_text("{not json", encoding="utf-8")

    load_raw.load_file(path, "raw.banxico_series")

    assert _count(raw_schema, "raw.banxico_series") == 0
    assert "ERROR loading" in caplog.text


def test_load_directory_loads_every_json_of_the_source(raw_schema, tmp_path, monkeypatch):
    monkeypatch.setattr(load_raw, "_PROJECT_ROOT", tmp_path)
    folder = tmp_path / "data" / "raw" / "bcra_tipo_cambio"
    folder.mkdir(parents=True)
    for i in range(3):
        (folder / f"tipo_cambio_2026010{i + 1}_120000.json").write_text('{"results": []}', encoding="utf-8")
    (folder / "notes.txt").write_text("ignored", encoding="utf-8")

    load_raw.load_directory("bcra_tipo_cambio")

    assert _count(raw_schema, "raw.bcra_tipo_cambio") == 3


def test_load_directory_with_no_files_only_warns(raw_schema, tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(load_raw, "_PROJECT_ROOT", tmp_path)

    load_raw.load_directory("dolarapi")

    assert "No JSON files found" in caplog.text


def test_every_source_maps_to_a_raw_table(raw_schema):
    with raw_schema.cursor() as cur:
        for table in load_raw._SOURCE_TABLE.values():
            cur.execute("SELECT to_regclass(%s)", (table,))
            assert cur.fetchone()[0] is not None, table
