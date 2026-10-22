import json
from datetime import date, timedelta
from unittest import mock

import pytest
import requests

from extractors import BanxicoClient, BCRAClient, DolarAPIClient
from extractors.base_client import APIClientError, BaseAPIClient


def _response(payload, status=200, url="https://example.test"):
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps(payload).encode()
    resp.url = url
    return resp


@pytest.fixture(autouse=True)
def tmp_cwd(tmp_path, monkeypatch):
    # save_raw writes relative to the working directory (data/raw/<source>/)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _patch_get(client, payload=None, status=200, side_effect=None):
    return mock.patch.object(
        client.session, "get",
        return_value=None if side_effect else _response(payload, status),
        side_effect=side_effect,
    )


# --- BaseAPIClient ---------------------------------------------------------

def test_get_joins_base_url_and_endpoint():
    client = BaseAPIClient("https://api.test/v1/", "src")
    with _patch_get(client, {"ok": True}) as get:
        assert client.get("/items", params={"a": 1}) == {"ok": True}
    get.assert_called_once_with("https://api.test/v1/items", params={"a": 1}, timeout=10)


def test_http_errors_become_api_client_error_with_status():
    client = BaseAPIClient("https://api.test", "src")
    with _patch_get(client, {"error": "nope"}, status=404):
        with pytest.raises(APIClientError) as exc:
            client.get("/missing")
    assert exc.value.status_code == 404
    assert exc.value.url == "https://api.test/missing"


def test_network_errors_become_api_client_error_with_status_zero():
    client = BaseAPIClient("https://api.test", "src")
    with _patch_get(client, side_effect=requests.ConnectionError("down")):
        with pytest.raises(APIClientError) as exc:
            client.get("/items")
    assert exc.value.status_code == 0
    assert "down" in str(exc.value)


def test_retries_are_configured_for_transient_errors():
    client = BaseAPIClient("https://api.test", "src")
    retry = client.session.get_adapter("https://api.test").max_retries
    assert retry.total == 5
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)


def test_save_raw_writes_timestamped_json(tmp_cwd):
    client = BaseAPIClient("https://api.test", "dolarapi")
    path = client.save_raw([{"casa": "blue", "venta": 1200}], "cotizaciones.json")

    assert path.parent == tmp_cwd.joinpath("data", "raw", "dolarapi").relative_to(tmp_cwd)
    assert path.name.startswith("cotizaciones_") and path.suffix == ".json"
    assert json.loads((tmp_cwd / path).read_text(encoding="utf-8")) == [{"casa": "blue", "venta": 1200}]


# --- Source clients --------------------------------------------------------

def test_dolarapi_fetches_and_saves_cotizaciones(tmp_cwd):
    client = DolarAPIClient()
    payload = [{"casa": "oficial", "compra": 1000, "venta": 1050}]
    with _patch_get(client, payload) as get:
        assert client.fetch_cotizaciones() == payload
    assert get.call_args.args[0] == "https://dolarapi.com/v1/dolares"
    assert len(list((tmp_cwd / "data/raw/dolarapi").glob("cotizaciones_*.json"))) == 1


def test_bcra_tipo_cambio_requests_last_week_and_saves_in_its_folder(tmp_cwd):
    client = BCRAClient()
    payload = {"results": [{"idVariable": 4, "detalle": []}]}
    with _patch_get(client, payload) as get:
        client.fetch_tipo_cambio()

    url, params = get.call_args.args[0], get.call_args.kwargs["params"]
    today = date.today()
    assert url == "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias/4"
    assert params == {"desde": (today - timedelta(days=7)).isoformat(), "hasta": today.isoformat()}
    assert list((tmp_cwd / "data/raw/bcra_tipo_cambio").glob("tipo_cambio_*.json"))
    assert client.source_name == "bcra"


def test_bcra_variables_saved_under_bcra_variables(tmp_cwd):
    client = BCRAClient()
    with _patch_get(client, {"results": []}) as get:
        client.fetch_principales_variables()
    assert get.call_args.args[0].endswith("/Monetarias")
    assert list((tmp_cwd / "data/raw/bcra_variables").glob("variables_*.json"))
    assert client.source_name == "bcra"


def test_bcra_restores_source_name_when_saving_fails():
    client = BCRAClient()
    with _patch_get(client, {"results": []}), \
         mock.patch.object(client, "save_raw", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            client.fetch_tipo_cambio()
    assert client.source_name == "bcra"


def test_banxico_sends_token_and_date_range(tmp_cwd):
    client = BanxicoClient(token="secret-token")
    assert client.session.headers["Bmx-Token"] == "secret-token"

    with _patch_get(client, {"bmx": {"series": []}}) as get:
        client.fetch_tipo_cambio(dias_atras=3)

    today = date.today()
    expected = (
        "https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF60653,SF46410/datos/"
        f"{(today - timedelta(days=3)).isoformat()}/{today.isoformat()}"
    )
    assert get.call_args.args[0] == expected
    assert list((tmp_cwd / "data/raw/banxico").glob("tipo_cambio_*.json"))
