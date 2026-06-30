import logging
from datetime import date, timedelta

from extractors.base_client import BaseAPIClient

logger = logging.getLogger(__name__)

_SERIES = "SF60653,SF46410"


class BanxicoClient(BaseAPIClient):
    def __init__(self, token: str) -> None:
        super().__init__(
            base_url="https://www.banxico.org.mx/SieAPIRest/service/v1",
            source_name="banxico",
        )
        self.session.headers.update({"Bmx-Token": token})

    def fetch_tipo_cambio(self, dias_atras: int = 7) -> dict | list:
        today = date.today()
        fecha_ini = (today - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
        fecha_fin = today.strftime("%Y-%m-%d")
        data = self.get(f"/series/{_SERIES}/datos/{fecha_ini}/{fecha_fin}")
        self.save_raw(data, "tipo_cambio.json")
        return data


if __name__ == "__main__":
    import logging
    import os
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    load_dotenv()

    token = os.environ["BMX_TOKEN"]
    client = BanxicoClient(token=token)

    tipo_cambio = client.fetch_tipo_cambio()
    series = tipo_cambio.get("bmx", {}).get("series", []) if isinstance(tipo_cambio, dict) else tipo_cambio
    total = sum(len(s.get("datos", [])) for s in series)
    logger.info("fetch_tipo_cambio → %d series, %d registros totales", len(series), total)
