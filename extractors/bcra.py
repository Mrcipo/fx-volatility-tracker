import logging
from datetime import date, timedelta

from extractors.base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class BCRAClient(BaseAPIClient):
    def __init__(self) -> None:
        super().__init__(
            base_url="https://api.bcra.gob.ar/estadisticas/v4.0",
            source_name="bcra",
        )

    def fetch_principales_variables(self) -> list:
        data = self.get("/Monetarias")
        self.save_raw(data, "variables.json")
        return data

    # id_variable=4 → Tipo de Cambio Minorista ($ por USD)
    # id_variable=5 → Tipo de Cambio Mayorista ($ por USD) - no usado
    #   en este pipeline, se compara contra paralelos minoristas
    def fetch_tipo_cambio(self, id_variable: int = 4) -> dict | list:
        today = date.today()
        fecha_desde = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        fecha_hasta = today.strftime("%Y-%m-%d")
        data = self.get(
            f"/Monetarias/{id_variable}",
            params={"desde": fecha_desde, "hasta": fecha_hasta},
        )
        self.save_raw(data, "tipo_cambio.json")
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    client = BCRAClient()

    variables = client.fetch_principales_variables()
    resultados = variables.get("results", variables) if isinstance(variables, dict) else variables
    logger.info("fetch_principales_variables → %d registros", len(resultados))

    tipo_cambio = client.fetch_tipo_cambio()
    cantidad_detalle = len(tipo_cambio["results"][0]["detalle"]) if tipo_cambio.get("results") else 0
    logging.info(f"fetch_tipo_cambio → {cantidad_detalle} registros")
