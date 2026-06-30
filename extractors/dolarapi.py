import logging

from extractors.base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class DolarAPIClient(BaseAPIClient):
    def __init__(self) -> None:
        super().__init__(
            base_url="https://dolarapi.com/v1",
            source_name="dolarapi",
        )

    def fetch_cotizaciones(self) -> list:
        data = self.get("/dolares")
        self.save_raw(data, "cotizaciones.json")
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    client = DolarAPIClient()

    cotizaciones = client.fetch_cotizaciones()
    logger.info("fetch_cotizaciones → %d registros", len(cotizaciones))
