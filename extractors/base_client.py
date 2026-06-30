import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APIClientError(Exception):
    def __init__(self, status_code: int, url: str, message: str = ""):
        self.status_code = status_code
        self.url = url
        super().__init__(message or f"HTTP {status_code} for URL: {url}")


class BaseAPIClient:
    def __init__(self, base_url: str, source_name: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.source_name = source_name

        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, endpoint: str, params: dict | None = None) -> dict | list:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info("GET %s | params=%s", url, params)
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise APIClientError(
                status_code=exc.response.status_code,
                url=url,
            ) from exc
        except requests.RequestException as exc:
            raise APIClientError(status_code=0, url=url, message=str(exc)) from exc
        return response.json()

    def save_raw(self, data: dict | list, filename: str) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        stem, _, ext = filename.rpartition(".")
        ext = ext or "json"
        timestamped_name = f"{stem}_{timestamp}.{ext}"

        dest = Path("data") / "raw" / self.source_name / timestamped_name
        dest.parent.mkdir(parents=True, exist_ok=True)

        dest.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved raw data → %s", dest)
        return dest


if __name__ == "__main__":
    # Example usage (requires network access):
    #
    # logging.basicConfig(level=logging.INFO)
    # client = BaseAPIClient(
    #     base_url="https://api.bcra.gob.ar",
    #     source_name="bcra",
    # )
    # data = client.get("/estadisticas/v3.0/monetarias")
    # client.save_raw(data, "variables.json")
    # => writes data/raw/bcra/variables_20240115_143022.json
    pass
