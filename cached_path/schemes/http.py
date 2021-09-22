from typing import Optional, IO

from overrides import overrides
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from cached_path.tqdm import Tqdm
from cached_path.schemes.scheme_client import SchemeClient


def session_with_backoff() -> requests.Session:
    """
    We ran into an issue where http requests to s3 were timing out,
    possibly because we were making too many requests too quickly.
    This helper function returns a requests session that has retry-with-backoff
    built in. See
    <https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library>.
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


class HttpClient(SchemeClient):
    scheme = ("http", "https")

    @overrides
    def get_etag(self) -> Optional[str]:
        with session_with_backoff() as session:
            response = session.head(self.resource, allow_redirects=True)
        self.validate_response(response)
        return response.headers.get("ETag")

    @overrides
    def get_resource(self, temp_file: IO) -> None:
        with session_with_backoff() as session:
            response = session.get(self.resource, stream=True)
            self.validate_response(response)
            content_length = response.headers.get("Content-Length")
            total = int(content_length) if content_length is not None else None
            progress = Tqdm.tqdm(
                unit="iB", unit_scale=True, unit_divisor=1024, total=total, desc="downloading"
            )
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    progress.update(len(chunk))
                    temp_file.write(chunk)
            progress.close()

    def validate_response(self, response):
        if response.status_code == 404:
            raise FileNotFoundError(self.resource)
        response.raise_for_status()
