import io
from typing import Dict, Optional

import requests  # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

from .scheme_client import SchemeClient

RECOVERABLE_SERVER_ERROR_CODES = (502, 503, 504)


class RecoverableServerError(requests.exceptions.HTTPError):
    """
    Server returned one of `RECOVERABLE_SERVER_ERROR_CODES`.
    """


def session_with_backoff(headers: Optional[Dict[str, str]] = None) -> requests.Session:
    """
    We ran into an issue where http requests to s3 were timing out,
    possibly because we were making too many requests too quickly.
    This helper function returns a requests session that has retry-with-backoff
    built in. See
    <https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library>.

    Parameters
    ----------
    headers : Optional[Dict[str, str]], optional
        Custom headers to add to all requests, by default None
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=RECOVERABLE_SERVER_ERROR_CODES)
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Add custom headers if provided
    if headers:
        session.headers.update(headers)

    return session


class HttpClient(SchemeClient):
    scheme = ("http", "https")
    recoverable_errors = SchemeClient.recoverable_errors + (RecoverableServerError,)

    def __init__(self, resource: str, headers: Optional[Dict[str, str]] = None) -> None:
        """
        Initialize an HTTP client for the given resource.

        Parameters
        ----------
        resource : str
            The URL to the resource.
        headers : Optional[Dict[str, str]], optional
            Custom headers to add to all requests, by default None.
            Example: {"Authorization": "Bearer YOUR_TOKEN"} for private resources.
        """
        super().__init__(resource)
        self._head_response = None
        self.headers = headers or {}

    @property
    def head_response(self):
        if self._head_response is None:
            try:
                with session_with_backoff(self.headers) as session:
                    response = session.head(self.resource, allow_redirects=True)
            except MaxRetryError as e:
                raise RecoverableServerError(e.reason)
            self.validate_response(response)
            self._head_response = response
            return self._head_response
        else:
            return self._head_response

    def get_etag(self) -> Optional[str]:
        return self.head_response.headers.get("ETag")

    def get_size(self) -> Optional[int]:
        content_length = self.head_response.headers.get("Content-Length")
        return None if content_length is None else int(content_length)

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        with session_with_backoff(self.headers) as session:
            try:
                response = session.get(self.resource, stream=True)
            except MaxRetryError as e:
                raise RecoverableServerError(e.reason)
            self.validate_response(response)
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    temp_file.write(chunk)

    # TODO (epwalsh): There may be a better way to do this, but...
    # HTTP range requests don't necessarily match our expectation in this context. For example, the range might
    # implicitly include header data, but we usually don't care about that. The server might also
    # interpret the range relative to an encoding of the data, not the underlying data itself.
    # So to avoid unexpected behavior we resort to the default behavior of downloading the whole file
    # and returning the desired bytes range from the cached content.
    #  def get_bytes_range(self, index: int, length: int) -> bytes:
    #      with session_with_backoff() as session:
    #          try:
    #              response = session.get(
    #                  self.resource, headers={"Range": f"bytes={index}-{index+length-1}"}
    #              )
    #          except MaxRetryError as e:
    #              raise RecoverableServerError(e.reason)
    #          self.validate_response(response)
    #          # 'content' might contain the full file if the server doesn't support the "Range" header.
    #          return response.content[:length]

    def validate_response(self, response):
        if response.status_code == 404:
            raise FileNotFoundError(self.resource)
        if response.status_code in RECOVERABLE_SERVER_ERROR_CODES:
            raise RecoverableServerError(response=response)
        response.raise_for_status()
