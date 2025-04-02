"""
Google Cloud Storage.
"""

import io
from functools import cache
from typing import Optional, Tuple

from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.retry import DEFAULT_RETRY

from ..common import _split_cloud_path
from .scheme_client import SchemeClient


class GsClient(SchemeClient):
    scheme = "gs"

    def __init__(self, resource: str) -> None:
        super().__init__(resource)
        self.blob = GsClient.get_gcs_blob(resource)
        self._loaded = False

    def load(self):
        if not self._loaded:
            try:
                self.blob.reload()
                self._loaded = True
            except NotFound as exc:
                raise FileNotFoundError(self.resource) from exc

    def get_etag(self) -> Optional[str]:
        self.load()
        return self.blob.etag or self.blob.md5_hash

    def get_size(self) -> Optional[int]:
        self.load()
        return self.blob.size

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        self.load()
        self.blob.download_to_file(temp_file, checksum="md5", retry=DEFAULT_RETRY)

    def get_bytes_range(self, index: int, length: int) -> bytes:
        self.load()
        return self.blob.download_as_bytes(start=index, end=index + length - 1)

    @staticmethod
    def split_gcs_path(resource: str) -> Tuple[str, str]:
        return _split_cloud_path(resource, "gs")

    @staticmethod
    def get_gcs_blob(resource: str) -> Blob:
        gcs_client = _get_gcs_client()
        bucket_name, gcs_path = GsClient.split_gcs_path(resource)
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        return blob


@cache
def _get_gcs_client() -> storage.Client:
    try:
        client = storage.Client()
    except DefaultCredentialsError:
        client = storage.Client.create_anonymous_client()

    return client
