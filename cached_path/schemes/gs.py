"""
Google Cloud Storage.
"""

from functools import wraps
from typing import Optional, IO, Tuple, Callable

from google.cloud import storage
from google.api_core.exceptions import NotFound
from overrides import overrides

from cached_path.common import _split_cloud_path
from cached_path.schemes.scheme_client import SchemeClient


class GsClient(SchemeClient):
    scheme = "gs"

    @overrides
    def get_etag(self) -> Optional[str]:
        return gcs_md5(self.resource)

    @overrides
    def get_resource(self, temp_file: IO) -> None:
        return gcs_get(self.resource, temp_file.name)


def split_gcs_path(url: str) -> Tuple[str, str]:
    return _split_cloud_path(url, "gs")


def gcs_request(func: Callable):
    """
    Wrapper function for gcs requests in order to create more helpful error
    messages.
    """

    @wraps(func)
    def wrapper(url: str, *args, **kwargs):
        try:
            return func(url, *args, **kwargs)
        except NotFound:
            raise FileNotFoundError("file {} not found".format(url))

    return wrapper


def get_gcs_blob(url: str) -> storage.blob.Blob:
    gcs_resource = storage.Client()
    bucket_name, gcs_path = split_gcs_path(url)
    bucket = gcs_resource.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob


@gcs_request
def gcs_md5(url: str) -> Optional[str]:
    """Get GCS object's md5."""
    blob = get_gcs_blob(url)
    return blob.md5_hash


@gcs_request
def gcs_get(url: str, temp_filename: str) -> None:
    """Pull a file directly from GCS."""
    blob = get_gcs_blob(url)
    blob.download_to_filename(temp_filename)
