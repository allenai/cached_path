"""
Google Cloud Storage.
"""

from typing import Optional, IO, Tuple

from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
from overrides import overrides

from cached_path.common import _split_cloud_path
from cached_path.schemes.scheme_client import SchemeClient
from cached_path.tqdm import Tqdm


class GsClient(SchemeClient):
    scheme = "gs"

    def __init__(self, resource: str) -> None:
        super().__init__(resource)
        self.blob = GsClient.get_gcs_blob(resource)

    @overrides
    def get_etag(self) -> Optional[str]:
        try:
            self.blob.reload()
        except NotFound:
            raise FileNotFoundError(self.resource)
        return self.blob.etag or self.blob.md5_hash

    @overrides
    def get_resource(self, temp_file: IO) -> None:
        with Tqdm.wrapattr(
            temp_file,
            "write",
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            total=self.blob.size,
            desc="downloading",
        ) as file_obj:
            self.blob.download_to_file(file_obj, checksum="md5", retry=DEFAULT_RETRY)

    @staticmethod
    def split_gcs_path(resource: str) -> Tuple[str, str]:
        return _split_cloud_path(resource, "gs")

    @staticmethod
    def get_gcs_blob(resource: str) -> storage.blob.Blob:
        try:
            gcs_resource = storage.Client()
        except DefaultCredentialsError:
            gcs_resource = storage.Client.create_anonymous_client()
        bucket_name, gcs_path = GsClient.split_gcs_path(resource)
        bucket = gcs_resource.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        return blob
