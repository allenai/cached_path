"""
AWS S3.
"""

import io
from typing import Optional, Tuple

import boto3.session
import botocore.client
import botocore.exceptions

from ..common import _split_cloud_path
from .scheme_client import SchemeClient


class S3Client(SchemeClient):
    recoverable_errors = SchemeClient.recoverable_errors + (
        botocore.exceptions.HTTPClientError,
        botocore.exceptions.ConnectionError,
    )
    scheme = "s3"

    def __init__(self, resource: str) -> None:
        super().__init__(resource)
        bucket_name, s3_path = S3Client.split_s3_path(resource)
        session = boto3.session.Session()
        if session.get_credentials() is None:
            # Use unsigned requests.
            s3_resource = session.resource(
                "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
            )
        else:
            s3_resource = session.resource("s3")
        self.s3_object = s3_resource.Object(bucket_name, s3_path)
        self._loaded = False

    def load(self):
        if not self._loaded:
            try:
                self.s3_object.load()
                self._loaded = True
            except botocore.exceptions.ClientError as exc:
                if int(exc.response["Error"]["Code"]) == 404:
                    raise FileNotFoundError("file {} not found".format(self.resource))
                else:
                    raise

    def get_etag(self) -> Optional[str]:
        self.load()
        return self.s3_object.e_tag

    def get_size(self) -> Optional[int]:
        self.load()
        return self.s3_object.content_length

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        self.load()
        self.s3_object.download_fileobj(temp_file)

    def get_bytes_range(self, index: int, length: int) -> bytes:
        self.load()
        return self.s3_object.get(Range=f"bytes={index}-{index + length - 1}")["Body"].read()

    @staticmethod
    def split_s3_path(url: str) -> Tuple[str, str]:
        return _split_cloud_path(url, "s3")
