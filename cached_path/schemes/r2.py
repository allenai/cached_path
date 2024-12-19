"""
Cloudflare R2.
"""

import io
import os
from functools import cache
from typing import Optional

import boto3.dynamodb
import boto3.session
import botocore.exceptions
from botocore.config import Config

from ..common import _split_cloud_path
from .scheme_client import SchemeClient


class R2Client(SchemeClient):
    recoverable_errors = SchemeClient.recoverable_errors + (
        botocore.exceptions.HTTPClientError,
        botocore.exceptions.ConnectionError,
    )

    scheme = "r2"

    def __init__(self, resource: str) -> None:
        SchemeClient.__init__(self, resource)
        self.bucket_name, self.path = _split_cloud_path(resource, "r2")

        self.s3 = _get_s3_resource_client()
        self.object_info = None

    def _ensure_object_info(self):
        if self.object_info is None:
            try:
                self.object_info = self.s3.head_object(Bucket=self.bucket_name, Key=self.path)
            except botocore.exceptions.ClientError as e:
                if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                    raise FileNotFoundError(f"File {self.resource} not found") from e
                raise

    def get_etag(self) -> Optional[str]:
        self._ensure_object_info()
        assert self.object_info is not None
        return self.object_info.get("ETag")

    def get_size(self) -> Optional[int]:
        self._ensure_object_info()
        assert self.object_info is not None
        return self.object_info.get("ContentLength")

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        self._ensure_object_info()
        self.s3.download_fileobj(Fileobj=temp_file, Bucket=self.bucket_name, Key=self.path)

    def get_bytes_range(self, index: int, length: int) -> bytes:
        self._ensure_object_info()
        response = self.s3.get_object(
            Bucket=self.bucket_name, Key=self.path, Range=f"bytes={index}-{index+length-1}"
        )
        return response["Body"].read()


@cache
def _get_s3_resource_client():
    # find credentials
    endpoint_url = os.environ.get("R2_ENDPOINT_URL")
    if endpoint_url is None:
        raise ValueError(
            "R2 endpoint url is not set. Did you forget to set the 'R2_ENDPOINT_URL' env var?"
        )
    profile_name = os.environ.get("R2_PROFILE")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    if access_key_id is not None and secret_access_key is not None:
        session_kwargs = {
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
        }
    elif profile_name is not None:
        session_kwargs = {"profile_name": profile_name}
    else:
        raise ValueError(
            "To authenticate for R2, you either have to set the 'R2_PROFILE' env var and set up this profile, "
            "or set R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY."
        )

    s3_session = boto3.session.Session(**session_kwargs)
    return s3_session.client(
        service_name="s3",
        endpoint_url=endpoint_url,
        region_name="auto",
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
