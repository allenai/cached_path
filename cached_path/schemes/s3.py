"""
AWS S3.
"""

from functools import wraps
from typing import Optional, IO, Tuple, Callable

import boto3
import botocore
from overrides import overrides

from cached_path.common import _split_cloud_path
from cached_path.schemes.cacher import Cacher


class S3Cacher(Cacher):
    ConnectionErrorTypes = Cacher.ConnectionErrorTypes + (
        botocore.exceptions.EndpointConnectionError,
    )

    @overrides
    def get_etag(self) -> Optional[str]:
        return s3_etag(self.resource)

    @overrides
    def get_resource(self, temp_file: IO) -> None:
        return s3_get(self.resource, temp_file)


def split_s3_path(url: str) -> Tuple[str, str]:
    return _split_cloud_path(url, "s3")


def s3_request(func: Callable):
    """
    Wrapper function for s3 requests in order to create more helpful error
    messages.
    """

    @wraps(func)
    def wrapper(url: str, *args, **kwargs):
        try:
            return func(url, *args, **kwargs)
        except botocore.exceptions.ClientError as exc:
            if int(exc.response["Error"]["Code"]) == 404:
                raise FileNotFoundError("file {} not found".format(url))
            else:
                raise

    return wrapper


def get_s3_resource():
    session = boto3.session.Session()
    if session.get_credentials() is None:
        # Use unsigned requests.
        s3_resource = session.resource(
            "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
        )
    else:
        s3_resource = session.resource("s3")
    return s3_resource


@s3_request
def s3_etag(url: str) -> Optional[str]:
    """Check ETag on S3 object."""
    s3_resource = get_s3_resource()
    bucket_name, s3_path = split_s3_path(url)
    s3_object = s3_resource.Object(bucket_name, s3_path)
    return s3_object.e_tag


@s3_request
def s3_get(url: str, temp_file: IO) -> None:
    """Pull a file directly from S3."""
    s3_resource = get_s3_resource()
    bucket_name, s3_path = split_s3_path(url)
    s3_resource.Bucket(bucket_name).download_fileobj(s3_path, temp_file)
