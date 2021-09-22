"""
AWS S3.
"""

from typing import Optional, IO, Tuple

import boto3
import botocore
from overrides import overrides

from cached_path.common import _split_cloud_path
from cached_path.schemes.scheme_client import SchemeClient
from cached_path.tqdm import Tqdm


class S3Client(SchemeClient):
    connection_error_types = SchemeClient.connection_error_types + (
        botocore.exceptions.EndpointConnectionError,
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

    @overrides
    def get_etag(self) -> Optional[str]:
        try:
            self.s3_object.load()
        except botocore.exceptions.ClientError as exc:
            if int(exc.response["Error"]["Code"]) == 404:
                raise FileNotFoundError("file {} not found".format(self.resource))
            else:
                raise
        return self.s3_object.e_tag

    @overrides
    def get_resource(self, temp_file: IO) -> None:
        progress = Tqdm.tqdm(
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            total=self.s3_object.content_length,
            desc="downloading",
        )
        self.s3_object.download_fileobj(temp_file, Callback=progress.update)
        progress.close()

    @staticmethod
    def split_s3_path(url: str) -> Tuple[str, str]:
        return _split_cloud_path(url, "s3")
