from cached_path.schemes.scheme_client import SchemeClient
from cached_path.schemes.http import HttpClient
from cached_path.schemes.hf import hf_get_from_cache
from cached_path.schemes.s3 import S3Client
from cached_path.schemes.gs import GsClient


def get_scheme_client(resource: str) -> SchemeClient:
    if resource.startswith("s3://"):
        return S3Client(resource)
    if resource.startswith("gs://"):
        return GsClient(resource)
    return HttpClient(resource)
