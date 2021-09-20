from cached_path.protocols.cacher import Cacher
from cached_path.protocols.http import HttpCacher
from cached_path.protocols.hf import hf_get_from_cache
from cached_path.protocols.s3 import S3Cacher
from cached_path.protocols.gs import GsCacher


def get_cacher(resource: str) -> Cacher:
    if resource.startswith("s3://"):
        return S3Cacher(resource)
    if resource.startswith("gs://"):
        return GsCacher(resource)
    return HttpCacher(resource)
