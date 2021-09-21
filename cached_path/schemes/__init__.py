from typing import Set, Type

from cached_path.schemes.scheme_client import SchemeClient
from cached_path.schemes.http import HttpClient
from cached_path.schemes.hf import hf_get_from_cache
from cached_path.schemes.s3 import S3Client
from cached_path.schemes.gs import GsClient


_SCHEME_TO_CLIENT = {}


def add_scheme_client(client: Type[SchemeClient]) -> None:
    """
    Add a new :class:`SchemeClient`.

    This can be used to extend :func:`cached_path.cached_path()` to handle custom schemes, or handle
    existing schemes differently.
    """
    global _SCHEME_TO_CLIENT
    if isinstance(client.scheme, tuple):
        for scheme in client.scheme:
            _SCHEME_TO_CLIENT[scheme] = client
    elif isinstance(client.scheme, str):
        _SCHEME_TO_CLIENT[client.scheme] = client
    else:
        raise ValueError(f"Unexpected type for {client} scheme: {client.scheme}")


for client in (HttpClient, S3Client, GsClient):
    add_scheme_client(client)


def get_scheme_client(resource: str) -> SchemeClient:
    """
    Get the right client for the given resource.
    """
    maybe_scheme = resource.split("://")[0]
    return _SCHEME_TO_CLIENT.get(maybe_scheme, HttpClient)(resource)


def get_supported_schemes() -> Set[str]:
    """
    Return all supported URL schemes.
    """
    return set(_SCHEME_TO_CLIENT.keys()) | {"hf"}
