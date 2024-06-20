from typing import Set, Type

from .gs import GsClient
from .hf import hf_get_from_cache
from .http import HttpClient
from .r2 import R2Client
from .s3 import S3Client
from .scheme_client import SchemeClient

__all__ = ["GsClient", "HttpClient", "S3Client", "R2Client", "SchemeClient", "hf_get_from_cache"]

try:
    from .beaker import BeakerClient

    __all__.append("BeakerClient")
except (ImportError, ModuleNotFoundError):
    BeakerClient = None  # type: ignore


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


for client in (HttpClient, S3Client, R2Client, GsClient):
    add_scheme_client(client)  # type: ignore
if BeakerClient is not None:
    add_scheme_client(BeakerClient)


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
