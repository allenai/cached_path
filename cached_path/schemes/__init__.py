import logging
from typing import Dict, Optional, Set, Type

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

logger = logging.getLogger(__name__)


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


def get_scheme_client(resource: str, headers: Optional[Dict[str, str]] = None) -> SchemeClient:
    """
    Get the right client for the given resource.

    Parameters
    ----------
    resource : str
        The URL or path to the resource.
    headers : Optional[Dict[str, str]], optional
        Custom headers to add to HTTP requests, by default None.
        Example: {"Authorization": "Bearer YOUR_TOKEN"} for private resources.
        Only used for HTTP/HTTPS resources.

    Returns
    -------
    SchemeClient
        The appropriate client for the resource.
    """
    maybe_scheme = resource.split("://")[0]
    client_cls = _SCHEME_TO_CLIENT.get(maybe_scheme, HttpClient)

    if headers:
        if issubclass(client_cls, HttpClient):
            return client_cls(resource, headers=headers)
        else:
            msg = f"Headers are only supported for HTTP/HTTPS resources, got {client_cls.__name__}"
            logger.warning(msg)

    return client_cls(resource)


def get_supported_schemes() -> Set[str]:
    """
    Return all supported URL schemes.
    """
    return set(_SCHEME_TO_CLIENT.keys()) | {"hf"}
