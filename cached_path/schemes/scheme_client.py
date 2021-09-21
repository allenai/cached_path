from typing import Optional, IO, Tuple, ClassVar, Union

import requests


class SchemeClient:
    """
    A client used for caching remote resources corresponding to URLs with a particular scheme.

    Subclasses must define the :attr:`scheme` class variable and implement
    :meth:`get_etag()` and :meth:`get_resource`.
    """

    connection_error_types: ClassVar[Tuple[BaseException, ...]] = (
        requests.exceptions.ConnectionError,
    )
    """
    Subclasses can override this to define error types that will be treated as
    retriable connection errors.
    """

    scheme: ClassVar[Union[str, Tuple[str, ...]]] = tuple()
    """
    The scheme or schemes that the client will be used for (e.g. "http").
    """

    def __init__(self, resource: str) -> None:
        self.resource = resource

    def get_etag(self) -> Optional[str]:
        """
        Get the Etag or an equivalent version identifier associated with the resource.
        """
        raise NotImplementedError

    def get_resource(self, temp_file: IO) -> None:
        """
        Download the resource to the given temporary file.
        """
        raise NotImplementedError
