from typing import Optional, IO, Tuple, ClassVar, Union

import requests


class SchemeClient:
    """
    A client used for caching remote resources corresponding to URLs with a particular scheme.

    Subclasses must define the :attr:`scheme` class variable and implement
    :meth:`get_etag()` and :meth:`get_resource()`.

    .. important::
        Take care when implementing subclasses to raise the right error types
        from :meth:`get_etag()` and :meth:`get_resource()`.
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

        Returns
        -------
        ``Optional[str]``
            The ETag as a ``str`` or ``None`` if there is no ETag associated with
            the resource.

        Raises
        ------
        ``FileNotFoundError``
            If the resource doesn't exist.

        ``Connection error``
            Any error type defined in ``SchemeClient.connection_error_types`` will
            be treated as a retriable connection error.

        ``Other errors``
            Any other error type can be raised, in which case ``cached_path()`` will
            log the error and move on to try to fetch the resource without the ETag.
        """
        raise NotImplementedError

    def get_resource(self, temp_file: IO) -> None:
        """
        Download the resource to the given temporary file.

        Raises
        ------
        ``FileNotFoundError``
            If the resource doesn't exist.

        ``Connection error``
            Any error type defined in ``SchemeClient.connection_error_types`` will
            be treated as a retriable connection error.

        ``Other errors``
            Any other error type can be raised, in which case ``cached_path()`` will
            fail and propogate the error.
        """
        raise NotImplementedError
