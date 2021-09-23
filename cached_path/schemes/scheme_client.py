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

    recoverable_errors: ClassVar[Tuple[BaseException, ...]] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )
    """
    Subclasses can override this to define error types that will be treated as recoverable.

    If ``cached_path()`` catches of one these errors while calling :meth:`get_etag()`, it
    will log a warning and return the latest cached version if there is one, otherwise
    it will propogate the error.
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

        ``Recoverable error``
            Any error type defined in ``SchemeClient.recoverable_errors`` will
            be treated as a recoverable error.

            This means that when of these is caught by ``cached_path()``,
            it will look for cached versions of the given resource and return the
            latest version if there are any.

            Otherwise the error is propogated.

        ``Other errors``
            Any other error type can be raised. These errors will be treated non-recoverable
            and will be propogated immediately by ``cached_path()``.
        """
        raise NotImplementedError

    def get_resource(self, temp_file: IO) -> None:
        """
        Download the resource to the given temporary file.

        Raises
        ------
        ``FileNotFoundError``
            If the resource doesn't exist.

        ``Other errors``
            Any other error type can be raised. These errors will be treated non-recoverable
            and will be propogated immediately by ``cached_path()``.
        """
        raise NotImplementedError
