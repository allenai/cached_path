from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

from ._cached_path import cached_path, get_from_cache
from .common import PathOrStr
from .schemes import get_scheme_client, get_supported_schemes

if TYPE_CHECKING:
    from rich.progress import Progress


def get_bytes_range(
    url_or_filename: PathOrStr,
    index: int,
    length: int,
    cache_dir: Optional[PathOrStr] = None,
    extract_archive: bool = False,
    force_extract: bool = False,
    quiet: bool = False,
    progress: Optional["Progress"] = None,
) -> bytes:
    """
    Get a range of up to ``length`` bytes starting at ``index``.

    In some cases the entire file may need to be downloaded, such as when the server does not support
    a range download or when you're trying to get a bytes range from a file within an archive.

    .. caution::
        You may get less than ``length`` bytes sometimes, such as when fetching a range from an HTTP
        resource starting at 0 since headers will be omitted in the bytes returned.

    Parameters
    ----------

    url_or_filename :
        A URL or path to parse and possibly download.

    index :
        The index of the byte to start at.

    length :
        The number of bytes to read.

    cache_dir :
        The directory to cache downloads. If not specified, the global default cache directory
        will be used (``~/.cache/cached_path``). This can be set to something else with
        :func:`set_cache_dir()`.

        This is only relevant when the bytes range cannot be obtained directly from the resource.

    extract_archive :
        Set this to ``True`` when you want to get a bytes range from a file within an archive.
        In this case the ``url_or_filename`` must contain an "!" followed by the relative path of the file
        within the archive, e.g. "s3://my-archive.tar.gz!my-file.txt".

        Note that the entire archive has to be downloaded in this case.

    force_extract :
        If ``True`` and the resource is a file within an archive (when the path contains an "!" and
        ``extract_archive=True``), it will be extracted regardless of whether or not the extracted
        directory already exists.

        .. caution::
            Use this flag with caution! This can lead to race conditions if used
            from multiple processes on the same file.

    quiet :
        If ``True``, progress displays won't be printed.

        This is only relevant when the bytes range cannot be obtained directly from the resource.

    progress :
        A custom progress display to use. If not set and ``quiet=False``, a default display
        from :func:`~cached_path.get_download_progress()` will be used.

        This is only relevant when the bytes range cannot be obtained directly from the resource.
    """
    if not isinstance(url_or_filename, str):
        url_or_filename = str(url_or_filename)

    # If we're using the /a/b/foo.zip!c/d/file.txt syntax, handle it here.
    exclamation_index = url_or_filename.find("!")
    if extract_archive and exclamation_index >= 0:
        archive_path = url_or_filename[:exclamation_index]
        file_name = url_or_filename[exclamation_index + 1 :]

        # Call 'cached_path' now to get the local path to the archive itself.
        cached_archive_path = cached_path(
            archive_path,
            cache_dir=cache_dir,
            extract_archive=True,
            force_extract=force_extract,
            quiet=quiet,
            progress=progress,
        )
        if not cached_archive_path.is_dir():
            raise ValueError(
                f"{url_or_filename} uses the ! syntax, but does not specify an archive file."
            )

        # Now load bytes from the desired file within the extracted archive, provided it exists.
        file_path = cached_archive_path / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"'{file_name}' not found within '{archive_path}'")

        return _bytes_range_from_file(file_path, index, length)

    if urlparse(url_or_filename).scheme in get_supported_schemes():
        # URL, so use the scheme client.
        client = get_scheme_client(url_or_filename)

        # Check if file is already downloaded.
        try:
            cache_path, _ = get_from_cache(
                url_or_filename,
                cache_dir=cache_dir,
                quiet=quiet,
                progress=progress,
                no_downloads=True,
                _client=client,
            )
            return _bytes_range_from_file(cache_path, index, length)
        except FileNotFoundError:
            pass

        # Otherwise try streaming bytes directly.
        try:
            return client.get_bytes_range(index, length)
        except NotImplementedError:
            # fall back to downloading the whole file.
            pass

    file_path = cached_path(url_or_filename, cache_dir=cache_dir, quiet=quiet, progress=progress)
    return _bytes_range_from_file(file_path, index, length)


def _bytes_range_from_file(path: PathOrStr, index: int, length: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(index)
        return f.read(length)
