import logging
import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple
from urllib.parse import urlparse
from zipfile import ZipFile, is_zipfile

from .cache_file import CacheFile
from .common import PathOrStr, get_cache_dir
from .file_lock import FileLock
from .meta import Meta
from .schemes import get_scheme_client, get_supported_schemes, hf_get_from_cache
from .util import (
    _lock_file_path,
    _meta_file_path,
    check_tarfile,
    find_latest_cached,
    resource_to_filename,
)

if TYPE_CHECKING:
    from rich.progress import Progress

logger = logging.getLogger("cached_path")


def cached_path(
    url_or_filename: PathOrStr,
    cache_dir: Optional[PathOrStr] = None,
    extract_archive: bool = False,
    force_extract: bool = False,
    quiet: bool = False,
    progress: Optional["Progress"] = None,
) -> Path:
    """
    Given something that might be a URL or local path, determine which.
    If it's a remote resource, download the file and cache it, and
    then return the path to the cached file. If it's already a local path,
    make sure the file exists and return the path.

    For URLs, the following schemes are all supported out-of-the-box:

    * ``http`` and ``https``,
    * ``s3`` for objects on `AWS S3`_,
    * ``gs`` for objects on `Google Cloud Storage (GCS)`_, and
    * ``hf`` for objects or repositories on `HuggingFace Hub`_.

    You can also extend ``cached_path()`` to handle more schemes with :func:`add_scheme_client()`.

    .. _AWS S3: https://aws.amazon.com/s3/
    .. _Google Cloud Storage (GCS): https://cloud.google.com/storage
    .. _HuggingFace Hub: https://huggingface.co/

    Examples
    --------

    To download a file over ``https``::

        cached_path("https://github.com/allenai/cached_path/blob/main/README.md")

    To download an object on GCS::

        cached_path("gs://allennlp-public-models/lerc-2020-11-18.tar.gz")

    To download the PyTorch weights for the model `epwalsh/bert-xsmall-dummy`_
    on HuggingFace, you could do::

        cached_path("hf://epwalsh/bert-xsmall-dummy/pytorch_model.bin")

    For paths or URLs that point to a tarfile or zipfile, you can append the path
    to a specific file within the archive to the ``url_or_filename``, preceeded by a "!".
    The archive will be automatically extracted (provided you set ``extract_archive`` to ``True``),
    returning the local path to the specific file. For example::

        cached_path("model.tar.gz!weights.th", extract_archive=True)

    .. _epwalsh/bert-xsmall-dummy: https://huggingface.co/epwalsh/bert-xsmall-dummy

    Parameters
    ----------

    url_or_filename :
        A URL or path to parse and possibly download.

    cache_dir :
        The directory to cache downloads. If not specified, the global default cache directory
        will be used (``~/.cache/cached_path``). This can be set to something else with
        :func:`set_cache_dir()`.

    extract_archive :
        If ``True``, then zip or tar.gz archives will be automatically extracted.
        In which case the directory is returned.

    force_extract :
        If ``True`` and the file is an archive file, it will be extracted regardless
        of whether or not the extracted directory already exists.

        .. caution::
            Use this flag with caution! This can lead to race conditions if used
            from multiple processes on the same file.

    quiet :
        If ``True``, progress displays won't be printed.

    progress :
        A custom progress display to use. If not set and ``quiet=False``, a default display
        from :func:`~cached_path.get_download_progress()` will be used.

    Returns
    -------
    :class:`pathlib.Path`
        The local path to the (potentially cached) resource.

    Raises
    ------
    ``FileNotFoundError``

        If the resource cannot be found locally or remotely.

    ``ValueError``
        When the URL is invalid.

    ``Other errors``
        Other error types are possible as well depending on the client used to fetch
        the resource.

    """
    cache_dir = Path(cache_dir if cache_dir else get_cache_dir()).expanduser()
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not isinstance(url_or_filename, str):
        url_or_filename = str(url_or_filename)

    file_path: Path
    extraction_path: Optional[Path] = None
    etag: Optional[str] = None

    # If we're using the /a/b/foo.zip!c/d/file.txt syntax, handle it here.
    exclamation_index = url_or_filename.find("!")
    if extract_archive and exclamation_index >= 0:
        archive_path = url_or_filename[:exclamation_index]
        file_name = url_or_filename[exclamation_index + 1 :]

        # Call 'cached_path' recursively now to get the local path to the archive itself.
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

        # Now return the full path to the desired file within the extracted archive,
        # provided it exists.
        file_path = cached_archive_path / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"'{file_name}' not found within '{archive_path}'")

        return file_path

    parsed = urlparse(url_or_filename)

    if parsed.scheme in get_supported_schemes():
        # URL, so get it from the cache (downloading if necessary)
        file_path, etag = get_from_cache(url_or_filename, cache_dir, quiet=quiet, progress=progress)

        if extract_archive and (is_zipfile(file_path) or tarfile.is_tarfile(file_path)):
            # This is the path the file should be extracted to.
            # For example ~/.cached_path/cache/234234.21341 -> ~/.cached_path/cache/234234.21341-extracted
            extraction_path = file_path.parent / (file_path.name + "-extracted")

    else:
        url_or_filename = Path(url_or_filename).expanduser()

        if url_or_filename.exists():
            # File, and it exists.
            file_path = url_or_filename
            # Normalize the path.
            url_or_filename = url_or_filename.resolve()

            if (
                extract_archive
                and file_path.is_file()
                and (is_zipfile(file_path) or tarfile.is_tarfile(file_path))
            ):
                # We'll use a unique directory within the cache to root to extract the archive to.
                # The name of the directory is a hash of the resource file path and it's modification
                # time. That way, if the file changes, we'll know when to extract it again.
                extraction_name = (
                    resource_to_filename(url_or_filename, str(os.path.getmtime(file_path)))
                    + "-extracted"
                )
                extraction_path = cache_dir / extraction_name

        elif parsed.scheme == "":
            # File, but it doesn't exist.
            raise FileNotFoundError(f"file {url_or_filename} not found")

        else:
            # Something unknown
            raise ValueError(f"unable to parse {url_or_filename} as a URL or as a local path")

    if extraction_path is not None:
        # If the extracted directory already exists (and is non-empty), then no
        # need to create a lock file and extract again unless `force_extract=True`.
        if os.path.isdir(extraction_path) and os.listdir(extraction_path) and not force_extract:
            return extraction_path

        # Extract it.
        with FileLock(_lock_file_path(extraction_path)):
            # Check again if the directory exists now that we've acquired the lock.
            if os.path.isdir(extraction_path) and os.listdir(extraction_path):
                if force_extract:
                    logger.warning(
                        "Extraction directory for %s (%s) already exists, "
                        "overwriting it since 'force_extract' is 'True'",
                        url_or_filename,
                        extraction_path,
                    )
                else:
                    return extraction_path

            logger.info("Extracting %s to %s", url_or_filename, extraction_path)
            shutil.rmtree(extraction_path, ignore_errors=True)

            # We extract first to a temporary directory in case something goes wrong
            # during the extraction process so we don't end up with a corrupted cache.
            tmp_extraction_dir = tempfile.mkdtemp(dir=os.path.split(extraction_path)[0])
            try:
                if is_zipfile(file_path):
                    with ZipFile(file_path, "r") as zip_file:
                        zip_file.extractall(tmp_extraction_dir)
                        zip_file.close()
                else:
                    tar_file = tarfile.open(file_path)
                    check_tarfile(tar_file)
                    tar_file.extractall(tmp_extraction_dir)
                    tar_file.close()
                # Extraction was successful, rename temp directory to final
                # cache directory and dump the meta data.
                os.replace(tmp_extraction_dir, extraction_path)
                meta = Meta.new(
                    url_or_filename,
                    extraction_path,
                    etag=etag,
                    extraction_dir=True,
                )
                meta.to_file()
            finally:
                shutil.rmtree(tmp_extraction_dir, ignore_errors=True)

        return extraction_path

    return file_path


def get_from_cache(
    url: str,
    cache_dir: Optional[PathOrStr] = None,
    quiet: bool = False,
    progress: Optional["Progress"] = None,
) -> Tuple[Path, Optional[str]]:
    """
    Given a URL, look for the corresponding dataset in the local cache.
    If it's not there, download it. Then return the path to the cached file and the ETag.
    """
    cache_dir = Path(cache_dir if cache_dir else get_cache_dir())

    if url.startswith("hf://"):
        return hf_get_from_cache(url, cache_dir), None

    client = get_scheme_client(url)

    # Get eTag to add to filename, if it exists.
    try:
        etag = client.get_etag()
    except client.recoverable_errors:  # type: ignore
        # We might be offline, in which case we don't want to throw an error
        # just yet. Instead, we'll try to use the latest cached version of the
        # target resource, if it exists. We'll only throw an exception if we
        # haven't cached the resource at all yet.
        logger.warning(
            "Connection error occurred while trying to fetch ETag for %s. "
            "Will attempt to use latest cached version of resource",
            url,
        )
        latest_cached = find_latest_cached(url, cache_dir)
        if latest_cached:
            logger.info(
                "ETag request failed with recoverable error, using latest cached "
                "version of %s: %s",
                url,
                latest_cached,
            )
            meta = Meta.from_path(_meta_file_path(latest_cached))
            return latest_cached, meta.etag
        else:
            logger.error(
                "ETag request failed with recoverable error, "
                "but no cached version of %s could be found",
                url,
            )
            raise

    filename = resource_to_filename(url, etag)

    # Get cache path to put the file.
    cache_path = cache_dir / filename

    # Multiple processes may be trying to cache the same file at once, so we need
    # to be a little careful to avoid race conditions. We do this using a lock file.
    # Only one process can own this lock file at a time, and a process will block
    # on the call to `lock.acquire()` until the process currently holding the lock
    # releases it.
    logger.debug("waiting to acquire lock on %s", cache_path)
    with FileLock(_lock_file_path(cache_path), read_only_ok=True):
        if os.path.exists(cache_path):
            logger.info("cache of %s is up-to-date", url)
        else:
            size = client.get_size()
            with CacheFile(cache_path) as cache_file:
                logger.info("%s not found in cache, downloading to %s", url, cache_path)

                from .progress import BufferedWriterWithProgress, get_download_progress

                start_and_cleanup = progress is None
                progress = progress or get_download_progress(quiet=quiet)

                if start_and_cleanup:
                    progress.start()

                try:
                    display_url = url if len(url) <= 50 else f"{url[:49]}\N{horizontal ellipsis}"
                    task_id = progress.add_task(f"Downloading [cyan i]{display_url}[/]", total=size)
                    writer_with_progress = BufferedWriterWithProgress(cache_file, progress, task_id)
                    client.get_resource(writer_with_progress)
                    progress.update(
                        task_id,
                        total=writer_with_progress.total_written,
                        completed=writer_with_progress.total_written,
                    )
                finally:
                    if start_and_cleanup:
                        progress.stop()

            logger.debug("creating metadata file for %s", cache_path)
            meta = Meta.new(
                url,
                cache_path,
                etag=etag,
            )
            meta.to_file()

    return cache_path, etag
