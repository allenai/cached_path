import os
import logging
import tempfile
from urllib.parse import urlparse
from typing import Optional, Tuple
from zipfile import ZipFile, is_zipfile
import tarfile
import shutil

from cached_path.cache_file import CacheFile
from cached_path.common import PathOrStr, get_cache_dir
from cached_path.file_lock import FileLock
from cached_path.meta import Meta
from cached_path.schemes import get_scheme_client, get_supported_schemes, hf_get_from_cache
from cached_path.util import (
    resource_to_filename,
    find_latest_cached,
    check_tarfile,
)


logger = logging.getLogger("cached_path")


def cached_path(
    url_or_filename: PathOrStr,
    cache_dir: Optional[PathOrStr] = None,
    extract_archive: bool = False,
    force_extract: bool = False,
) -> str:
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

    Returns
    -------
    ``str``
        The local path to the (potentially cached) resource.

        .. important::
            The return type is always a ``str`` even if the original argument was a ``Path``.

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
    cache_dir = cache_dir if cache_dir else get_cache_dir()
    cache_dir = os.path.expanduser(cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    if not isinstance(url_or_filename, str):
        url_or_filename = str(url_or_filename)

    file_path: str
    extraction_path: Optional[str] = None
    etag: Optional[str] = None

    # If we're using the /a/b/foo.zip!c/d/file.txt syntax, handle it here.
    exclamation_index = url_or_filename.find("!")
    if extract_archive and exclamation_index >= 0:
        archive_path = url_or_filename[:exclamation_index]
        file_name = url_or_filename[exclamation_index + 1 :]

        # Call 'cached_path' recursively now to get the local path to the archive itself.
        cached_archive_path = cached_path(archive_path, cache_dir, True, force_extract)
        if not os.path.isdir(cached_archive_path):
            raise ValueError(
                f"{url_or_filename} uses the ! syntax, but does not specify an archive file."
            )

        # Now return the full path to the desired file within the extracted archive,
        # provided it exists.
        file_path = os.path.join(cached_archive_path, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"file {file_name} not found within {archive_path}")

        return file_path

    parsed = urlparse(url_or_filename)

    if parsed.scheme in get_supported_schemes():
        # URL, so get it from the cache (downloading if necessary)
        file_path, etag = get_from_cache(url_or_filename, cache_dir)

        if extract_archive and (is_zipfile(file_path) or tarfile.is_tarfile(file_path)):
            # This is the path the file should be extracted to.
            # For example ~/.cached_path/cache/234234.21341 -> ~/.cached_path/cache/234234.21341-extracted
            extraction_path = file_path + "-extracted"

    else:
        url_or_filename = os.path.expanduser(url_or_filename)

        if os.path.exists(url_or_filename):
            # File, and it exists.
            file_path = url_or_filename
            # Normalize the path.
            url_or_filename = os.path.abspath(url_or_filename)

            if (
                extract_archive
                and os.path.isfile(file_path)
                and (is_zipfile(file_path) or tarfile.is_tarfile(file_path))
            ):
                # We'll use a unique directory within the cache to root to extract the archive to.
                # The name of the directory is a hash of the resource file path and it's modification
                # time. That way, if the file changes, we'll know when to extract it again.
                extraction_name = (
                    resource_to_filename(url_or_filename, str(os.path.getmtime(file_path)))
                    + "-extracted"
                )
                extraction_path = os.path.join(cache_dir, extraction_name)

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
        with FileLock(extraction_path + ".lock"):
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


def get_from_cache(url: str, cache_dir: Optional[PathOrStr] = None) -> Tuple[str, Optional[str]]:
    """
    Given a URL, look for the corresponding dataset in the local cache.
    If it's not there, download it. Then return the path to the cached file and the ETag.
    """
    cache_dir = cache_dir if cache_dir else get_cache_dir()

    if url.startswith("hf://"):
        return hf_get_from_cache(url, cache_dir), None

    client = get_scheme_client(url)

    # Get eTag to add to filename, if it exists.
    try:
        etag = client.get_etag()
    except FileNotFoundError:
        raise
    except client.connection_error_types:  # type: ignore
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
                "ETag request failed with connection error, using latest cached "
                "version of %s: %s",
                url,
                latest_cached,
            )
            meta = Meta.from_path(latest_cached + ".json")
            return latest_cached, meta.etag
        else:
            logger.error(
                "Connection failed while trying to fetch ETag, "
                "and no cached version of %s could be found",
                url,
            )
            raise
    except Exception as exc:
        # Other exceptions may be triggered if we were unable to fetch the eTag.
        # If this is the case, try to proceed without eTag check.
        logger.error("Encountered error while trying to fetch ETag for %s: %s", url, exc)
        etag = None

    filename = resource_to_filename(url, etag)

    # Get cache path to put the file.
    cache_path = os.path.join(cache_dir, filename)

    # Multiple processes may be trying to cache the same file at once, so we need
    # to be a little careful to avoid race conditions. We do this using a lock file.
    # Only one process can own this lock file at a time, and a process will block
    # on the call to `lock.acquire()` until the process currently holding the lock
    # releases it.
    logger.debug("waiting to acquire lock on %s", cache_path)
    with FileLock(cache_path + ".lock", read_only_ok=True):
        if os.path.exists(cache_path):
            logger.info("cache of %s is up-to-date", url)
        else:
            with CacheFile(cache_path) as cache_file:
                logger.info("%s not found in cache, downloading to %s", url, cache_path)
                client.get_resource(cache_file)

            logger.debug("creating metadata file for %s", cache_path)
            meta = Meta.new(
                url,
                cache_path,
                etag=etag,
            )
            meta.to_file()

    return cache_path, etag
