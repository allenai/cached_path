"""
The idea behind cached-path is to provide a unified, simple interface for accessing both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.
"""

import glob
import os
import logging
import tempfile
import json
from dataclasses import dataclass, asdict
from os import PathLike
from urllib.parse import urlparse
from pathlib import Path
from typing import (
    Optional,
    Tuple,
    Union,
    IO,
    Callable,
    Set,
    List,
)
from hashlib import sha256
from functools import wraps
from zipfile import ZipFile, is_zipfile
import tarfile
import shutil
import time
import warnings

import boto3
import botocore
from filelock import FileLock as _FileLock
from google.cloud import storage
from google.api_core.exceptions import NotFound
from overrides import overrides
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import huggingface_hub as hf_hub

from cached_path.version import VERSION
from cached_path.tqdm import Tqdm

logger = logging.getLogger(__name__)

CACHE_DIRECTORY = Path(os.getenv("CACHED_PATH_CACHE_ROOT", Path.home() / ".cache" / "cached_path"))

PathOrStr = Union[str, PathLike]


class FileLock(_FileLock):
    """
    This is just a subclass of the `FileLock` class from the `filelock` library, except that
    it adds an additional argument to the `__init__` method: `read_only_ok`.

    By default this flag is `False`, which an exception will be thrown when a lock
    can't be acquired due to lack of write permissions.
    But if this flag is set to `True`, a warning will be emitted instead of an error when
    the lock already exists but the lock can't be acquired because write access is blocked.
    """

    def __init__(self, lock_file: PathOrStr, timeout=-1, read_only_ok: bool = False) -> None:
        super().__init__(str(lock_file), timeout=timeout)
        self._read_only_ok = read_only_ok

    @overrides
    def acquire(self, timeout=None, poll_interval=0.05):
        try:
            super().acquire(timeout=timeout, poll_intervall=poll_interval)
        except OSError as err:
            # OSError could be a lot of different things, but what we're looking
            # for in particular are permission errors, such as:
            #  - errno 1  - EPERM  - "Operation not permitted"
            #  - errno 13 - EACCES - "Permission denied"
            #  - errno 30 - EROFS  - "Read-only file system"
            if err.errno not in (1, 13, 30):
                raise

            if os.path.isfile(self._lock_file) and self._read_only_ok:
                warnings.warn(
                    f"Lacking permissions required to obtain lock '{self._lock_file}'. "
                    "Race conditions are possible if other processes are writing to the same resource.",
                    UserWarning,
                )
            else:
                raise


def _resource_to_filename(resource: str, etag: str = None) -> str:
    """
    Convert a `resource` into a hashed filename in a repeatable way.
    If `etag` is specified, append its hash to the resources's, delimited
    by a period.
    """
    resource_bytes = resource.encode("utf-8")
    resource_hash = sha256(resource_bytes)
    filename = resource_hash.hexdigest()

    if etag:
        etag_bytes = etag.encode("utf-8")
        etag_hash = sha256(etag_bytes)
        filename += "." + etag_hash.hexdigest()

    return filename


def filename_to_url(filename: str, cache_dir: PathOrStr = CACHE_DIRECTORY) -> Tuple[str, str]:
    """
    Return the url and etag (which may be `None`) stored for `filename`.
    Raise `FileNotFoundError` if `filename` or its stored metadata do not exist.
    """
    cache_path = os.path.join(cache_dir, filename)
    if not os.path.exists(cache_path):
        raise FileNotFoundError("file {} not found".format(cache_path))

    meta_path = cache_path + ".json"
    if not os.path.exists(meta_path):
        raise FileNotFoundError("file {} not found".format(meta_path))

    with open(meta_path) as meta_file:
        metadata = json.load(meta_file)
    url = metadata["url"]
    etag = metadata["etag"]

    return url, etag


def check_tarfile(tar_file: tarfile.TarFile):
    """Tar files can contain files outside of the extraction directory, or symlinks that point
    outside the extraction directory. We also don't want any block devices fifos, or other
    weird file types extracted. This checks for those issues and throws an exception if there
    is a problem."""
    base_path = os.path.join("tmp", "pathtest")
    base_path = os.path.normpath(base_path)

    def normalize_path(path: str) -> str:
        path = path.rstrip("/")
        path = path.replace("/", os.sep)
        path = os.path.join(base_path, path)
        path = os.path.normpath(path)
        return path

    for tarinfo in tar_file:
        if not (
            tarinfo.isreg()
            or tarinfo.isdir()
            or tarinfo.isfile()
            or tarinfo.islnk()
            or tarinfo.issym()
        ):
            raise ValueError(
                f"Tar file {str(tar_file.name)} contains invalid member {tarinfo.name}."
            )

        target_path = normalize_path(tarinfo.name)
        if os.path.commonprefix([base_path, target_path]) != base_path:
            raise ValueError(
                f"Tar file {str(tar_file.name)} is trying to create a file outside of its extraction directory."
            )

        if tarinfo.islnk() or tarinfo.issym():
            target_path = normalize_path(tarinfo.linkname)
            if os.path.commonprefix([base_path, target_path]) != base_path:
                raise ValueError(
                    f"Tar file {str(tar_file.name)} is trying to link to a file "
                    "outside of its extraction directory."
                )


def cached_path(
    url_or_filename: PathOrStr,
    cache_dir: PathOrStr = CACHE_DIRECTORY,
    extract_archive: bool = False,
    force_extract: bool = False,
) -> str:
    """
    Given something that might be a URL or local path, determine which.
    If it's a remote resource, download the file and cache it, and
    then return the path to the cached file. If it's already a local path,
    make sure the file exists and return the path.

    For URLs, "http://", "https://", "s3://", "gs://", and "hf://" are all supported.
    The latter corresponds to the HuggingFace Hub.

    For example, to download the PyTorch weights for the model `epwalsh/bert-xsmall-dummy`
    on HuggingFace, you could do:

    ```python
    cached_path("hf://epwalsh/bert-xsmall-dummy/pytorch_model.bin")
    ```

    For paths or URLs that point to a tarfile or zipfile, you can also add a path
    to a specific file to the `url_or_filename` preceeded by a "!", and the archive will
    be automatically extracted (provided you set `extract_archive` to `True`),
    returning the local path to the specific file. For example:

    ```python
    cached_path("model.tar.gz!weights.th", extract_archive=True)
    ```

    # Parameters

    url_or_filename : `PathOrStr`
        A URL or path to parse and possibly download.

    cache_dir : `PathOrStr`, optional (default = `CACHE_DIRECTORY`)
        The directory to cache downloads.

    extract_archive : `bool`, optional (default = `False`)
        If `True`, then zip or tar.gz archives will be automatically extracted.
        In which case the directory is returned.

    force_extract : `bool`, optional (default = `False`)
        If `True` and the file is an archive file, it will be extracted regardless
        of whether or not the extracted directory already exists.

        !!! Warning
            Use this flag with caution! This can lead to race conditions if used
            from multiple processes on the same file.
    """
    cache_dir = os.path.expanduser(cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    if not isinstance(url_or_filename, str):
        url_or_filename = str(url_or_filename)

    file_path: str
    extraction_path: Optional[str] = None

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

    if parsed.scheme in ("http", "https", "s3", "hf", "gs"):
        # URL, so get it from the cache (downloading if necessary)
        file_path = get_from_cache(url_or_filename, cache_dir)

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
                    _resource_to_filename(url_or_filename, str(os.path.getmtime(file_path)))
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
                meta = _Meta(
                    resource=url_or_filename,
                    cached_path=extraction_path,
                    creation_time=time.time(),
                    extraction_dir=True,
                    size=_get_resource_size(extraction_path),
                )
                meta.to_file()
            finally:
                shutil.rmtree(tmp_extraction_dir, ignore_errors=True)

        return extraction_path

    return file_path


def is_url_or_existing_file(url_or_filename: PathOrStr) -> bool:
    """
    Given something that might be a URL (or might be a local path),
    determine check if it's url or an existing file path.
    """
    if url_or_filename is None:
        return False
    url_or_filename = os.path.expanduser(str(url_or_filename))
    parsed = urlparse(url_or_filename)
    return parsed.scheme in ("http", "https", "s3", "gs") or os.path.exists(url_or_filename)


def _split_s3_path(url: str) -> Tuple[str, str]:
    return _split_cloud_path(url, "s3")


def _split_gcs_path(url: str) -> Tuple[str, str]:
    return _split_cloud_path(url, "gs")


def _split_cloud_path(url: str, provider: str) -> Tuple[str, str]:
    """Split a full s3 path into the bucket name and path."""
    parsed = urlparse(url)
    if not parsed.netloc or not parsed.path:
        raise ValueError("bad {} path {}".format(provider, url))
    bucket_name = parsed.netloc
    provider_path = parsed.path
    # Remove '/' at beginning of path.
    if provider_path.startswith("/"):
        provider_path = provider_path[1:]
    return bucket_name, provider_path


def _s3_request(func: Callable):
    """
    Wrapper function for s3 requests in order to create more helpful error
    messages.
    """

    @wraps(func)
    def wrapper(url: str, *args, **kwargs):
        try:
            return func(url, *args, **kwargs)
        except botocore.exceptions.ClientError as exc:
            if int(exc.response["Error"]["Code"]) == 404:
                raise FileNotFoundError("file {} not found".format(url))
            else:
                raise

    return wrapper


def _get_s3_resource():
    session = boto3.session.Session()
    if session.get_credentials() is None:
        # Use unsigned requests.
        s3_resource = session.resource(
            "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
        )
    else:
        s3_resource = session.resource("s3")
    return s3_resource


@_s3_request
def _s3_etag(url: str) -> Optional[str]:
    """Check ETag on S3 object."""
    s3_resource = _get_s3_resource()
    bucket_name, s3_path = _split_s3_path(url)
    s3_object = s3_resource.Object(bucket_name, s3_path)
    return s3_object.e_tag


@_s3_request
def _s3_get(url: str, temp_file: IO) -> None:
    """Pull a file directly from S3."""
    s3_resource = _get_s3_resource()
    bucket_name, s3_path = _split_s3_path(url)
    s3_resource.Bucket(bucket_name).download_fileobj(s3_path, temp_file)


def _gcs_request(func: Callable):
    """
    Wrapper function for gcs requests in order to create more helpful error
    messages.
    """

    @wraps(func)
    def wrapper(url: str, *args, **kwargs):
        try:
            return func(url, *args, **kwargs)
        except NotFound:
            raise FileNotFoundError("file {} not found".format(url))

    return wrapper


def _get_gcs_client():
    storage_client = storage.Client()
    return storage_client


def _get_gcs_blob(url: str) -> storage.blob.Blob:
    gcs_resource = _get_gcs_client()
    bucket_name, gcs_path = _split_gcs_path(url)
    bucket = gcs_resource.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob


@_gcs_request
def _gcs_md5(url: str) -> Optional[str]:
    """Get GCS object's md5."""
    blob = _get_gcs_blob(url)
    return blob.md5_hash


@_gcs_request
def _gcs_get(url: str, temp_filename: str) -> None:
    """Pull a file directly from GCS."""
    blob = _get_gcs_blob(url)
    blob.download_to_filename(temp_filename)


def _session_with_backoff() -> requests.Session:
    """
    We ran into an issue where http requests to s3 were timing out,
    possibly because we were making too many requests too quickly.
    This helper function returns a requests session that has retry-with-backoff
    built in. See
    <https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library>.
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return session


def _http_etag(url: str) -> Optional[str]:
    with _session_with_backoff() as session:
        response = session.head(url, allow_redirects=True)
    if response.status_code != 200:
        raise OSError(
            "HEAD request failed for url {} with status code {}".format(url, response.status_code)
        )
    return response.headers.get("ETag")


def _http_get(url: str, temp_file: IO) -> None:
    with _session_with_backoff() as session:
        req = session.get(url, stream=True)
        req.raise_for_status()
        content_length = req.headers.get("Content-Length")
        total = int(content_length) if content_length is not None else None
        progress = Tqdm.tqdm(unit="B", unit_scale=True, total=total, desc="downloading")
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                progress.update(len(chunk))
                temp_file.write(chunk)
        progress.close()


def _find_latest_cached(url: str, cache_dir: PathOrStr) -> Optional[str]:
    filename = _resource_to_filename(url)
    cache_path = os.path.join(cache_dir, filename)
    candidates: List[Tuple[str, float]] = []
    for path in glob.glob(cache_path + "*"):
        if path.endswith(".json") or path.endswith("-extracted") or path.endswith(".lock"):
            continue
        mtime = os.path.getmtime(path)
        candidates.append((path, mtime))
    # Sort candidates by modification time, newest first.
    candidates.sort(key=lambda x: x[1], reverse=True)
    if candidates:
        return candidates[0][0]
    return None


class CacheFile:
    """
    This is a context manager that makes robust caching easier.

    On `__enter__`, an IO handle to a temporarily file is returned, which can
    be treated as if it's the actual cache file.

    On `__exit__`, the temporarily file is renamed to the cache file. If anything
    goes wrong while writing to the temporary file, it will be removed.
    """

    def __init__(self, cache_filename: PathOrStr, mode: str = "w+b", suffix: str = ".tmp") -> None:
        self.cache_filename = (
            cache_filename if isinstance(cache_filename, Path) else Path(cache_filename)
        )
        self.cache_directory = os.path.dirname(self.cache_filename)
        self.mode = mode
        self.temp_file = tempfile.NamedTemporaryFile(
            self.mode, dir=self.cache_directory, delete=False, suffix=suffix
        )

    def __enter__(self):
        return self.temp_file

    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_file.close()
        if exc_value is None:
            # Success.
            logger.debug(
                "Renaming temp file %s to cache at %s", self.temp_file.name, self.cache_filename
            )
            # Rename the temp file to the actual cache filename.
            os.replace(self.temp_file.name, self.cache_filename)
            return True
        # Something went wrong, remove the temp file.
        logger.debug("removing temp file %s", self.temp_file.name)
        os.remove(self.temp_file.name)
        return False


@dataclass
class _Meta:
    """
    Any resource that is downloaded to - or extracted in - the cache directory will
    have a meta JSON file written next to it, which corresponds to an instance
    of this class.

    In older versions of AllenNLP, this meta document just had two fields: 'url' and
    'etag'. The 'url' field is now the more general 'resource' field, but these old
    meta files are still compatible when a `_Meta` is instantiated with the `.from_path()`
    class method.
    """

    resource: str
    """
    URL or normalized path to the resource.
    """

    cached_path: str
    """
    Path to the corresponding cached version of the resource.
    """

    creation_time: float
    """
    The unix timestamp of when the corresponding resource was cached or extracted.
    """

    size: int = 0
    """
    The size of the corresponding resource, in bytes.
    """

    etag: Optional[str] = None
    """
    Optional ETag associated with the current cached version of the resource.
    """

    extraction_dir: bool = False
    """
    Does this meta corresponded to an extraction directory?
    """

    def to_file(self) -> None:
        with open(self.cached_path + ".json", "w") as meta_file:
            json.dump(asdict(self), meta_file)

    @classmethod
    def from_path(cls, path: PathOrStr) -> "_Meta":
        path = str(path)
        with open(path) as meta_file:
            data = json.load(meta_file)
            # For backwards compat:
            if "resource" not in data:
                data["resource"] = data.pop("url")
            if "creation_time" not in data:
                data["creation_time"] = os.path.getmtime(path[:-5])
            if "extraction_dir" not in data and path.endswith("-extracted.json"):
                data["extraction_dir"] = True
            if "cached_path" not in data:
                data["cached_path"] = path[:-5]
            if "size" not in data:
                data["size"] = _get_resource_size(data["cached_path"])
        return cls(**data)


def _hf_hub_download(
    url, model_identifier: str, filename: Optional[str], cache_dir: PathOrStr = CACHE_DIRECTORY
) -> str:
    revision: Optional[str]
    if "@" in model_identifier:
        repo_id = model_identifier.split("@")[0]
        revision = model_identifier.split("@")[1]
    else:
        repo_id = model_identifier
        revision = None

    if filename is not None:
        hub_url = hf_hub.hf_hub_url(repo_id=repo_id, filename=filename, revision=revision)
        cache_path = str(
            hf_hub.cached_download(
                url=hub_url,
                library_name="cached_path",
                library_version=VERSION,
                cache_dir=cache_dir,
            )
        )
        # HF writes it's own meta '.json' file which uses the same format we used to use and still
        # support, but is missing some fields that we like to have.
        # So we overwrite it when it we can.
        with FileLock(cache_path + ".lock", read_only_ok=True):
            meta = _Meta.from_path(cache_path + ".json")
            # The file HF writes will have 'resource' set to the 'http' URL corresponding to the 'hf://' URL,
            # but we want 'resource' to be the original 'hf://' URL.
            if meta.resource != url:
                meta.resource = url
                meta.to_file()
    else:
        cache_path = str(hf_hub.snapshot_download(repo_id, revision=revision, cache_dir=cache_dir))
        # Need to write the meta file for snapshot downloads if it doesn't exist.
        with FileLock(cache_path + ".lock", read_only_ok=True):
            if not os.path.exists(cache_path + ".json"):
                meta = _Meta(
                    resource=url,
                    cached_path=cache_path,
                    creation_time=time.time(),
                    extraction_dir=True,
                    size=_get_resource_size(cache_path),
                )
                meta.to_file()
    return cache_path


# TODO(joelgrus): do we want to do checksums or anything like that?
def get_from_cache(url: str, cache_dir: PathOrStr = CACHE_DIRECTORY) -> str:
    """
    Given a URL, look for the corresponding dataset in the local cache.
    If it's not there, download it. Then return the path to the cached file.
    """
    if url.startswith("hf://"):
        # Remove the 'hf://' prefix
        identifier = url[5:]

        if identifier.count("/") > 1:
            filename = "/".join(identifier.split("/")[2:])
            model_identifier = "/".join(identifier.split("/")[:2])
            return _hf_hub_download(url, model_identifier, filename, cache_dir)
        elif identifier.count("/") == 1:
            # 'hf://' URLs like 'hf://xxxx/yyyy' are potentially ambiguous,
            # because this could refer to either:
            #  1. the file 'yyyy' in the 'xxxx' repository, or
            #  2. the repo 'yyyy' under the user/org name 'xxxx'.
            # We default to (1), but if we get a 404 error then we try (2).
            try:
                model_identifier, filename = identifier.split("/")
                return _hf_hub_download(url, model_identifier, filename, cache_dir)
            except requests.exceptions.HTTPError as exc:
                if exc.response.status_code == 404:
                    return _hf_hub_download(url, identifier, None, cache_dir)
                raise
        else:
            return _hf_hub_download(url, identifier, None, cache_dir)

    # Get eTag to add to filename, if it exists.
    try:
        if url.startswith("s3://"):
            etag = _s3_etag(url)
        elif url.startswith("gs://"):
            etag = _gcs_md5(url)
        else:
            etag = _http_etag(url)
    except (requests.exceptions.ConnectionError, botocore.exceptions.EndpointConnectionError):
        # We might be offline, in which case we don't want to throw an error
        # just yet. Instead, we'll try to use the latest cached version of the
        # target resource, if it exists. We'll only throw an exception if we
        # haven't cached the resource at all yet.
        logger.warning(
            "Connection error occurred while trying to fetch ETag for %s. "
            "Will attempt to use latest cached version of resource",
            url,
        )
        latest_cached = _find_latest_cached(url, cache_dir)
        if latest_cached:
            logger.info(
                "ETag request failed with connection error, using latest cached "
                "version of %s: %s",
                url,
                latest_cached,
            )
            return latest_cached
        else:
            logger.error(
                "Connection failed while trying to fetch ETag, "
                "and no cached version of %s could be found",
                url,
            )
            raise
    except OSError:
        # OSError may be triggered if we were unable to fetch the eTag.
        # If this is the case, try to proceed without eTag check.
        etag = None

    filename = _resource_to_filename(url, etag)

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

                # GET file object
                if url.startswith("s3://"):
                    _s3_get(url, cache_file)
                elif url.startswith("gs://"):
                    _gcs_get(url, cache_file.name)
                else:
                    _http_get(url, cache_file)

            logger.debug("creating metadata file for %s", cache_path)
            meta = _Meta(
                resource=url,
                cached_path=cache_path,
                creation_time=time.time(),
                etag=etag,
                size=_get_resource_size(cache_path),
            )
            meta.to_file()

    return cache_path


def _get_resource_size(path: str) -> int:
    """
    Get the size of a file or directory.
    """
    if os.path.isfile(path):
        return os.path.getsize(path)
    inodes: Set[int] = set()
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link or the same as a file we've already accounted
            # for (this could happen with hard links).
            inode = os.stat(fp).st_ino
            if not os.path.islink(fp) and inode not in inodes:
                inodes.add(inode)
                total_size += os.path.getsize(fp)
    return total_size
