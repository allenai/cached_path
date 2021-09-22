import glob
from hashlib import sha256
import os
import tarfile
from typing import Optional, Tuple, List
from urllib.parse import urlparse

from cached_path.schemes import get_supported_schemes
from cached_path.common import PathOrStr, get_cache_dir
from cached_path.meta import Meta


def resource_to_filename(resource: str, etag: Optional[str] = None) -> str:
    """
    Convert a ``resource`` into a hashed filename in a repeatable way.
    If ``etag`` is specified, append its hash to the resources', delimited
    by a period.

    THis is essentially the inverse of :func:`filename_to_url()`.
    """
    resource_bytes = resource.encode("utf-8")
    resource_hash = sha256(resource_bytes)
    filename = resource_hash.hexdigest()

    if etag:
        etag_bytes = etag.encode("utf-8")
        etag_hash = sha256(etag_bytes)
        filename += "." + etag_hash.hexdigest()

    return filename


def filename_to_url(
    filename: str, cache_dir: Optional[PathOrStr] = None
) -> Tuple[str, Optional[str]]:
    """
    Return the URL and etag (which may be ``None``) stored for ``filename``.
    Raises :exc:`FileNotFoundError` if ``filename`` or its stored metadata do not exist.

    This is essentially the inverse of :func:`resource_to_filename()`.
    """
    cache_dir = cache_dir if cache_dir else get_cache_dir()
    cache_path = os.path.join(cache_dir, filename)
    if not os.path.exists(cache_path):
        raise FileNotFoundError("file {} not found".format(cache_path))

    meta_path = cache_path + ".json"
    if not os.path.exists(meta_path):
        raise FileNotFoundError("file {} not found".format(meta_path))

    metadata = Meta.from_path(meta_path)
    return metadata.resource, metadata.etag


def find_latest_cached(url: str, cache_dir: Optional[PathOrStr] = None) -> Optional[str]:
    """
    Get the path to the latest cached version of a given resource.
    """
    cache_dir = cache_dir if cache_dir else get_cache_dir()
    filename = resource_to_filename(url)
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


def is_url_or_existing_file(url_or_filename: PathOrStr) -> bool:
    """
    Given something that might be a URL or local path,
    determine if it's actually a url or the path to an existing file.
    """
    if url_or_filename is None:
        return False
    url_or_filename = os.path.expanduser(str(url_or_filename))
    parsed = urlparse(url_or_filename)
    return parsed.scheme in get_supported_schemes() or os.path.exists(url_or_filename)
