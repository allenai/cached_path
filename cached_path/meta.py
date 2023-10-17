import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Optional, Set

from .common import PathOrStr


@dataclass
class Meta:
    """
    Any resource that is downloaded to - or extracted in - the cache directory will
    have a meta JSON file written next to it, which corresponds to an instance
    of this class.

    In older versions of AllenNLP, this meta document just had two fields: 'url' and
    'etag'. The 'url' field is now the more general 'resource' field, but these old
    meta files are still compatible when a `Meta` is instantiated with the `.from_path()`
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

    @classmethod
    def new(
        cls,
        resource: PathOrStr,
        cached_path: PathOrStr,
        *,
        etag: Optional[str] = None,
        extraction_dir: bool = False,
    ) -> "Meta":
        return cls(  # type: ignore
            resource=str(resource),
            cached_path=str(cached_path),
            creation_time=time.time(),
            size=cls.get_resource_size(cached_path),
            etag=etag,
            extraction_dir=extraction_dir,
        )

    def to_file(self) -> None:
        with open(self.cached_path + ".json", "w") as meta_file:
            json.dump(asdict(self), meta_file)

    @classmethod
    def from_path(cls, path: PathOrStr) -> "Meta":
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
                data["size"] = cls.get_resource_size(data["cached_path"])
        return cls(**data)  # type: ignore

    @staticmethod
    def get_resource_size(path: PathOrStr) -> int:
        """
        Get the size of a file or directory.
        """
        if os.path.isfile(path):
            return os.path.getsize(path)
        inodes: Set[int] = set()
        total_size = 0
        for dirpath, _, filenames in os.walk(str(path)):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link or the same as a file we've already accounted
                # for (this could happen with hard links).
                inode = os.stat(fp).st_ino
                if not os.path.islink(fp) and inode not in inodes:
                    inodes.add(inode)
                    total_size += os.path.getsize(fp)
        return total_size
