import io
from typing import List, Optional

from rich.progress import BarColumn, DownloadColumn, Progress, TaskID, TimeElapsedColumn


class BufferedWriterWithProgress(io.BufferedWriter):
    def __init__(self, handle: io.BufferedWriter, progress: Progress, task_id: TaskID):
        self.handle = handle
        self.progress = progress
        self.task_id = task_id
        self.total_written = 0

    def __enter__(self) -> "BufferedWriterWithProgress":
        self.handle.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def closed(self) -> bool:
        return self.handle.closed

    def close(self):
        self.handle.close()

    def fileno(self):
        return self.handle.fileno()

    def flush(self):
        self.handle.flush()

    def isatty(self) -> bool:
        return self.handle.isatty()

    def readable(self) -> bool:
        return self.handle.readable()

    def seekable(self) -> bool:
        return self.handle.seekable()

    def writable(self) -> bool:
        return True

    def read(self, size: Optional[int] = -1) -> bytes:
        return self.handle.read(size)

    def read1(self, size: Optional[int] = -1) -> bytes:
        return self.handle.read1()

    def readinto(self, b):
        return self.handle.readinto(b)

    def readinto1(self, b):
        return self.handle.readinto1(b)

    def readline(self, size: Optional[int] = -1) -> bytes:
        return self.handle.readline(size)

    def readlines(self, hint: int = -1) -> List[bytes]:
        return self.handle.readlines(hint)

    def write(self, b) -> int:
        n = self.handle.write(b)
        self.total_written += n
        self.progress.advance(self.task_id, n)
        return n

    def writelines(self, lines):
        return self.handle.writelines(lines)

    def seek(self, offset: int, whence: int = 0) -> int:
        pos = self.handle.seek(offset, whence)
        self.progress.update(self.task_id, completed=pos)
        return pos

    def tell(self) -> int:
        return self.handle.tell()

    @property
    def raw(self):
        return self.handle.raw

    def detach(self):
        return self.handle.detach()


def get_download_progress(quiet: bool = False) -> Progress:
    return Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        DownloadColumn(),
        disable=quiet,
    )
