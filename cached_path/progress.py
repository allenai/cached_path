import io
from typing import Optional

from rich.progress import (
    BarColumn,
    DownloadColumn,
    FileSizeColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeElapsedColumn,
)


class BufferedWriterWithProgress(io.BufferedWriter):
    def __init__(self, writer: io.BufferedWriter, progress: Progress, task_id: TaskID):
        super().__init__(writer.raw)
        self.writer = writer
        self.progress = progress
        self.task_id = task_id
        self.total_written = 0

    def write(self, b) -> int:
        n = self.writer.write(b)
        self.total_written += n
        self.progress.advance(self.task_id, n)
        return n


def get_unsized_download_progress(quiet: bool = False) -> Progress:
    return Progress(
        "[progress.description]{task.description}",
        SpinnerColumn(),
        TimeElapsedColumn(),
        FileSizeColumn(),
        disable=quiet,
    )


def get_sized_download_progress(quiet: bool = False) -> Progress:
    return Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        DownloadColumn(),
        disable=quiet,
    )


def get_download_progress(size: Optional[int], quiet: bool = False) -> Progress:
    return (
        get_sized_download_progress(quiet=quiet)
        if size is not None
        else get_unsized_download_progress(quiet=quiet)
    )
