import io

from rich.progress import (
    FileSizeColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeElapsedColumn,
)


class BufferedWriterWithProgress(io.BufferedWriter):
    def __init__(self, writer: io.BufferedWriter, progress: Progress, task_id: TaskID):
        super().__init__(writer.raw)
        self.progress = progress
        self.task_id = task_id
        self.total_written = 0

    def write(self, b) -> int:
        n = super().write(b)
        self.total_written += n
        self.progress.advance(self.task_id, n)
        return n


def get_unsized_write_progress(quiet: bool = False) -> Progress:
    return Progress(
        "[progress.description]{task.description}",
        SpinnerColumn(),
        TimeElapsedColumn(),
        FileSizeColumn(),
        disable=quiet,
    )
