import io
from pathlib import Path
from typing import Optional

from beaker import Beaker, ChecksumFailedError, DatasetNotFound, DatasetReadError

from .scheme_client import SchemeClient


class BeakerClient(SchemeClient):
    scheme = ("beaker",)
    recoverable_errors = SchemeClient.recoverable_errors + (DatasetReadError, ChecksumFailedError)

    def __init__(self, resource: str) -> None:
        super().__init__(resource)
        self.beaker = Beaker.from_env()
        # Beaker resources should be in the form "{user}/{dataset_name}/{path}/{to}/{file}"
        path = Path(resource.split("://")[1])
        if len(path.parts) < 2:
            raise ValueError(
                f"Invalid beaker resource URL '{resource}'. "
                "Resources should be in the form 'beaker://{user_name}/{dataset_name}/{path_to_file}' "
                "or beaker://{dataset_id}/{path_to_file}."
            )

        try:
            user, dataset_name, *filepath_parts = path.parts
            self.dataset = self.beaker.dataset.get(f"{user}/{dataset_name}")
        except DatasetNotFound:
            dataset_id, *filepath_parts = path.parts
            self.dataset = self.beaker.dataset.get(dataset_id)

        self.filepath = "/".join(filepath_parts)
        self.file_info = self.beaker.dataset.file_info(self.dataset, self.filepath)

    def get_etag(self) -> Optional[str]:
        return None if self.file_info.digest is None else str(self.file_info.digest)

    def get_size(self) -> Optional[int]:
        return self.file_info.size

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        for chunk in self.beaker.dataset.stream_file(self.dataset, self.filepath, quiet=True):
            if chunk:
                temp_file.write(chunk)
