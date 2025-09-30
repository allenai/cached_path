import io
from pathlib import Path
from typing import Optional

from beaker import Beaker
from beaker.exceptions import BeakerChecksumFailedError  # type: ignore
from beaker.exceptions import BeakerDatasetNotFound  # type: ignore

from .scheme_client import SchemeClient


class BeakerClient(SchemeClient):
    scheme = ("beaker",)
    recoverable_errors = SchemeClient.recoverable_errors + (BeakerChecksumFailedError,)

    def __init__(self, resource: str) -> None:
        super().__init__(resource)

        # Beaker resources should be in the form "{user}/{dataset_name}/{path}/{to}/{file}"
        path = Path(resource.split("://")[1])
        if len(path.parts) < 2:
            raise ValueError(
                f"Invalid beaker resource URL '{resource}'. "
                "Resources should be in the form 'beaker://{user_name}/{dataset_name}/{path_to_file}' "
                "or beaker://{dataset_id}/{path_to_file}."
            )

        with Beaker.from_env() as beaker:  # type: ignore
            try:
                user, dataset_name, *filepath_parts = path.parts
                self.dataset = beaker.dataset.get(f"{user}/{dataset_name}")
            except BeakerDatasetNotFound:
                dataset_id, *filepath_parts = path.parts
                self.dataset = beaker.dataset.get(dataset_id)

            self.filepath = "/".join(filepath_parts)
            self.file_info = beaker.dataset.get_file_info(self.dataset, self.filepath)

    def get_etag(self) -> Optional[str]:
        return None if self.file_info.digest is None else str(self.file_info.digest)

    def get_size(self) -> Optional[int]:
        return self.file_info.size

    def get_resource(self, temp_file: io.BufferedWriter) -> None:
        with Beaker.from_env() as beaker:  # type: ignore
            for chunk in beaker.dataset.stream_file(self.dataset, self.filepath):
                if chunk:
                    temp_file.write(chunk)
