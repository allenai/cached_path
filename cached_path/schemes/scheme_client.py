from typing import Optional, IO, Tuple, ClassVar

import requests


class SchemeClient:
    ConnectionErrorTypes: ClassVar[Tuple[BaseException, ...]] = (
        requests.exceptions.ConnectionError,
    )

    def __init__(self, resource: str) -> None:
        self.resource = resource

    def get_etag(self) -> Optional[str]:
        raise NotImplementedError

    def get_resource(self, temp_file: IO) -> None:
        raise NotImplementedError
