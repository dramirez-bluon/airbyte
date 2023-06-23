#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime
from typing import Optional


class RemoteFile(ABC):
    """
    A file in a file-based stream.
    """

    def __init__(self, uri: str, last_modified: datetime, file_type: Optional[str] = None):
        self.uri = uri
        self.last_modified = last_modified
        self.file_type = file_type
