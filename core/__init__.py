from os import getenv

from .connect import *
from .metadata import *


class Project:

    def __init__(self, host=None, path=None, token=None):
        """Initialize API connector and metadata for user project"""
        if host is None and path is None and token is None:
            self.metadata = Metadata()
        else:
            self.api = Connector(
                host or getenv("REDCAP_API_HOST"),
                path or getenv("REDCAP_API_PATH"),
                token or getenv("REDCAP_API_TOKEN")
            )
            with self.api as api:
                self.metadata = Metadata(
                    api.metadata("export"), api.field_names("export")
                )

    def summary(self, **kwargs):
        pass
