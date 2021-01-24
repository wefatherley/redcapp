from csv import DictWriter
from logging import getLogger
from os import getenv
from statistics import mean

from .core import *


LOGGER = getLogger(__name__)


class Project:

    def __init__(self, host=None, path=None, token=None):
        """Initialize API connector and metadata for user project"""
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
        