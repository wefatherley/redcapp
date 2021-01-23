from os import getenv

from .core import *


class REDCap:
    def __init__(self, **kwargs):
        if "mime_type" not in kwargs:
            mime_type = "json"
        self.api = Connector(
            mime_type,
            kwargs["host"] or getenv("REDCAP_API_HOST"),
            kwargs["path"] or getenv("REDCAP_API_PATH"),
            kwargs["token"] or getenv("REDCAP_API_TOKEN")
        )
        