"""Connector objects and related items"""
import csv
import http.client
import json
import os
import re
import socket
import ssl
import urllib.parse

from .helpers import make_pythonic


__all__ = [
    "Connector"
]


#
# HTTP state constants
#

CERTBUNDLE = os.getenv("CLIENT_CERTS", None)
HOST = os.getenv("REDCAP_HOST", None)
API_DIR = os.getenv("REDCAP_API_DIR", None)
HEADERS = { # must build in decaptalize
    "user-agent" : "REDCapp/1.0",
    "content-type" : "application/x-www-form-urlencoded",
    "accept-encoding" : "identity",
    "host": HOST
}


#
# REDCap state constants
#

TOKEN = os.getenv("REDCAP_TOKEN", None)


#
# Connectors
#

class BaseConnector(http.client.HTTPSConnection):
    """Base class for connecting to a REDCap API"""
    
    @staticmethod
    def _post_urlencoded(connection, data=None):
        """Handles POST procedure. Returns response data"""
        if data is None or data == "":
            raise http.client.CannotSendRequest(
                "Nothing to POST :/"
            )
        elif isinstance(data, dict):
            data = urllib.parse.urlencode(
                data
            ).encode("latin-1")
        elif isinstance(data, (list, tuple)):
            raise RuntimeError("not implemented :/")
        if connection.sock is None:
            connection.connect()
        try:
            connection.putrequest(
                method="POST",
                url=API_DIR,
                skip_host=True,
                skip_accept_encoding=True
            )
            for k,v in HEADERS.items():
                connection.putheader(k,v)
            connection.endheaders(
                message_body=data,
                encode_chunked=False
            )
        except: raise
        else:
            response = connection.getresponse()
            if response.status != 200:
                connection.close()
                raise http.client.HTTPException(
                    "No data :/"
                )
        finally:
            connection.close()
            return response.read().decode("latin-1")

    def __init__(self):
        """Initialize BaseConnector instance"""
        if CERTBUNDLE:
            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH
            )
            ssl_context.load_cert_chain(CERTBUNDLE)
        else:
            self.ssl_context = ssl.create_default_context()
        self.token = token
        super().__init__(
            host = HOST,
            port = 443, 
            timeout = socket._GLOBAL_DEFAULT_TIMEOUT,
            context = self.ssl_context
        )


class Connector(BaseConnector):
    """Public connector for communicating with REDCap API"""

    def __init__(self, token=TOKEN, loadmeta=False):
        """Initialize Connector instance"""
        super().__init__()
        if not token:
            raise RuntimeError("No API token provided :/")
        else:
            self.token = token
        if meta:
            metadata = json.loads(
                self._post_urlencoded(
                    connection = self,
                    headers = HEADERS,
                    data = {
                        "token": token,
                        "content": "metadata",
                        "format": "json"
                    }
                )
            )
            fieldnames = json.loads(
                self._post_urlencoded(
                    connection = self,
                    headers = HEADERS,
                    data = {
                        "token": token,
                        "content": "exportFieldNames",
                        "format": "json"
                    }
                )
            )
            self.meta = []
            while len(fieldnames) > 0:
                fn = fieldnames.pop()
                metadatum = list(filter(
                    lambda d: d["field_name"] \
                    == fn["original_field_name"],
                    metadata
                )).pop()
                metadatum["pbl"] = make_pythonic(
                    metadatum["branching_logic"]
                )
                self.meta.append(
                    (fn["export_field_name"], metadatum)
                )
            self.meta = dict(self.meta)

    def arms(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        elif action == "delete":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def events(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        elif action == "delete":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def field_names(self, action="export", **kwgs):
        if action != "export":
            raise RuntimeError(
                "Can only export list of export field names"
            )
        pass
        

    def files(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        elif action == "delete":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def instruments(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def metadata(self, action, **kwgs):
        if action == "create":
            pass
        elif action == "import":
            pass
        elif action == "export":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def projects(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def records(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def repeating_ie(self, action="export", **kwgs):
        if action != "export":
            raise RuntimeError(
                "Can only export list of export" \
                + " repeating instruments and events :/"
            )
        pass

    def reports(self, action="export", **kwgs):
        if action != "export":
            raise RuntimeError(
                "Can only export report :/"
            )
        pass

    def redcap(self, action="export", **kwgs):
        if action != "export":
            raise RuntimeError(
                "Can only export REDCap version :/"
            )
        pass

    def surveys(self, action="export", **kwgs):
        if action != "export":
            raise RuntimeError(
                "Can only export surveys :/"
            )
        pass

    def users(self, action, **kwgs):
        if action == "import":
            pass
        elif action == "export":
            pass
        else:
            raise RuntimeError("Bad action :/")

    def __enter__(self):
        pass

    def __exit__(self, typ, val, trb):
        pass
