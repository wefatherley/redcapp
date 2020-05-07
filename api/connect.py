"""Connector objects and related items"""

# ----------------------------------------------------------------------
# imports, includes, hacks, etc.
# ----------------------------------------------------------------------

#import csv
from http import client, HTTPStatus
#import inspect
import json
import os
import socket
import ssl
import urllib.parse

from .helpers import (
    make_pythonic,
    cast_record,
    Payload
)

__all__ = ["Connector"]


# ----------------------------------------------------------------------
# things from environment, and so derived
# ----------------------------------------------------------------------

CERTBUNDLE = os.getenv("REDCAPP_CLIENT_CERTS", None)
HOST = os.getenv("REDCAPP_HOST", None)
API_PATH = os.getenv("REDCAPP_API_DIR", None)
HEADERS = {
    "user-agent" : "redcapp/1.0",
    "content-type" : "application/x-www-form-urlencoded",
    "accept-encoding" : "identity",
    "host": HOST,
    "location": API_PATH
}
TOKEN = os.getenv("REDCAP_TOKEN", None)


# ----------------------------------------------------------------------
# Connector classes
# ----------------------------------------------------------------------


class BaseConnector(client.HTTPSConnection):
    """Base class for connecting to a REDCap API"""

    def __init__(self):
        """Initialize BaseConnector instance"""

        # handle TLS situation
        if CERTBUNDLE:
            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH
            )
            ssl_context.load_cert_chain(CERTBUNDLE)
        else:
            self.ssl_context = ssl.create_default_context()

        # initialize the base connector
        super().__init__(
            host = HOST,
            port = client.HTTPS_PORT, 
            timeout = socket._GLOBAL_DEFAULT_TIMEOUT,
            context = self.ssl_context
        )
    
    @staticmethod
    def post_urlencoded(connection, data=None):
        """Handles POST procedure. Returns response data"""

        # url-encode POST data
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
        
        # POST request data
        try:
            connection.putrequest(
                method="POST",
                url=API_PATH,
                skip_host=True,
                skip_accept_encoding=True
            )
            for k,v in HEADERS.items():
                connection.putheader(k,v)
            connection.endheaders(
                message_body=data,
                encode_chunked=False
            )

        # just raise the problem
        except: raise

        # if no excpetions, get the response
        else:
            response = connection.getresponse()

            # get data on 200
            if response.status != HTTPStatus.OK:
                connection.close()
                raise http.client.HTTPException(
                    "No data :/"
                )
        finally:

            # close and return data
            connection.close()
            return response.read().decode("latin-1")

        # TODO: keep-alive support, link support


class Connector(BaseConnector):
    """Public connector for communicating with REDCap API"""

    def __init__(self, token=None, deserializer=json.loads):
        """Initialize Connector instance"""

        # just get the token
        if not token:
            raise RuntimeError("No API token provided :/")
        else:
            super().__init__()
            self.token = token
            self._loads = deserializer

    def arms(self, action, **kwgs):
        pass

    def events(self, action, **kwgs):
        pass

    def field_names(self, action, **kwgs):
        pass

    def files(self, action, **kwgs):
        pass

    def instruments(self, action, **kwgs):
        pass

    def metadata(self, action, **kwgs):
        pass

    def projects(self, action, **kwgs):
        pass

    def records(self, action, **kwgs):
        pass

    def repeating_ie(self, action, **kwgs):
        pass

    def reports(self, action, **kwgs):
        pass

    def redcap(self, action, **kwgs):
        pass

    def surveys(self, action, **kwgs):
        pass

    def users(self, action, **kwgs):
        pass

    def __enter__(self):
        """Support context manager protocol"""
        return self

    def __exit__(self, typ, val, trb):
        return False


if __name__ == "__main__":
    pass


"""
DEPRECATED LAND:

    def _sync_metadata(self):
        pass


old load-metadata procedure from Connector.__init__:

        if loadmeta:
            metadata = json.loads(
                self.post_urlencoded(
                    connection = self,
                    data = {
                        "token": token,
                        "content": "metadata",
                        "format": "json"
                    }
                )
            )
            fieldnames = json.loads(
                self.post_urlencoded(
                    connection = self,
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
                    lambda d: d["field_name"] == fn["original_field_name"],
                    metadata
                )).pop()
                metadatum["pbl"] = make_pythonic_bl(
                    metadatum["branching_logic"]
                )
                self.meta.append((fn["export_field_name"], metadatum))
            self.meta = dict(self.meta)

    This procedure was removed because it's not a real aspect in wrapping the API
"""
