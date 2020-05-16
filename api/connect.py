"""Connector objects and related items"""

# ----------------------------------------------------------------------
# imports, includes, hacks, etc.
# ----------------------------------------------------------------------

#import csv
from http import client, HTTPStatus
import itertools
import json
import os
import socket
import ssl
import urllib.parse

from . import RedcapException
from .helpers import (
    make_pythonic,
    cast_record,
    Payload
)

__all__ = ["Connector"]


# ----------------------------------------------------------------------
# Connector objects
# ----------------------------------------------------------------------


class BaseConnector(client.HTTPSConnection):
    """
    Base class for connecting to a REDCap API. Handles
    lower-level aspects like HTTP requests and other
    stuff down toward the transport layer.
    """

    def __init__(self):
        """Initialize BaseConnector instance"""

        if CERTBUNDLE:
            ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH
            )
            ssl_context.load_cert_chain(CERTBUNDLE)
        else:
            ssl_context = ssl.create_default_context()

        super().__init__(
            host = HOST,
            port = client.HTTPS_PORT, 
            timeout = socket._GLOBAL_DEFAULT_TIMEOUT,
            context = ssl_context
        )
    
    def getresponse(self, *args, **kwgs):
        pass

    # TODO: move to Connector
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
            raise RedcappException("Cannot URL-encode data :/")
        
        # POST request data
        try:

            # put together and send request
            connection.putrequest(
                method="POST",
                url=API_PATH,
                skip_host=False,
                skip_accept_encoding=False
            )
            for k,v in HEADERS.items():
                connection.putheader(k,v)
            connection.endheaders(
                message_body=data,
                encode_chunked=False
            )

            # instantiate response, headers, etc
            response = connection.getresponse()
            resp_headers = response.getheaders()
            if len(resp_headers) > 1:
                resp_headers.sort(key=lambda t: t[0])
                resp_headers = {
                    k.lower() : v for k, v
                    in itertools.groupby(
                        resp_headers,
                        key=lambda t: t[0]
                    )
                }
            else: resp_headers = dict(resp_headers)

        # WIP: exception blocks
        except client.ResponseNotReady: raise
        except ConnectionError: raise
        except: raise

        # do course of action on HTTP headers
        else:
            if resp_headers.get("connection") == "keep-alive":
                connection.timeout, connection.retries = resp_headers.get(
                    "keep-alive"
                ).split(",")

                
        # 
        finally:
            return response.read().decode("latin-1")

        # TODO: finish keep-alive support, begin link support


class Connector(BaseConnector):
    """
    Public connector for communicating with REDCap API. This
    class is where all the API-specific logic is dealt with.
    """

    def __init__(self, token=TOKEN, deserializer=json.loads):
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
        # PIL for images?
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


# ----------------------------------------------------------------------
# Session object
# ----------------------------------------------------------------------

class Session:


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

    This procedure was removed because it's not a necessary
    aspect in wrapping the API -will
"""
