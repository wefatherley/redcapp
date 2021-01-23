"""Connector objects and related items"""
from http import client, HTTPStatus
from logging import getLogger
from urllib.parse import urlencode


LOGGER = getLogger(__name__)


class BaseConnector(client.HTTPSConnection):
    """HTTP logic for REDCap API"""

    path_stack = list()
    req_headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded"
    }
        
    def post(self, data):
        """Handles POST procedure. Returns response data as a string"""
        try:
            self.putrequest(method="POST", url=self.path_stack[-1])
            for k,v in self.req_headers.items():
                self.putheader(k,v)
            self.endheaders(
                message_body=urlencode(data).encode("latin-1")
            )
        except:
            # TODO: perform certain retries
            LOGGER.error(
                "bad request: status=%i, reason=%s",
                response.status, response.reason
            )
            return ""
        else:
            response = self.getresponse()
            response.headers = {k.lower(): v for k,v in response.headers}
            if response.status == 200:
                LOGGER.info(
                    "response received sucessfully: octets=%s",
                    response.headers.get("content-length")
                )
                self.path_stack = self.path_stack[:1]
                return response.read().decode("latin-1")
            elif 300 <= response.status <= 399:
                LOGGER.info(
                    "following redirect: link=%s",
                    response.headers.get("link")
                )
                redirect_path = self.parse_link_header(
                    response.headers.get("link")
                )
                self.path_stack.append(redirect_path)
                return self.post(data)
            elif 400 <= response.status <= 499:
                # TODO: perform certain retries
                LOGGER.error(
                    "bad request: status=%i, reason=%s",
                    response.status, response.reason
                )
                return ""
            elif 500 <= response.status <= 599:
                # TODO: perform certain retries
                LOGGER.error(
                    "API issues: status=%i, reason=%s",
                    response.status, response.reason
                )
                return ""

    @staticmethod
    def parse_link_header(header):
        """Returns a URL from link header value"""
        pass


__all__ = ["BaseConnector"]
