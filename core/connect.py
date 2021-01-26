"""Connector objects and related items"""
from http import client, HTTPStatus
from logging import getLogger
from urllib.parse import urlencode


LOGGER = getLogger(__name__)


class Connector(BaseConnector):
    """REDCap API wrapper"""

    def __init__(self, host, path, token):
        """Construct object with mime type, and environment"""
        if path is None or token is None:
            raise RuntimeError("path and/or token required")
        self.path_stack.append(path)
        self.token = token
        super().__init__(host)
        
    def arms(self, action, **parameters):
        pass

    def events(self, action, **parameters):
        pass

    def field_names(self, action, **parameters):
        pass

    def files(self, action, **parameters):
        # PIL for images?
        pass

    def instruments(self, action, **parameters):
        pass

    def metadata(self, action, **parameters):
        pass

    def projects(self, action, **parameters):
        pass

    def records(self, action, **parameters):
        pass

    def repeating_ie(self, action, **parameters):
        pass

    def reports(self, action, **parameters):
        pass

    def redcap(self, action, **parameters):
        pass

    def surveys(self, action, **parameters):
        pass

    def users(self, action, **parameters):
        pass

    def __enter__(self):
        if self.sock is None:
            self.connect()
        return self

    def __exit__(self, typ, val, trb):
        self.close()
        return False


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
        except Exception as e:
            # TODO: perform certain retries
            LOGGER.error("request threw exception: exc=%s", e)
            return None
        else:
            response = self.getresponse()
            response.headers = {k.lower(): v for k,v in response.headers}
            if response.status == HTTPStatus.OK:
                LOGGER.info(
                    "response received sucessfully: octets=%s",
                    response.headers.get("content-length")
                )
                self.path_stack = self.path_stack[:1]
                return response
            elif (
                HTTPStatus.MULTIPLE_CHOICES
                <= response.status <
                HTTPStatus.BAD_REQUEST
            ):
                LOGGER.info(
                    "following redirect: link=%s",
                    response.headers.get("link")
                )
                redirect_path = self.parse_link_header(
                    response.headers.get("link")
                )
                self.path_stack.append(redirect_path)
                return self.post(data)
            elif (
                HTTPStatus.BAD_REQUEST
                <= response.status <=
                HTTPStatus.INTERNAL_SERVER_ERROR
            ):
                # TODO: perform certain retries
                LOGGER.error(
                    "bad request: status=%i, reason=%s",
                    response.status, response.reason
                )
                return None
            elif HTTPStatus.INTERNAL_SERVER_ERROR <= response.status:
                # TODO: perform certain retries
                LOGGER.error(
                    "API issues: status=%i, reason=%s",
                    response.status, response.reason
                )
                return None

    @staticmethod
    def parse_link_header(header):
        """Returns a URL from link header value"""
        pass


__all__ = ["Connector"]
