from http import HTTPStatus, server
from logging import getLogger


LOGGER = getLogger(__name__)


class UIService(server.ThreadingHTTPServer): pass