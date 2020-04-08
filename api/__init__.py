import os

from .connect import *
from .helpers import *
from .listen import *


# ----------------------------------------------------------------------
# imports, includes, hacks, etc.
# ----------------------------------------------------------------------

CERTBUNDLE = os.getenv("REDCAPP_CLIENT_CERTS", None)
HOST = os.getenv("REDCAPP_HOST", None)
API_PATH = os.getenv("REDCAPP_API_DIR", None)
HEADERS = {
    "user-agent" : "redcapp/1.0",
    "content-type" : "application/x-www-form-urlencoded",
    "accept-encoding" : "identity",
    "host": HOST
}
TOKEN = os.getenv("REDCAP_TOKEN", None)