from .connect import *
from .db import *
from .helpers import *
from .listen import *


class Connector(BaseConnector):
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
        self.close()
        return False