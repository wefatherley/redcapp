from logging import getLogger

from .connect import *
from .util import *


class Connector(BaseConnector):
    """Communicates with REDCap API via local state"""

    def __init__(self, mime_type, host, path, token):
        """Construct object with mime type, and environment"""
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
        """Support context manager protocol"""
        return self

    def __exit__(self, typ, val, trb):
        self.close()
        return False


__all__ = ["Connector", "make_logic_pythonic", "make_sql_migration"]