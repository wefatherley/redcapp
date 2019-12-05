__author__ = 'Will Fatherley'
__version__ = '0.1-dev'
__license__ = 'MIT'


import csv
import datetime
import http.client
import itertools
import json
import os
import re
import socket
import ssl
import urllib.parse


CERTBUNDLE = os.getenv("CLIENT_CERTS", None) #dev
MAX_BYTES = os.getenv("REDCAP_MAX_READ", 1000000) #dev
TOKEN = os.getenv("REDCAP_TOKEN", None)
HOST = os.getenv("REDCAP_HOST", None)
API_DIR = os.getenv("REDCAP_API_DIR", None)
HEADERS = { # must build in decaptalize
    "content-type" : "application/x-www-form-urlencoded",
    "accept" : "application/json",
    "accept-encoding" : "identity",
    "host": HOST
} # might need ordereddict
VALID_OPTIONS = {
    option : None
    for option in
    os.getenv(
        "REDCAP_VALID_OPTIONS",
        None
    ).split(",")
}


class MetaDict(dict):
    """Dot-notation container for metadata and field 
    names."""
    @staticmethod
    def make_pythonic(blogic):
        checkbox_snoop = re.findall(
            "\[[a-z0-9_]*\([0-9]*\)\]", 
            blogic
        )
        if len(checkbox_snoop) > 0:
            for item in checkbox_snoop: #left to right
                item = re.sub("\)\]", "\']", item)
                item = re.sub("\(", "___", item)
                item = re.sub("\[", "record[\'", item)
                blogic = re.sub(
                    "\[[a-z0-9_]*\([0-9]*\)\]",
                    item,
                    blogic
                )
            blogic = re.sub("<=", "Z11Z", blogic)
            blogic = re.sub(">=", "X11X", blogic)
            blogic = re.sub("=", "==", blogic)
            blogic = re.sub("Z11Z", "<=", blogic)
            blogic = re.sub("X11X", ">=", blogic)
            blogic = re.sub("<>", "!=", blogic)
            blogic = re.sub("\[", "record[\'", blogic)
            blogic = re.sub("\]", "\']", blogic)
        return blogic

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__dict__.update({key : value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super().__delitem__(key)
        del self.__dict__[key]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                if len(arg) > 3: #it's metadata
                    arg["python_bl"] = make_pythonic(
                        arg["branching_logic"]
                    )
                    self[arg["field_name"]] = arg
                if len(arg) == 3: #it's fieldnames
                    self[arg["export_field_name"]] = arg
        if kwargs:
            for k, v in kwargs.items():
                self[k] = v


def _post_urlencoded(connection, headers, data):
    """Handles HTTP's POST procedure. Note the three
    arguments to this function, as they are our
    abstraction for the underlying HTTP specification,

    i.e.
        POST /index.html HTTP/1.1 ---> `connection`
        content-type: text        ---> `headers`
        accept: *                 ---> `headers`
                                    ---> `headers`
        ?date=&id=&...            ---> `data`

    Returns the the tuple
        ```(
            http.client.HTTPResponse.status, 
            http.client.HTTPResponse.reason, 
            http.client.HTTPResponse.version,
            dict(http.client.HTTPResponse.getheaders()), 
            http.client.HTTPResponse.read()
        )```
    if `http.client.HTTPResponse.status == 200`, and if
    the response's content length header > 0.
    """
    if data is None or data == "":
        # TODO: logging
        raise http.client.CannotSendRequest(
            "Nothing to POST."
        )
    elif isinstance(data, dict):
        data = urllib.parse.urlencode(
            data
        ).encode("latin-1")
    elif isinstance(data, (list, tuple)):
        raise RuntimeError("malformed POST data?")

    if connection.sock is None:
        try:
            connection.connect()
        except:
            # TODO: logging
            raise # might need to ref another attrib
    try:
        connection.putrequest(
            method="POST",
            url=API_DIR,
            skip_host=True,
            skip_accept_encoding=True
        )
        for k,v in headers: # might need ordereddict
            connection.putheader(k,v)
        connection.endheaders(
            message_body=data,
            encode_chunked=False
        )
    except:
        # TODO: logging
        raise
    else:
        resp = connection.getresponse()
        if resp.status != 200:
            raise http.client.HTTPException(
                "No data :/"
            )
        else:
            headers = dict(
                resp.getheaders()
            )
            cl = header.get("content-length")
            if cl is None or cl <= 0 or cl > MAX_BYTES:
                raise Exception(
                    "No data :/"
                )
    finally:
        resp = resp.status, resp.reason, resp.version, \
            headers, resp.read().decode("latin-1")
        self.close()
        return resp


class Connector(http.client.HTTPSConnection):
    """Context manager for working with REDCap API."""
    def do_export(self, token=TOKEN, **parameters):
        paramaters = {"token": token, **parameters}
        response = _post_urlencoded(
            connection = self,
            headers = HEADERS,
            data = parameters
        )
        if parameters["content"] == "metadata":
            return Metadata(response[-1])
        if parameters["content"] == "exportFieldNames":
            return Fieldnames(response[-1])

    def do_import(self, token=TOKEN, **parameters):
        pass

    def __init__(self, metadata=None, fieldnames=None):
        if CERTBUNDLE:
            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH
            )
            ssl_context.load_cert_chain(CERTBUNDLE)
        else:
            self.ssl_context = ssl.create_default_context()
        super().__init__(
            host = HOST,
            port = 443, 
            timeout = socket._GLOBAL_DEFAULT_TIMEOUT,
            context = self.ssl_context
        )
        if metadata:
            self.metadata = self.do_export(
                content="metadata"
            )
        if fieldname:
            self.fieldnames = self.do_export(
                content="exportFieldNames"
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # rm data from memory
        self.close()


#
# Deprecated
#


# TEXTUAL_CONTENT = [
#     'arm',
#     'event',
#     'exportFieldNames',
#     'instrument',
#     'formEventMapping',
#     'metadata',
#     'project',
#     'record',
#     'repeatingFormsEvents',
#     'report',
#     'version',
#     'user'
# ]


# ###############################################################################
# # Connection object
# ###############################################################################


# class Connect(http.client.HTTPSConnection):
    
    
#     def flush(self):
        
#         # (re)sets the following attributes
#         self.headers = None
#         self.status  = None
#         self.reason  = None
        
        
#     def __init__(self, host = HOST, port = PORT, timeout = 5, 
#                  context = ssl.create_default_context(), **cached_options):
        
#         # these attributes are required to make requests from REDCap
#         # NOTE: `host` and `port` are located in `REDCap_rc`
#         self.host    = host
#         self.port    = port
#         self.timeout = timeout
#         self.context = context
        
#         # `self.memory` is a viable but poor way
#         # to avoid using dir() :(
#         self.memory  = []
        
#         self.cached_options = cached_options
        
        
#     def post(self, data_format = None, **options):
        
#         # clear request attributes
#         self.flush()
        
#         # if no options, raise error
#         if not options and not self.options_cache:
#             raise RuntimeError('No export performed, \
#                                 please provide valid options.'
#             )
        
#         _options = {**self.cached_options, **options}
        
#         self.memory.append(_options['content'])
#         self.memory = list(set(self.memory))
        
#         # concatenate options dict into string
#         if data_format:
#             _options = options_concat(_options) \
#                        + '&format={0}'.format(data_format)
#         else:
#             _options = options_concat(_options)
        
#         # initialize an HTTPSConnection object
#         super().__init__(host = self.host, port = self.port, 
#                          timeout = self.timeout, context = self.context)

#         # ask REDCap for data
#         self.request(
#             method  = 'POST',
#             url     = URL,
#             body    = _options,
#             headers = HEADERS
#         )
        
#         # set `Connect` request attributes with requested data
#         with self.getresponse() as response:
            
#             self.headers = response.getheaders()
#             self.status  = response.status
#             self.reason  = response.reason
            
#             # read the data REDCap sent back
#             if self.status == 200:
                
#                 if data_format:
                    
#                     setattr(self, '{0}'.format(options['content']),
#                         json.loads(
#                             response.read().decode('ascii')
#                         )
#                     )
                    
#                 else:
                    
#                     setattr(self, '{0}'.format(options['content']),
#                             response.read()
#                     )
                    
#         # close the connection
#         self.close()
        
        
#     def write_data(self, filename, content):
        
#         if content in self.memory:
            
#             # if `self.data` is bytes
#             if content != TEXTUAL_CONTENT:
#                 with open(filename, 'wb') as fp:
#                     fp.write(getattr(self, content))
            
#             # currently optimal for content of type `record` or `metadata`
#             else:
#                 with open(filename, 'w', newline='') as fp:
#                     writer = csv.DictWriter(
#                         fp, 
#                         fieldnames = list(self.record[0].keys())
#                     )
#                     writer.writeheader()
#                     for k in range(len(self.record)):
#                         writer.writerow(self.record[k])        


# ###############################################################################
# # casting rules for fields
# ###############################################################################


# def date_dmy(str):
#     frmt = '%d-%m-%Y'
#     return datetime.date.strptime(str, frmt)


# def date_mdy(str):
#     frmt = '%m-%d-%Y'
#     return datetime.date.strptime(str, frmt)


# def date_ymd(str):
#     frmt = '%Y-%m-%d'
#     return datetime.date.strptime(str, frmt)


# def datetime_dmy(str):
#     frmt = '%d-%m-%Y %H:%M'
#     return datetime.datetime.strptime(str, frmt)


# def datetime_mdy(str):
#     frmt = '%m-%d-%Y %H:%M'
#     return datetime.datetime.strptime(str, frmt)


# def datetime_ymd(str):
#     frmt = '%Y-%m-%d %H:%M'
#     return datetime.datetime.strptime(str, frmt)


# def datetime_seconds_dmy(str):
#     frmt = '%Y-%m-%d %H:%M:%S'
#     return datetime.datetime.strptime(str, frmt)


# def datetime_seconds_mdy(str):
#     frmt = '%m-%d-%Y %H:%M:%S'
#     return datetime.datetime.strptime(str, frmt)


# def datetime_seconds_ymd(str):
#     frmt = '%Y-%m-%d %H:%M:%S'
#     return datetime.datetime.strptime(str, frmt)


# def _time(str):
#     frmt = '%H:%M'
#     return datetime.time.strptime(str, frmt)


# def time_mm_ss(str):
#     frmt = '%M:%S'
#     return datetime.time.strptime(str, frmt)


# def make_decimal(str):
#     return float(re.sub(',', '.', str))


# TEXT_VALIDATION_TYPES = {
#     'date_dmy'                 : date_dmy,
#     'date_mdy'                 : date_mdy,
#     'date_ymd'                 : date_ymd,
#     'datetime_dmy'             : datetime_dmy,
#     'datetime_mdy'             : datetime_mdy,
#     'datetime_ymd'             : datetime_ymd,
#     'datetime_seconds_dmy'     : datetime_seconds_dmy,
#     'datetime_seconds_mdy'     : datetime_seconds_mdy,
#     'datetime_seconds_ymd'     : datetime_seconds_ymd,
#     'email'                    : str,
#     'integer'                  : int,
#     'alpha_only'               : str,
#     'number'                   : float,
#     'number_1dp_comma_decimal' : make_decimal,
#     'number_1dp'               : float,
#     'number_2dp_comma_decimal' : make_decimal,
#     'number_2dp'               : float,
#     'number_3dp_comma_decimal' : make_decimal,
#     'number_3dp'               : float,
#     'number_4dp_comma_decimal' : make_decimal,
#     'number_4dp'               : float,
#     'number_comma_decimal'     : make_decimal,
#     'phone_australia'          : str,
#     'phone'                    : str,
#     'postalcode_australia'     : str,
#     'postalcode_canada'        : str,
#     'ssn'                      : str,
#     'time'                     : _time,
#     'time_mm_ss'               : time_mm_ss,
#     'vmrn'                     : str,
#     'Zipcode'                  : str,
#     ''                         : str
# }


# #
# # dev
# #



# class REDCap(http.client.HTTPSConnection):
#     """
#     HTTP client as an abstraction of a REDCap instance. Conveniently
#     exposes the REDCap API.
#     """
#     def __init__(self, host=HOST, api_dir=API_DIR,
#                             headers=HEADERS, token=None):
        
#         if host is None or api_dir is None or token is None:
#             raise RuntimeError(
#                 "REDCap connection failed in __init__"
#             )
#         else:
#             self.host = host
#             self.api_dir = api_dir
#             self.headers = headers
#             self.token = token
            
#         try:
#             super().__init__(host=self.host)
#             self.putrequest(
#                 method = "POST",
#                 url = self.api_dir,
#                 skip_host = True,
#                 skip_accept_encoding = True
#             )
#             for k,v in self.headers.items():
#                 self.putheader(k, v)
#             self.endheaders()
#             self.send(
#                 urllib.parse.urlencode([
#                     ("token", self.token),
#                     ("content", "project"),
#                     ("format", "json"),
#                     ("returnFormat", "json")
#                 ]).encode("latin-1")
#             )
#             init_response = self.getresponse()
#         except:
#             raise
#         else:
#             if init_response.status >= 400:
#                 raise HTTPException(
#                     "Couldn\'t load REDCap project ({}, {})".format(
#                         init_response.status, init_response.reason
#                     )
#                 )
#         finally:
#             self.project_info = json.loads(
#                 init_response.read().decode("latin-1")
#             )[0]
        
#     def do_export(self, *handlers, **options):
#         pass
#     def do_import(self, *handlers, **options):
#         pass

#     def __enter__(self):
#         pass
#     def __exit__(self, typ, val, trb):
#         pass
