"""Helper objects"""

# ----------------------------------------------------------------------
# imports, includes, hacks, etc.
# ----------------------------------------------------------------------

import datetime
import re

__all__ = [
    "make_pythonic_bl",
    "cast_record"
]


# ----------------------------------------------------------------------
# typing utilities
# ----------------------------------------------------------------------

def date_dmy(item):
    frmt = '%d-%m-%Y'
    return datetime.date.strptime(item, frmt)


def date_mdy(item):
    frmt = '%m-%d-%Y'
    return datetime.date.strptime(item, frmt)


def date_ymd(item):
    frmt = '%Y-%m-%d'
    return datetime.date.strptime(item, frmt)


def datetime_dmy(item):
    frmt = '%d-%m-%Y %H:%M'
    return datetime.datetime.strptime(item, frmt)


def datetime_mdy(item):
    frmt = '%m-%d-%Y %H:%M'
    return datetime.datetime.strptime(item, frmt)


def datetime_ymd(item):
    frmt = '%Y-%m-%d %H:%M'
    return datetime.datetime.strptime(item, frmt)


def datetime_seconds_dmy(item):
    frmt = '%Y-%m-%d %H:%M:%S'
    return datetime.datetime.strptime(item, frmt)


def datetime_seconds_mdy(item):
    frmt = '%m-%d-%Y %H:%M:%S'
    return datetime.datetime.strptime(item, frmt)


def datetime_seconds_ymd(item):
    frmt = '%Y-%m-%d %H:%M:%S'
    return datetime.datetime.strptime(item, frmt)


def time_hm(item):
    frmt = '%H:%M'
    return datetime.time.strptime(item, frmt)


def time_mm_ss(item):
    frmt = '%M:%S'
    return datetime.time.strptime(item, frmt)


def make_decimal(item):
    return float(re.sub(',', '.', item))


cast_map = {
    'date_dmy'                 : date_dmy,
    'date_mdy'                 : date_mdy,
    'date_ymd'                 : date_ymd,
    'datetime_dmy'             : datetime_dmy,
    'datetime_mdy'             : datetime_mdy,
    'datetime_ymd'             : datetime_ymd,
    'datetime_seconds_dmy'     : datetime_seconds_dmy,
    'datetime_seconds_mdy'     : datetime_seconds_mdy,
    'datetime_seconds_ymd'     : datetime_seconds_ymd,
    'email'                    : str,
    'integer'                  : int,
    'alpha_only'               : str,
    'number'                   : float,
    'number_1dp_comma_decimal' : make_decimal,
    'number_1dp'               : float,
    'number_2dp_comma_decimal' : make_decimal,
    'number_2dp'               : float,
    'number_3dp_comma_decimal' : make_decimal,
    'number_3dp'               : float,
    'number_4dp_comma_decimal' : make_decimal,
    'number_4dp'               : float,
    'number_comma_decimal'     : make_decimal,
    'phone_australia'          : str,
    'phone'                    : str,
    'postalcode_australia'     : str,
    'postalcode_canada'        : str,
    'ssn'                      : str,
    'time'                     : time_hm,
    'time_mm_ss'               : time_mm_ss,
    'vmrn'                     : str,
    'Zipcode'                  : str,
    ''                         : str
}


# ----------------------------------------------------------------------
# public objects
# ----------------------------------------------------------------------


def make_pythonic_bl(blogic):
    """
    Accepts branching logic string, and returns
    another with Pythonic syntax.
    """
    checkbox_snoop = re.findall( #left to right
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


def cast_record(record, metadata):
    """Casts a record's data to respective Python type"""
    for k,v in record:
        v = cast_map[
            metadata[k][
                "text_validation_type_or_show_slider_number"
            ]
        ](v)
    return record


# ----------------------------------------------------------------------
# things from environment, and so derived
# ----------------------------------------------------------------------

"""
CREATE SCHEMA IF NOT EXISTS redcapp;

CREATE TABLE IF NOT EXISTS redcapp.atom(
  id CHAR PRIMARY KEY,

  netloc CHAR,
  path CHAR,

  user_id CHAR,

  inserted_at TIMESTAMP,
  modified_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS redcapp.user(
  id CHAR PRIMARY KEY,
  
  name CHAR,
  email CHAR,

  inserted_at TIMESTAMP,
  modified_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS redcapp.query(
  id INTEGER PRIMARY KEY
  
  content CHAR,
  format CHAR,
  type CHAR,
  records CHAR,
  forms CHAR,
  events CHAR,
  rawOrLabel CHAR,
  rawOrLabelHeaders CHAR,
  exportCheckboxLabel CHAR,
  returnFormat CHAR,
  exportSurveyFields CHAR,
  exportDataAccessGroups CHAR,
  filterLogic CHAR,

  inserted_at TIMESTAMP,
  modified_at TIMESTAMP
);
"""


class Environment:
    API_HOST = os.getenv("REDCAP_API_HOST", None)
    API_PATH = os.getenv("REDCAP_API_PATH", None)
    API_TOKEN = os.getenv("REDCAP_API_TOKEN", None)
    HTTP_HEADERS = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
        "location": API_PATH,
        "user-agent": "{}/{}".format("redcapp", "1.0")
    }
    TLS_CERTBUNDLE = os.getenv("REDCAP_API_CERTS", None)
