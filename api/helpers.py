import datetime
import re


def date_dmy(str):
    frmt = '%d-%m-%Y'
    return datetime.date.strptime(str, frmt)


def date_mdy(str):
    frmt = '%m-%d-%Y'
    return datetime.date.strptime(str, frmt)


def date_ymd(str):
    frmt = '%Y-%m-%d'
    return datetime.date.strptime(str, frmt)


def datetime_dmy(str):
    frmt = '%d-%m-%Y %H:%M'
    return datetime.datetime.strptime(str, frmt)


def datetime_mdy(str):
    frmt = '%m-%d-%Y %H:%M'
    return datetime.datetime.strptime(str, frmt)


def datetime_ymd(str):
    frmt = '%Y-%m-%d %H:%M'
    return datetime.datetime.strptime(str, frmt)


def datetime_seconds_dmy(str):
    frmt = '%Y-%m-%d %H:%M:%S'
    return datetime.datetime.strptime(str, frmt)


def datetime_seconds_mdy(str):
    frmt = '%m-%d-%Y %H:%M:%S'
    return datetime.datetime.strptime(str, frmt)


def datetime_seconds_ymd(str):
    frmt = '%Y-%m-%d %H:%M:%S'
    return datetime.datetime.strptime(str, frmt)


def time_hm(str):
    frmt = '%H:%M'
    return datetime.time.strptime(str, frmt)


def time_mm_ss(str):
    frmt = '%M:%S'
    return datetime.time.strptime(str, frmt)


def make_decimal(str):
    return float(re.sub(',', '.', str))


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


def make_pythonic(blogic):
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
