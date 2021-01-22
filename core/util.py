from datetime import date, datetime, time
from re import findall, sub


cast_map = {
    "date_dmy": lambda d: date.strptime(d, "%d-%m-%Y"),
    "date_mdy": lambda d: date.strptime(d, "%m-%d-%Y"),
    "date_ymd": lambda d: date.strptime(d, "%Y-%m-%d"),
    "datetime_dmy": lambda d: datetime.strptime(d, "%d-%m-%Y %H:%M"),
    "datetime_mdy": lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M"),
    "datetime_ymd": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M"),
    "datetime_seconds_dmy": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
    "datetime_seconds_mdy": lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M:%S"),
    "datetime_seconds_ymd": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
    "email": str,
    "integer": int,
    "alpha_only": str,
    "number": float,
    "number_1dp_comma_decimal": lambda n: float(sub(r",", ".", n)),
    "number_1dp": float,
    "number_2dp_comma_decimal": lambda n: float(sub(r",", ".", n)),
    "number_2dp": float,
    "number_3dp_comma_decimal": lambda n: float(sub(r",", ".", n)),
    "number_3dp": float,
    "number_4dp_comma_decimal": lambda n: float(sub(r",", ".", n)),
    "number_4dp": float,
    "number_comma_decimal": lambda n: float(sub(r",", ".", n)),
    "phone_australia": str,
    "phone": str,
    "postalcode_australia": str,
    "postalcode_canada": str,
    "ssn": str,
    "time": lambda t: time.strptime(t, "%H:%M"),
    "time_mm_ss": lambda t: time.strptime(t, "%M:%S"),
    "vmrn": str,
    "Zipcode": str,
    "": str
}


def make_logic_pythonic(logic):
    """Convert REDCap logic sytanx to Python logic syntax"""
    if not logic:
        return ""
    checkbox_snoop = re.findall(r"\[[a-z0-9_]*\([0-9]*\)\]", logic)
    if len(checkbox_snoop) > 0:
        for item in checkbox_snoop: #left to right
            item = re.sub(r"\)\]", "\']", item)
            item = re.sub(r"\(", "___", item)
            item = re.sub(r"\[", "record[\'", item)
            logic = re.sub(r"\[[a-z0-9_]*\([0-9]*\)\]", item, logic)
    for pattern, substitute in [
        (r"<=", "Z11Z"),
        (r">=", "X11X"),
        (r"=", "=="),
        (r"Z11Z", "<="),
        (r"X11X", ">="),
        (r"<>", "!="),
        (r"\[", "record[\'"),
        (r"\]", "\']")
    ]:
        logic = re.sub(patern, substitute, logic)
    return logic


def cast_record(record, metadata):
    """Casts a record's data to respective Python type"""
    for k,v in record:
        v = cast_map[
            metadata[k][
                "text_validation_type_or_show_slider_number"
            ]
        ](v)
    return record
