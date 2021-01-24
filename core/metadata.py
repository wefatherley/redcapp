from ast import literal_eval
from datetime import date, datetime, time
from re import findall, sub


class Metadata(dict):
    """Abstraction of REDCap metadata"""
    
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

    @classmethod
    def dump_logic(cls, logic):
        """Convert Python logic syntax to REDCap logic syntax"""
        pass

    @classmethod
    def load_logic(cls, logic):
        """Convert REDCap logic syntax to Python logic syntax"""
        if not logic:
            return ""
        checkbox_snoop = findall(r"\[[a-z0-9_]*\([0-9]*\)\]", logic)
        if len(checkbox_snoop) > 0:
            for item in checkbox_snoop:
                item = sub(r"\)\]", "\']", item)
                item = sub(r"\(", "___", item)
                item = sub(r"\[", "record[\'", item)
                logic = sub(r"\[[a-z0-9_]*\([0-9]*\)\]", item, logic)
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
            logic = sub(patern, substitute, logic)
        return logic

    def __init__(self, raw_metadata, raw_field_names):
        pass

    def load_record(self, record):
        """Return record with Python typing"""
        for k,v in record:
            v = self.cast_map[
                self[k]["text_validation_type_or_show_slider_number"]
            ](v)
        return record

    def dump_record(self, record):
        """Return Pythonic record as JSON-compliant dict"""
        pass
