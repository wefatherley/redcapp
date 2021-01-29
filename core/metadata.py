"""Metadata object"""
from csv import DictWriter
from datetime import date, datetime, time
from decimal import Context, Decimal, ROUND_HALF_UP
from json import dumps
from logging import getLogger
from re import compile, finditer, sub


LOGGER = getLogger(__name__)


class Metadata(dict):
    """Abstraction for REDCap metadata, and corresponding functionality"""

    # metadata columns
    columns = [
        "field_name", "form_name", "section_header", "field_type", "field_label",
        "select_choices_or_calculations", "field_note",
        "text_validation_type_or_show_slider_number" ,"text_validation_min",
        "text_validation_max", "identifier", "branching_logic", "required_field",
        "custom_alignment", "question_number", "matrix_group_name", "matrix_ranking",
        "field_annotation"
    ]

    # decimal context map
    dcm = {
        "number": Context(prec=None, rounding=ROUND_HALF_UP)
        "number_1dp_comma_decimal": Context(prec=1, rounding=ROUND_HALF_UP)
        "number_1dp": Context(prec=1, rounding=ROUND_HALF_UP)
        "number_2dp_comma_decimal": Context(prec=2, rounding=ROUND_HALF_UP)
        "number_2dp": Context(prec=2, rounding=ROUND_HALF_UP)
        "number_3dp_comma_decimal": Context(prec=3, rounding=ROUND_HALF_UP)
        "number_3dp": Context(prec=3, rounding=ROUND_HALF_UP)
        "number_4dp_comma_decimal": Context(prec=4), rounding=ROUND_HALF_UP
        "number_4dp": Context(prec=4, rounding=ROUND_HALF_UP)
        "number_comma_decimal": Context(prec=None, rounding=ROUND_HALF_UP)
    }
    
    load_cast_map = {
        "date_dmy": lambda d: date.strptime(d, "%d-%m-%Y"),
        "date_mdy": lambda d: date.strptime(d, "%m-%d-%Y"),
        "date_ymd": lambda d: date.strptime(d, "%Y-%m-%d"),
        "datetime_dmy": lambda d: datetime.strptime(d, "%d-%m-%Y %H:%M"),
        "datetime_mdy": lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M"),
        "datetime_ymd": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M"),
        "datetime_seconds_dmy": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
        "datetime_seconds_mdy": lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M:%S"),
        "datetime_seconds_ymd": lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
        "email": lambda s: s,
        "integer": lambda s: s,
        "alpha_only": lambda s: s,
        "number": lambda n: Decimal(sub(r",", ".", n), context=DCM["number"]),
        "number_1dp_comma_decimal": lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_1dp_comma_decimal"]
        ),
        "number_1dp":  lambda n: Decimal(n, context=DCM["number_1dp"]),
        "number_2dp_comma_decimal": lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_2dp_comma_decimal"]
        ),
        "number_2dp": lambda n: Decimal(n, context=DCM["number_2dp"]),
        "number_3dp_comma_decimal": lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_3dp_comma_decimal"]
        ),
        "number_3dp": lambda n: Decimal(n, context=DCM["number_3dp"]),
        "number_4dp_comma_decimal": lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_4dp_comma_decimal"]
        ),
        "number_4dp": lambda n: Decimal(n, context=DCM["number_4dp"]),
        "number_comma_decimal": lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_comma_decimal"]
        ),
        "phone_australia": lambda s: s,
        "phone": lambda s: s,
        "postalcode_australia": lambda s: s,
        "postalcode_canada": lambda s: s,
        "ssn": lambda s: s,
        "time": lambda t: time.strptime(t, "%H:%M"),
        "time_mm_ss": lambda t: time.strptime(t, "%M:%S"),
        "vmrn": lambda s: s,
        "Zipcode": lambda s: s,
        "": lambda s: s,
    }

    dump_cast_map = {
        "date_dmy": lambda d: d.strftime("%d-%m-%Y"),
        "date_mdy": lambda d: d.strftime("%m-%d-%Y"),
        "date_ymd": lambda d: d.strftime("%Y-%m-%d"),
        "datetime_dmy": lambda d: d.strftime("%d-%m-%Y %H:%M"),
        "datetime_mdy": lambda d: d.strftime("%m-%d-%Y %H:%M"),
        "datetime_ymd": lambda d: d.strftime("%Y-%m-%d %H:%M"),
        "datetime_seconds_dmy": lambda d: d.strftime("%Y-%m-%d %H:%M:%S"),
        "datetime_seconds_mdy": lambda d: d.strftime("%m-%d-%Y %H:%M:%S"),
        "datetime_seconds_ymd": lambda d: d.strftime("%Y-%m-%d %H:%M:%S"),
        "email": lambda s: s,
        "integer": int,
        "alpha_only": lambda s: s,
        "number": lambda n: str(n),
        "number_1dp_comma_decimal": lambda n: sub(r"\.", ",", str(n)),
        "number_1dp": lambda n: str(n),
        "number_2dp_comma_decimal": lambda n: sub(r"\.", ",", str(n)),
        "number_2dp": lambda n: str(n),
        "number_3dp_comma_decimal": lambda n: sub(r"\.", ",", str(n)),
        "number_3dp": lambda n: str(n),
        "number_4dp_comma_decimal": lambda n: sub(r"\.", ",", str(n)),
        "number_4dp": lambda n: str(n),
        "number_comma_decimal": lambda n: sub(r"\.", ",", str(n)),
        "phone_australia": lambda s: s,
        "phone": lambda s: s,
        "postalcode_australia": lambda s: s,
        "postalcode_canada": lambda s: s,
        "ssn": lambda s: s,
        "time": lambda t: t.strftime("%H:%M"),
        "time_mm_ss": lambda t: t.strftime("%M:%S"),
        "vmrn": lambda s: s,
        "Zipcode": lambda s: s,
        "": lambda s: s,
    }

    load_variable_re = compile(r"\[[\w()]+\]")
    load_operator_re = compile(r"<>|\s=\s|\w=\w")
    dump_variable_re = compile(r"record\['[\w]+'\]")
    dump_operator_re = compile(r"==|!=")

    def __init__(self, raw_metadata=None, raw_field_names=None):
        """Contructor"""
        if raw_field_names is not None and raw_metadata is not None:
            self.headers = list(raw_metadata[0].keys())
            self.raw_metadata = {d["field_name"]: d for d in raw_metadata}
            self.raw_field_names = {
                d["export_field_name"]: d for d in raw_field_names
            }
        super().__init__()

    def __getitem__(self, key):
        """Lazy getter"""
        if key not in self:
            raw_metadatum = self.raw_metadata.pop(
                self.raw_field_names[key]["original_field_name"]
            )
            raw_metadatum["branching_logic"] = self.load_logic(
                raw_metadatum["branching_logic"]
            )
            self.__setitem__(key, raw_metadatum)
        return super().__getitem__(key)
        
    def evaluate_logic(self, logic):
        try: logic = eval(logic)
        except SyntaxError: logic = eval(self.load_logic(logic))
        else: return logic

    def load_logic(self, logic):
        """Convert REDCap logic syntax to Python logic syntax"""
        if not logic: return ""
        for match in self.load_variable_re.finditer(logic):
            var_str = match.group(0).strip("[]")
            if "(" in var_str and ")" in var_str:
                var_str = "___".join(
                    [s.strip(")") for s in var_str.split("(")]
                )
            var_str = "record['" + var_str + "']"
            logic = logic[:match.start()] + var_str + logic[:match.end()]
        for match in self.load_operator_re.finditer(logic):
            ope_str = match.group(0)
            if ope_str == "=": ope_str = "=="
            elif ope_str == "<>": ope_str = "!="
            logic = logic[:match.start()] + ope_str + logic[:match.end()]
        return logic

    def dump_logic(self, logic):
        """Convert Python logic syntax to REDCap logic syntax"""
        if not logic:
            return ""
        for match in self.dump_variable_re.finditer(logic):
            var_str = match.group(0).lstrip("record['").rstrip("']")
            if "___" in var_str:
                var_str = "(".join(var_str.split("___")) + ")"
            var_str = "[" + var_str + "]"
            logic = logic[:match.start()] + var_str + logic[:match.end()]
        for match in self.dump_operator_re.finditer(logic):
            ope_str = match.group(0)
            if ope_str == "==": ope_str = "="
            elif ope_str == "!=": ope_str = "<>"
            logic = logic[:match.start()] + ope_str + logic[:match.end()]
        return logic

    def load_record(self, record):
        """Return record with Python typing"""
        for k,v in record:
            v = self.load_cast_map[
                self[k]["text_validation_type_or_show_slider_number"]
            ](v)
        return record

    def dump_record(self, record):
        """Return Pythonic record as JSON-compliant dict"""
        for k,v in record:
            v = self.dump_cast_map[
                self[k]["text_validation_type_or_show_slider_number"]
            ](v)
        return record

    def write(self, path, fmt="csv"):
        """Write formatted metadata to path"""
        if fmt == "csv":
            with open(path, "w", newline="") as fp:
                writer = DictWriter(fp, fieldnames=self.columns)
                writer.writeheader()
                for metadatum in self.raw_metadata:
                    writer.writerow(metadatum)
                for metadatum in self.values():
                    metadatum["branching_logic"] = self.dump_logic(
                        metadatum["branching_logic"]
                    )
                    writer.writerow(metadatum)
        elif fmt == "sql_migration": pass
        elif fmt == "html_table": pass


__all__ = ["Metadata"]
