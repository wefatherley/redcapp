"""Metadata object"""
from csv import DictReader, DictWriter
from datetime import date, datetime, time
from decimal import Context, Decimal, ROUND_HALF_UP
from itertools import groupby
from logging import getLogger
from re import compile, finditer, sub


LOGGER = getLogger(__name__)


class SQL:
    create_schema = "CREATE SCHEMA IF NOT EXISTS {};\n"
    create_table = "CREATE TABLE IF NOT EXISTS {}.{}();\n"
    add_column = "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {};\n"


COLUMNS = [
    "field_name", "form_name", "section_header", "field_type", "field_label",
    "select_choices_or_calculations", "field_note",
    "text_validation_type_or_show_slider_number" ,"text_validation_min",
    "text_validation_max", "identifier", "branching_logic", "required_field",
    "custom_alignment", "question_number", "matrix_group_name", "matrix_ranking",
    "field_annotation"
]


DCM = { # decimal context map
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


RECORD_TYPE_MAP = {
    "date_dmy": (
        lambda d: date.strptime(d, "%d-%m-%Y"),
        lambda d: d.strftime("%d-%m-%Y"),
        "DATE",
    ),
    "date_mdy": (
        lambda d: date.strptime(d, "%m-%d-%Y"),
        lambda d: d.strftime("%m-%d-%Y"),
        "DATE",
    ),
    "date_ymd": (
        lambda d: date.strptime(d, "%Y-%m-%d"),
        lambda d: d.strftime("%Y-%m-%d"),
        "DATE",
    ),
    "datetime_dmy": (
        lambda d: datetime.strptime(d, "%d-%m-%Y %H:%M"),
        lambda d: d.strftime("%d-%m-%Y %H:%M"),
        "DATETIME",
    ),
    "datetime_mdy": (
        lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M"),
        lambda d: d.strftime("%m-%d-%Y %H:%M"),
        "DATETIME",
    ),
    "datetime_ymd": (
        lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M"),
        lambda d: d.strftime("%Y-%m-%d %H:%M"),
        "DATETIME",
    ),
    "datetime_seconds_dmy": (
        lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
        lambda d: d.strftime("%Y-%m-%d %H:%M:%S"),
        "DATETIME",
    ),
    "datetime_seconds_mdy": (
        lambda d: datetime.strptime(d, "%m-%d-%Y %H:%M:%S"),
        lambda d: d.strftime("%m-%d-%Y %H:%M:%S"),
        "DATETIME",
    ),
    "datetime_seconds_ymd": (
        lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S"),
        lambda d: d.strftime("%Y-%m-%d %H:%M:%S"),
        "DATETIME",
    ),
    "email": (lambda s: s, lambda s: s, "TEXT",),
    "integer": (int, str, "INT",),
    "alpha_only": (lambda s: s, lambda s: s, "TEXT",),
    "number": (
        lambda n: Decimal(sub(r",", ".", n), context=DCM["number"]),
        lambda n: str(n),
        "FLOAT"
    ),
    "number_1dp_comma_decimal": (
        lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_1dp_comma_decimal"]
        ),
        lambda n: sub(r"\.", ",", str(n)),
        "FLOAT",
    ),
    "number_1dp": (
        lambda n: Decimal(n, context=DCM["number_1dp"]),
        lambda n: str(n),
        "FLOAT",
    ),
    "number_2dp_comma_decimal": (
        lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_2dp_comma_decimal"]
        ),
        lambda n: sub(r"\.", ",", str(n)),
        "FLOAT",
    ),
    "number_2dp": (
        lambda n: Decimal(n, context=DCM["number_2dp"]),
        lambda n: str(n),
        "FLOAT"
    ),
    "number_3dp_comma_decimal": (
        lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_3dp_comma_decimal"]
        ),
        lambda n: sub(r"\.", ",", str(n)),
        "FLOAT",
    ),
    "number_3dp": (
        lambda n: Decimal(n, context=DCM["number_3dp"]),
        lambda n: str(n),
        "FLOAT",
    ),
    "number_4dp_comma_decimal": (
        lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_4dp_comma_decimal"]
        ),
        lambda n: sub(r"\.", ",", str(n)),
        "FLOAT",
    ),
    "number_4dp": (
        lambda n: Decimal(n, context=DCM["number_4dp"]),
        lambda n: str(n),,
        "FLOAT",
    ),
    "number_comma_decimal": (
        lambda n: Decimal(
            sub(r",", ".", n), context=DCM["number_comma_decimal"]
        ),
        lambda n: sub(r"\.", ",", str(n)),
        "FLOAT"
    ),
    "phone_australia": (lambda s: s, lambda s: s, "TEXT",),
    "phone": (lambda s: s, lambda s: s, "TEXT",),
    "postalcode_australia": (lambda s: s, lambda s: s, "TEXT",),
    "postalcode_canada": (lambda s: s, lambda s: s, "TEXT",),
    "ssn": (lambda s: s, lambda s: s, "TEXT",),
    "time": (
        lambda t: time.strptime(t, "%H:%M"),
        lambda t: t.strftime("%H:%M"),
        "TIME",
    ),
    "time_mm_ss": (
        lambda t: time.strptime(t, "%M:%S"),
        lambda t: t.strftime("%M:%S"),
        "TIME"
    "vmrn": (lambda s: s, lambda s: s, "TEXT",),
    "Zipcode": (lambda s: s, lambda s: s, "TEXT",),
    "": (lambda s: s, lambda s: s, "TEXT",),
}


LOAD_VARIABLE_RE = compile(r"\[[\w()]+\]")
LOAD_OPERATOR_RE = compile(r"<>|\s=\s|\w=\w")
DUMP_VARIABLE_RE = compile(r"record\['[\w]+'\]")
DUMP_OPERATOR_RE = compile(r"==|!=")


class Metadata(dict):
    """Abstraction for REDCap metadata, and corresponding functionality"""

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
        for match in LOAD_VARIABLE_RE.finditer(logic):
            var_str = match.group(0).strip("[]")
            if "(" in var_str and ")" in var_str:
                var_str = "___".join(
                    [s.strip(")") for s in var_str.split("(")]
                )
            var_str = "record['" + var_str + "']"
            logic = logic[:match.start()] + var_str + logic[:match.end()]
        for match in LOAD_OPERATOR_RE.finditer(logic):
            ope_str = match.group(0)
            if ope_str == "=": ope_str = "=="
            elif ope_str == "<>": ope_str = "!="
            logic = logic[:match.start()] + ope_str + logic[:match.end()]
        return logic

    def dump_logic(self, logic):
        """Convert Python logic syntax to REDCap logic syntax"""
        if not logic:
            return ""
        for match in DUMP_VARIABLE_RE.finditer(logic):
            var_str = match.group(0).lstrip("record['").rstrip("']")
            if "___" in var_str:
                var_str = "(".join(var_str.split("___")) + ")"
            var_str = "[" + var_str + "]"
            logic = logic[:match.start()] + var_str + logic[:match.end()]
        for match in DUMP_OPERATOR_RE.finditer(logic):
            ope_str = match.group(0)
            if ope_str == "==": ope_str = "="
            elif ope_str == "!=": ope_str = "<>"
            logic = logic[:match.start()] + ope_str + logic[:match.end()]
        return logic

    def load_record(self, record):
        """Return record with Python typing"""
        for k,v in record:
            v = RECORD_TYPE_MAP[
                self[k]["text_validation_type_or_show_slider_number"]
            ][0](v)
        return record

    def dump_record(self, record):
        """Return Pythonic record as JSON-compliant dict"""
        for k,v in record:
            v = RECORD_TYPE_MAP[
                self[k]["text_validation_type_or_show_slider_number"]
            ][1](v)
        return record

    def write(self, path, fmt="csv", **kwargs):
        """Write formatted metadata to path"""
        for metadatum in self:
            self[metadatum]["branching_logic"] = self.dump_logic(
                self[metadatum]["branching_logic"]
            )
        if fmt == "csv":
            with open(path, "w", newline="") as fp:
                writer = DictWriter(fp, fieldnames=self.columns)
                writer.writeheader()
                for metadatum in self.raw_metadata:
                    writer.writerow(metadatum)
                for metadatum in self.values():
                    writer.writerow(metadatum)
        elif fmt == "sql_migration":
            migration = open(path, "w")
            if "schema" in kwargs:
                migration.write(SQL.create_schema.format(schema))
            else:
                schema = "public"
            if "sql_by" not in kwargs:
                sql_by = "field_type"
            key = lambda d: d[sql_by]
            for table, columns in groupby(
                sorted(list(self.values()) + self.raw_metadata, key=key),
                key=key
            ):
                migration.write(SQL.create_table.format(pubic, table))
                for column in columns:
                    migration.write(SQL.add_column.format(
                        column, RECORD_TYPE_MAP[
                            column["text_validation_type_or_show_slider_number"]
                        ][2]
                    )
        elif fmt == "html_table":
            pass
        for metadatum in self:
            self[metadatum]["branching_logic"] = self.load_logic(
                self[metadatum]["branching_logic"]
            )


__all__ = ["Metadata"]
