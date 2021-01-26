from datetime import date, datetime, time
from re import compile, finditer, sub


class Metadata(dict):
    """Abstraction for REDCap metadata, and corresponding functionality"""
    
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
        "time": lambda t: t.strftime("%H:%M"),
        "time_mm_ss": lambda t: t.strftime("%M:%S"),
        "vmrn": str,
        "Zipcode": str,
        "": str
    }

    load_variable_re = compile(r"\[[\w()]+\]")
    load_operator_re = compile(r"<>|\s=\s|\w=\w")
    dump_variable_re = compile(r"record\['[\w]+'\]")
    dump_operator_re = compile(r"==|!=")

    def __init__(self, raw_metadata, raw_field_names):
        """Contructor"""
        self.raw_metadata = {d["field_name"]: d for d in raw_metadata}
        self.raw_field_names = {
            d["export_field_name"]: d for d in raw_field_names
        }

    def __getitem__(self, key):
        """Lazy getter"""
        if key not in self:
            raw_metadatum = self.raw_metadata[
                self.raw_field_names[key]["original_field_name"]
            ]
            metadatum["pbl"] = self.load_logic(metadatum["branching_logic"])
            self.__setitem__(key, metadatum)
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

    def write_sql_migration(self):
        """Return a file-like containing metadata's SQL migration"""
        pass

    def write_edit_table_html(self):
        """Return a file-like containing metadata's HTML edit table"""
        pass
        

class Metadatum(dict):
    """WIP Validated metadata column"""

    def __init__(self):
        """Constructor that refuses kwargs"""
        super().__init__(
            field_name=None,
            form_name=None,
            section_header=None,
            field_type=None,
            field_label=None,
            select_choices_or_calculations=None,
            field_note=None,
            text_validation_type_or_show_slider_number=None,
            text_validation_min=None,
            text_validation_max=None,
            identifier=None,
            branching_logic=None,
            required_field=None,
            custom_alignment=None,
            question_number=None,
            matrix_group_name=None,
            matrix_ranking=None,
            field_annotation=None
        )

    def __setitem__(self, key, value):
        """Validating item setter"""
        pass