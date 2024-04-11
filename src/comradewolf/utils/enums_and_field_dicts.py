import enum


class ImportTypes(enum.Enum):
    """
    Types of import from folders containing
    """
    TABLE = "table"
    JOINS = "first_table"
    FILTERS = "filter_name"


class TableTypes(enum.Enum):
    DATA = "data"
    DIMENSION = "dimension"


class FieldType(enum.Enum):
    SELECT = "select"
    VALUE = "value"
    CALCULATION = "calculation"


class TomlStructure:
    """
    Base class for toml import
    """

    def __init__(self, fields: dict):
        self.__fields = fields

    def get_mandatory_single_fields(self) -> list:
        return self.__fields["mandatory_single_fields"]

    def get_mandatory_dictionaries(self) -> list:
        return self.__fields["mandatory_dictionaries"]

    def get_all_mandatory_fields(self) -> list:
        mandatory_fields = []
        mandatory_fields.extend(self.get_mandatory_dictionaries())
        mandatory_fields.extend(self.get_mandatory_single_fields())
        return mandatory_fields


class AllFieldsForImport:
    """
    Contains all field names for any kind of import toml files for StructureGenerator class
    """

    __table_fields = {
        "mandatory_single_fields": ["table", "schema", "database", "table_type", "fields"],
        "mandatory_dictionaries": ["fields"]
    }

    __join_fields = {
        "mandatory_single_fields": ["first_table", "schema", "database"],
        "mandatory_dictionaries": ["second_table"]
    }

    __where_fields = {
        "mandatory_single_fields": [],
        "mandatory_dictionaries": []
    }

    def get_join_fields(self):
        return self.__join_fields

    def get_table_fields(self):
        return self.__table_fields

    def get_where_dictionary(self):
        return self.__where_fields


class FrontendTypeFields(enum.Enum):
    SELECT = "select"
    WHERE = "where"
    CALCULATIONS = "calculations"


class FrontFieldTypes(enum.Enum):
    DATE = "date"
    NUMBER = "number"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    TEXT = "text"


class WhereFieldsProperties(enum.Enum):
    FRONTEND_NAME = "front_end_name"
    FIELDS_LIST = "fields_list"
    WHERE_QUERY = "where_query"
    SHOW_GROUP = "show_group"


class OlapFieldTypes(enum.Enum):
    VALUE = "value"
    DIMENSION = "dimension"
    SERVICE_KEY = "service_key"


class OlapCalculations(enum.Enum):
    """
    Calculations that is applied to the field
    """
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
    NONE = "none"


class OlapFollowingCalculations(enum.Enum):
    """
    Enum of possible next calculations if any was applied
    """
    NONE = "none"
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
