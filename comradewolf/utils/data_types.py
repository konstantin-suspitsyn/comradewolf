import warnings
from collections import UserDict

from comradewolf.utils.enums_and_field_dicts import FrontendTypeFields, TableTypes, FieldType, FrontFieldTypes
from comradewolf.utils.exceptions import UnknownFieldType, FieldsFromFrontendWrongValue, UnknownTableType, \
    UnknownFieldTypeForField, NotAllMandatoryFields, ObjectExists
from comradewolf.utils.utils import get_table_from_field


class AllTables(UserDict):
    """
    Dictionary containing all tables
    Structure:
    {
        "table_name": "table_type" # table name should be in style of {database}.{schema}.{field} (no {})
                                   # table type should be one of class TableTypes(enum.Enum)
    }
    """

    def __init__(self) -> None:

        super().__init__()

    def add_table(self, table_name: str, table_type: str) -> None:
        """
        Add a table to the dictionary
        :param table_name:
        :param table_type:
        :return: None
        """
        self.__does_table_exists(table_name)
        self.data[table_name] = table_type

    @staticmethod
    def __check_table_type(field_name: str, type_name: str) -> bool:
        """
        Checks if type_name is one of TableTypes(enum.Enum).values
        :param field_name:
        :param type_name:
        :raises UnknownFieldTypeForField: If type_name is not one of TableTypes(enum)
        :return: True if type_name is one of TableTypes()
        """
        table_types: list = [f.value for f in TableTypes]
        if type_name not in table_types:
            raise UnknownFieldTypeForField(field_name, type_name)

        return True

    def __does_table_exists(self, table_name) -> bool:
        """
        Checks if table_name is in self.data
        :param table_name:
        :raises :
        :return:
        """
        if table_name in self.data.keys():
            raise ObjectExists(f"{table_name} уже существует")

        return False

    def get_table_type(self, table_name: str) -> str:
        """
        Returns table type
        :param table_name:
        :return:
        """
        return self.data[table_name]


class AllFields(UserDict):
    """
    Data type for all fields
    Structure:
    {"field_name": str # Name of field in style of {database}.{schema}.{field} (no {})
        {
            show: boolean, # if we show this field on frontend
            frontend_name: str, # Name of the field for human to read on frontend. Optional. Only shown if show == True
            front_field_type: str, # Optional. Only shown if show == True. Should be one of FrontFieldTypes(enum.Enum)
            show_group: str, # Shows group for frontend. Optional. Only shown if show == True
            field_type: str, # Type of field. Should be one of FieldType(enum.Enum)
            calculation: str, # Exact calculation formula. Optional. Only shown if type == FieldType.CALCULATION.value
            included_fields: list[str] # List of fields included into calculation and where of calculation.
                                         Optional. Only shown if type == FieldType.CALCULATION.value
            where: str | None # Where for calculation if needed.
                                Optional. Only shown if type == FieldType.CALCULATION.value
        }
    }
    """

    def __init__(self, dictionary: dict | None = None) -> None:
        if dictionary is None:
            dictionary = {}
        super().__init__(dictionary)

    def add_field(self,
                  field_name: str,
                  show: bool,
                  field_type: str,
                  frontend_name: str | None = None,
                  front_field_type: str | None = None,
                  show_group: str | None = None,
                  calculation: str | None = None,
                  included_fields: list[str] | None = None,
                  where: str | None = None
                  ) -> None:
        """
        Adds a field to the dictionary
        For fields read class structure
        :param field_name:
        :param show:
        :param field_type:
        :param frontend_name:
        :param front_field_type:
        :param show_group:
        :param calculation:
        :param included_fields:
        :param where:
        :return:
        """

        self.__is_field_exists(field_name)
        self.data[field_name] = {}

        self.__check_field_type(field_name, field_type)

        self.data[field_name]['show'] = show
        self.data[field_name]['field_type'] = field_type

        # If this field will be used in frontend
        if show is True:
            self.__check_show_properties(field_name, frontend_name, front_field_type, show_group)
            self.data[field_name]['frontend_name'] = frontend_name
            self.data[field_name]['front_field_type'] = front_field_type
            self.data[field_name]['show_group'] = show_group

        # If this field is calculation
        if field_type == FieldType.CALCULATION.value:
            self.__check_calculation(field_name, calculation, included_fields)
            self.data[field_name]['calculation'] = calculation
            self.data[field_name]['included_fields'] = included_fields
            self.data[field_name]['where'] = where

    def get_field_type(self, field_name):
        return self.data[field_name]['field_type']

    def __is_field_exists(self, field_name: str) -> bool:
        """
        Checks if field exists in self.data
        :param field_name: name of field
        :return: True or false
        """
        if field_name in self.data.keys():
            warnings.warn(f"Поле {field_name} уже существует")
            return True

        return False

    @staticmethod
    def __check_field_type(field_name: str, field_type: str) -> None:
        """
        Checks type of the field
        :param field_name:
        :param field_type: should be one of FieldType(enum.Enum)
        :throws UnknownFieldTypeForField: if the field != on of FieldType(enum.Enum)
        :return: None
        """
        if field_type not in [f.value for f in FieldType]:
            raise UnknownFieldTypeForField(field_name, field_type)

    @staticmethod
    def __check_show_properties(field_name: str,
                                frontend_name: str | None,
                                front_field_type: str | None,
                                show_group: str | None) -> None:
        """
        Checks if all mandatory fields for front-end are present
        :param field_name:
        :param frontend_name:
        :param front_field_type:
        :param show_group:
        :return:
        """
        if (frontend_name is None) or (front_field_type is None) or (show_group is None):
            no_field = (f"Одно из обязательных полей для {field_name} отсутствует frontend_name: {frontend_name}, "
                        f"front_field_type: {front_field_type}, show_group: {show_group}")
            raise NotAllMandatoryFields(no_field)

        if front_field_type not in [f.value for f in FrontFieldTypes]:
            raise UnknownFieldTypeForField(field_name, front_field_type)

    @staticmethod
    def __check_calculation(field_name: str,
                            calculation: str | None = None,
                            included_fields: list[str] | None = None) -> None:
        """
        Checks if all calculation fields are ok
        :param field_name
        :param calculation:
        :param included_fields:
        :return:
        """
        if (calculation is None) or (len(calculation) == 0) or (included_fields is None) or \
                (len(included_fields) == 0) or (included_fields is None):
            raise NotAllMandatoryFields(f"Не все поля {field_name} заполнены")

    def get_backend_field_type(self, field_name: str) -> str:
        """
        Returns field type for field_name
        :param field_name:
        :return:
        """
        return self.data[field_name]["field_type"]

    def get_calculation(self, field_name: str) -> str:
        return self.data[field_name]["calculation"]

    def get_calculation_where(self, field_name: str) -> str | None:
        where: str | None = None

        if "where" in self.data[field_name]:
            where = self.data[field_name]["where"]

        return where

    def get_included_fields(self, field_name) -> list:
        included_fields: list = []

        if "included_fields" in self.data[field_name]:
            included_fields = self.data[field_name]["included_fields"]

        return included_fields

    def get_frontend_name(self, field_name: str) -> str:
        return self.data[field_name]["frontend_name"]

    def get_frontend_type(self, field_name: str) -> str:
        return self.data[field_name]["front_field_type"]


class FieldsFromFrontend(UserDict):
    """
    Type from frontend to other types of query builder
    dictionary with fields from frontend
            {
                "select": [list of fields to select, ...],
                "calculations": [list of calculations],
                "where": {where_string: where_string, fields_used_in_where: [fields]}
            }
    """

    def __init__(self, dictionary=None):

        if dictionary is None:
            dictionary = {}

        if isinstance(dictionary, dict):

            # Check correct types
            possible_fields: list = [f.value for f in FrontendTypeFields if f != FrontendTypeFields.WHERE.value]

            # Where should be dict or null
            if (not isinstance(dictionary[FrontendTypeFields.WHERE.value], list)) and \
                    dictionary[FrontendTypeFields.WHERE.value] is not None:
                raise FieldsFromFrontendWrongValue(FrontendTypeFields.WHERE.value, "dict or null",
                                                   type(dictionary[FrontendTypeFields.WHERE.value]))
            # Other keys should be list
            for key in possible_fields:
                if not isinstance(dictionary[key], list):
                    raise FieldsFromFrontendWrongValue(key, "list", type(dictionary[key]))

            # Create complete structure if some fields are missing
            for key in FrontendTypeFields:
                if FrontendTypeFields.WHERE.value not in dictionary:
                    dictionary[FrontendTypeFields.WHERE.value] = {}
                if key.value not in dictionary:
                    dictionary[key.value] = []

        super().__init__(dictionary)

    def add_select(self, select_fields: list) -> None:
        """
        Adds select parameters to the dictionary
        :param select_fields: list of fields to add
        :return: None
        """
        for select_field in select_fields:
            if select_field not in self.data[FrontendTypeFields.SELECT.value]:
                self.data[FrontendTypeFields.SELECT.value].append(select_field)

    def add_calculation(self, calculation_fields: list) -> None:
        """
        Adds calculation parameters to the dictionary
        :param calculation_fields: list of fields to add
        :return: None
        """
        for calculation_field in calculation_fields:
            if calculation_field not in self.data[FrontendTypeFields.CALCULATIONS.value]:
                self.data[FrontendTypeFields.CALCULATIONS.value].append(calculation_field)

    def create_or_replace_where(self, where: dict) -> None:
        """
        Replaces where dictionary
        :param where: new where dictionary
        :return:
        """
        self.data[FrontendTypeFields.WHERE.value] = where

    def get_fields_by_type(self, type_of_fields: str) -> list:
        possible_field_type_values = [ft.value for ft in FrontendTypeFields]
        if type_of_fields not in possible_field_type_values:
            raise UnknownFieldType(type_of_fields)

        return self.data[type_of_fields]


class CteFields(UserDict):
    """
    Dictionary for CTE
    Which fields to use for join of main cte and fact_tables CTEs in QueryGenerator class
    """

    def add_table_if_not_exists(self, table_name: str) -> None:
        if table_name not in self.data:
            self.data[table_name] = set()

    def add_field(self, table_name: str, fields_to_join: str):
        self.add_table_if_not_exists(table_name)
        self.data[table_name].add(fields_to_join)

    def update_field(self, table_name: str, fields_to_join: set):
        self.add_table_if_not_exists(table_name)
        self.data[table_name].update(fields_to_join)


class FactTableJoins(UserDict):
    """
    All joins that should appear between FactTables

    In structure:
    {
        "fact_table_left": {
            "fact_table_right": []
        }
    }

    """

    def add_join(self, left_table: str, right_table: str, join_tables: list[str]):
        """
        Add joins between two fact tables
        :param left_table:
        :param right_table:
        :param join_tables:
        :return:
        """
        if left_table not in self.data:
            self.data[left_table] = {}

        self.data[left_table][right_table] = join_tables


class WhereFields(UserDict):
    """
    Pre-defined WhereFields
    Structure:
        {"where_field_name": {
            front_name: front_name_value,
            query: query_value,
            fields: [field_name_value, ],
            show_group: show_group_name
            }
        }
    """

    def __init__(self) -> None:
        base_dict = {}
        super().__init__(base_dict)

    def add_where_field(self,
                        where_field_name: str,
                        front_name: str,
                        query: str,
                        fields: list,
                        show_group: str) -> None:
        """
        Adds one pre-defined where field
        :param show_group:
        :param where_field_name:
        :param front_name:
        :param query:
        :param fields:
        :return:
        """
        self.data[where_field_name] = {
            "front_name": front_name,
            "query": query,
            "fields": fields,
            "show_group": show_group
        }

    def get_show_group(self, field_name: str) -> str:
        return self.data[field_name]["show_group"]

    def get_frontend_name(self, field_name: str) -> str:
        return self.data[field_name]["front_name"]

    def get_fields(self, table_name: str) -> list:
        return self.data[table_name]["fields"]

    def get_where_query(self, field_name: str) -> str:
        return self.data[field_name]["query"]


class AllJoins(UserDict):
    """
    Dictionary containing all joins between tables
    Structure:
    {
    "left_table_name":
        {"right_table_name":
            {
                "how": "join_type", # inner/left/right
                "on": {
                        "between_tables": [join_signs: str], # =/</>
                        "first_table_on": [field_name: str], # field names
                        "second_table_on": [field_name: str], # field names
                      }
            }
        }
    }
    """

    def __init__(self) -> None:
        super().__init__()

    def add_join(self,
                 left_table_name: str,
                 right_table_name: str,
                 how: str,
                 between_table_signs: list[str],
                 first_table_on: list[str],
                 second_table_on: list[str]) -> None:
        """
        Add joins between tables
        :param right_table_name:
        :param left_table_name: name of left table
        :param how: inner/left/right/outer
        :param between_table_signs: list of "=", "<", ">"
        :param first_table_on: list of fields of left table
        :param second_table_on: list of fields of right table
        :raises ValueError: if how is not "inner/left/right/outer"
                            if one of between_table_signs, first_table_on, second_table_on have different lengths
                                or one of them is empty
        :return:
        """
        self.__check_values(left_table_name, between_table_signs, first_table_on, how, second_table_on)

        if left_table_name not in self.data:
            self.data[left_table_name] = {}

        self.data[left_table_name][right_table_name] = {
            "how": how,
            "on": {
                "between_tables": between_table_signs,
                "first_table_on": first_table_on,
                "second_table_on": second_table_on,
            }
        }

    @staticmethod
    def __check_values(table_name: str,
                       between_table_signs: list[str],
                       first_table_on: list[str],
                       how: str,
                       second_table_on: list[str]) -> None:
        """

        :param table_name:
        :param between_table_signs:
        :param first_table_on:
        :param how:
        :param second_table_on:
        :raises ValueError: if how is not "inner/left/right/outer"
                            if one of between_table_signs, first_table_on, second_table_on have different lengths
                                or one of them is empty
        :return:
        """
        if len(between_table_signs) == 0:
            raise ValueError(f"Проблема с джойнами в {table_name}")
        if len(first_table_on) != len(second_table_on) \
                or len(between_table_signs) != len(first_table_on) \
                or len(second_table_on) != len(between_table_signs):
            raise ValueError(f"Проблема с джойнами в {table_name}")
        if how not in ["inner", "left", "right", "outer"]:
            raise ValueError(f"Джойн таблицы {table_name} некорректного типа {how}")


class FieldsForQuery(UserDict):
    """
    Format that is created from frontend dictionary
    {
        "data": # everything containing by data tables
            {
                table_name:
                    {
                        "select": list_of_fields_with_select,
                        "calculations": list_of_fields_with_calculations,
                        "not_for_select": list_of_fields_not_for_select, # used for ctes with where statement
                        "where": list_with_local_where # could be from calculations
                    },
            },
        "dimension": # everything containing by dimension tables
            {
                table_name:
                    {
                        "select": list_of_fields_with_select,
                        "calculations": list_of_fields_with_calculations,
                        "not_for_select": list_of_fields_not_for_select, # used for ctes with where statement
                        "where": list_with_local_where # could be from calculations
                    },
            },

        "overall_where": where_dictionary # where created by frontend
    }
    """

    def __init__(self):

        current_data = {}

        for table_type in [_table_type.value for _table_type in TableTypes]:
            current_data[table_type] = {}

        current_data["overall_where"] = {}

        super().__init__(current_data)

    def add_field(self, field_name: str, all_fields: AllFields, all_tables: AllTables):
        """
        Adds any field to self.data
        :param all_tables:
        :param field_name:
        :param all_fields:
        :return:
        """
        table_name = get_table_from_field(field_name)
        table_type = all_tables.get_table_type(table_name)
        field_type = all_fields.get_backend_field_type(field_name)

        self.add_table_if_not_exist(table_name, table_type)

        if field_type == FieldType.SELECT.value:
            if field_name not in self.data[table_type][table_name][field_type]:
                self.data[table_type][table_name][field_type].append(field_name)

            # If we added this field not for select previously
            if field_name in self.data[table_type][table_name]["not_for_select"]:
                self.data[table_type][table_name][field_type]["not_for_select"].remove(field_name)

        if field_type == FieldType.CALCULATION.value:
            if field_name not in self.data[table_type][table_name][field_type]:
                calculations = all_fields.get_calculation(field_name=field_name)
                where = all_fields.get_calculation_where(field_name=field_name)

                if calculations not in self.data[table_type][table_name][field_type]:
                    self.data[table_type][table_name][field_type].append(calculations)

                if where not in self.data[table_type][table_name]["where"]:
                    self.data[table_type][table_name]["where"].append(where)

                for included_field in all_fields.get_included_fields(field_name=field_name):
                    included_field_table_name: str = get_table_from_field(included_field)
                    included_field_table_type: str = all_tables.get_table_type(included_field_table_name)
                    self.add_table_if_not_exist(included_field_table_name, included_field_table_type)

                    # If field used in calculations where and does not yet exists in select, we append it
                    if (included_field in where) and \
                            (included_field not in
                             self.data[included_field_table_type][included_field_table_name]["not_for_select"]):
                        self.data[included_field_table_type][included_field_table_name]["not_for_select"].append(
                            included_field)

    def add_user_calculations_field(self, field_name: str, calculation: str, all_tables: AllTables) -> None:
        """

        :param all_tables:
        :param field_name:
        :param calculation:
        :return:
        """
        table_name = get_table_from_field(field_name)
        table_type = all_tables.get_table_type(table_name)

        self.add_table_if_not_exist(table_name, table_type)

        if calculation not in self.data[table_type][table_name][FieldType.CALCULATION.value]:
            self.data[table_type][table_name][FieldType.CALCULATION.value].append(calculation)

    def add_overall_where(self, where_from_frontend: dict, all_fields: AllFields, all_tables: AllTables,
                          predefined_where: WhereFields) -> None:
        """
        Adds overall_where
        :param predefined_where:
        :param where_from_frontend:
        :param all_fields:
        :param all_tables:
        :return:
        """
        self.data["overall_where"] = where_from_frontend

        self.__add_tables_from_where(where_from_frontend, all_tables, all_fields, predefined_where)

    def get_fact_tables(self) -> set:
        tables = set()
        for key in self.data[TableTypes.DATA.value]:
            tables.add(key)

        return tables

    def get_dimension_tables(self) -> set:
        tables = set()
        for key in self.data[TableTypes.DIMENSION.value]:
            tables.add(key)

        return tables

    def remove_all_fact_tables_except_named(self, table_name: str):
        fact_tables = self.get_fact_tables()
        fact_tables.remove(table_name)

        for table in fact_tables:
            del self.data[TableTypes.DATA.value][table]

    def get_all_tables(self) -> set:
        all_tables = set()
        all_tables.update(self.get_dimension_tables())
        all_tables.update(self.get_fact_tables())
        return all_tables

    @staticmethod
    def __create_table_structure() -> dict:
        """
        Creates table structure for new table
        :return: table structure for new table
        """
        table_structure: dict = {FieldType.SELECT.value: [], FieldType.CALCULATION.value: [], "not_for_select": [],
                                 "where": []}
        # table_structure["not_for_select"] = set()
        # table_structure["join_tables"] = set()
        # table_structure["fact_must_join_on"] = set()
        # table_structure["no_join_fact"] = set()

        return table_structure

    def add_table_if_not_exist(self, table_name: str, table_type: str) -> None:
        """
        Add table to dictionary
        :param table_name: table name
        :param table_type: should be one of @class TableTypes
        :return:
        """
        possible_table_type_values = [tt.value for tt in TableTypes]
        if table_type not in possible_table_type_values:
            UnknownTableType(table_name, table_type)

        if table_name not in self.data[table_type]:
            self.data[table_type][table_name] = self.__create_table_structure()

    # def add_fields_to_table(self, table_name: str, table_type: str, select: set | None, calculations: set | None,
    #                         where: set | None, join_tables: set | None) -> None:
    #     """
    #     Creates table if it doesn't exist and add fields
    #     :param table_name: table name
    #     :param table_type: should be one of @class TableTypes
    #     :param select: set of select fields. Could be None
    #     :param calculations: set of calculation fields. Could be None
    #     :param where: set of where statements. Could be None
    #     :param join_tables: set of join tables. Could be None
    #     :return:
    #     """
    #     self.add_table_if_not_exist(table_name, table_type)
    #     if select is not None:
    #         self.data[table_type][table_name]["select"].update(select)
    #     if select is not None:
    #         self.data[table_type][table_name]["where"].update(where)
    #     if select is not None:
    #         self.data[table_type][table_name]["calculations"].update(calculations)
    #     if select is not None:
    #         self.data[table_type][table_name]["join_tables"].update(join_tables)

    def add_standalone_where(self, where: set) -> None:
        """
        Adds standalone where to set
        :param where:
        :return:
        """
        self.data[FrontendTypeFields.WHERE.value].update(where)

    def add_standalone_calculation(self, calculations: set) -> None:
        """
        Adds standalone calculations to set
        :param calculations:
        :return:
        """
        self.data[FrontendTypeFields.CALCULATIONS.value].update(calculations)

    def add_data_tables_if_not_exist(self, data_tables: set) -> None:
        """

        :param data_tables: Set of data tables
        :return:
        """
        for data_table in data_tables:
            self.add_table_if_not_exist(data_table, TableTypes.DATA.value)

    def add_dimension_tables_if_not_exist(self, dimension_tables: set) -> None:
        """

        :param dimension_tables: Set of dimension tables
        :return:
        """
        for dimension_table in dimension_tables:
            self.add_table_if_not_exist(dimension_table, TableTypes.DIMENSION.value)

    def __add_tables_from_where(self, where_from_frontend: dict, all_tables: AllTables, all_fields: AllFields,
                                predefined_where: WhereFields) -> None:
        """
        Adds tables from where field to list of tables for later join
        :param where_from_frontend:
        :param all_tables:
        :param all_fields:
        :param predefined_where:
        :return:
        """
        for key in where_from_frontend:
            if (key == "or") or (key == "and"):
                for where_item in where_from_frontend[key]:
                    self.__add_tables_from_where(where_item, all_tables, all_fields, predefined_where)

                return

            if where_from_frontend[key]["operator"] == "predefined":
                for field_name in predefined_where.get_fields(key):
                    table_name: str = get_table_from_field(field_name)
                    table_type: str = all_tables.get_table_type(table_name)
                    self.add_table_if_not_exist(table_name, table_type)

            else:
                table_name: str = get_table_from_field(key)
                table_type: str = all_tables.get_table_type(table_name)
                self.add_table_if_not_exist(table_name, table_type)

    def get_overall_where(self) -> dict:
        return self.data["overall_where"]

    def get_select_fields(self, table_name: str) -> list:
        if table_name in self.data[TableTypes.DATA.value]:
            return self.data[TableTypes.DATA.value][table_name]["select"]

        if table_name in self.data[TableTypes.DIMENSION.value]:
            return self.data[TableTypes.DIMENSION.value][table_name]["select"]

        raise RuntimeError
