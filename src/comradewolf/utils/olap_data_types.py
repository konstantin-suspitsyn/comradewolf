from collections import UserDict

from docutils.nodes import table

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes, OlapFollowingCalculations, OlapCalculations, \
    FilterTypes
from comradewolf.utils.exceptions import OlapCreationException, OlapTableExists, ConditionFieldsError, OlapException
from comradewolf.utils.utils import create_field_with_calculation, get_calculation_from_field_name

ERROR_FOLLOWING_CALC_SPECIFIED_WITHOUT_CALC = "Following calculation specified, but no calculation type specified"

NO_FRONT_NAME_ERROR = r"Front name should be specified only when field_type is dimension"

SERVICE_KEY_EXISTS_ERROR_MESSAGE = r"Service key already exists"


class OlapDataTable(UserDict):
    """
    Table created for OLAP
    Should represent types of fields (which calculations could be performed or were performed)
    Can you continue calculation and how

    {
            table_name: table_name,
            fields: {
                alias_name:
                    {
                        field_type: field_type
                        field_name: "field_name",
                        calculation_type: "calculation_type",
                        following_calculation: string of OlapFollowingCalculations.class,
                        "front_name": front_name,
                    },
                }
        }


    """

    def __init__(self, table_name: str) -> None:
        """
        :param table_name: table name with in style of db.schema.table
        """
        super().__init__({"table_name": table_name, "fields": {}})

    def add_field(self, field_name: str, alias_name: str, field_type: str, calculation_type: str | None,
                  following_calculation: str | None, data_type: str, front_name: str | None = None) \
            -> None:
        """
        Adds a field to this object
        :param data_type:
        :param field_name:
        :param alias_name:
        :param field_type:
        :param calculation_type:
        :param following_calculation:
        :param front_name:
        :return:
        """

        self.__check_field_type(field_type)
        self.__check_calculation_type(calculation_type)
        if calculation_type is not None:
            alias_name = create_field_with_calculation(alias_name, calculation_type)
        if following_calculation is not None:
            self.__check_following_calculation(calculation_type, following_calculation)
        if calculation_type is None:
            self.__check_front_name(field_type, front_name)

        self.data["fields"][alias_name] = {
            "field_name": field_name,
            "field_type": field_type,
            "calculation_type": calculation_type,
            "following_calculation": following_calculation,
            "front_name": front_name,
            "data_type": data_type,
        }

    @staticmethod
    def __check_field_type(field_type) -> None:
        """
        Check field type
        :param field_type: Field type
        :return:
        """
        field_types: list = [f.value for f in OlapFieldTypes]

        field_type_for_error: str = ", ".join(field_types)

        if field_type not in field_types:
            raise OlapCreationException(f"{field_type} is not one of [{field_type_for_error}]")

    @staticmethod
    def __check_following_calculation(calculation_type: str | None, following_calculation: str | None) -> None:
        """
        Should be one of OlapFollowingCalculations
        :param calculation_type:
        :param following_calculation:
        :return:
        """
        following_calculations: list[str] = [f.value for f in OlapFollowingCalculations]

        following_calculation_for_error: str = ", ".join(following_calculations)

        if (calculation_type is None) and (following_calculation is None):
            return

        if (calculation_type is None) and (following_calculation is not None):
            raise OlapCreationException(ERROR_FOLLOWING_CALC_SPECIFIED_WITHOUT_CALC)

        if following_calculation not in following_calculations:
            raise OlapCreationException(f"{following_calculation} is not one of [{following_calculation_for_error}]")

    @staticmethod
    def __check_calculation_type(calculation_type: str | None) -> None:
        """
        Should be one of OlapCalculations
        :param calculation_type:
        :return:
        """

        if calculation_type is None:
            return

        olap_calculations: list[str] = [f.value for f in OlapCalculations]
        olap_calculations_for_error: str = ", ".join(olap_calculations)

        if calculation_type not in olap_calculations:
            raise OlapCreationException(f"{olap_calculations} is not one of [{olap_calculations_for_error}]")

    @staticmethod
    def __check_front_name(field_type: str, front_name: str | None) -> None:
        """
        Checks if front name set for any type of field except SERVICE_KEY
        :param field_type:
        :param front_name:
        :return:
        """
        if (field_type == OlapFieldTypes.SERVICE_KEY.value) & (front_name is not None):
            raise OlapCreationException(f"front_name is set on field_type = SERVICE_KEY")

        if (field_type != OlapFieldTypes.SERVICE_KEY.value) & (front_name is None):
            raise OlapCreationException(f"front_name should be specified on field_type != SERVICE_KEY")

    def get_name(self) -> str:
        """Returns the name of the OlapDataTable"""
        return self.data["table_name"]


class OlapDimensionTable(UserDict):
    """
    Dimensions for OLAPDataTable

    {
            table_name: table_name,
            fields: {
                alias_name:
                    {
                        "field_name": field_name,
                        "field_type": field_type,
                        "front_name": front_name
                    },
                }
        }

    """

    def __init__(self, table_name: str) -> None:
        """
        :param table_name: table name with in style of db.schema.table
        """
        super().__init__({"table_name": table_name, "fields": {}})

    def add_field(self, field_name: str, field_type: str, alias_name: str, data_type: str,
                  front_name: str | None = None, use_sk_for_count: bool = False) -> None:
        """
        Creates new field
        :param data_type: data type. Should be one of values from OlapDataType()
        :param use_sk_for_count: True if you can use service key both for count and count distinct
        :param field_name: table name of field
        :param field_type: either OlapFieldTypes.DIMENSION.value or OlapFieldTypes.SERVICE_KEY.value
        :param front_name: should be not None if field_type == OlapFieldTypes.DIMENSION.value
        :param alias_name: will be used to join tables
        :return:
        """

        self.__check_dimension_field_types(field_type)

        if (field_type == OlapFieldTypes.DIMENSION.value) & (front_name is None):
            raise OlapCreationException(NO_FRONT_NAME_ERROR)

        self.data["fields"][alias_name] = {
            "field_name": field_name,
            "field_type": field_type,
            "front_name": front_name,
            "use_sk_for_count": use_sk_for_count,
            "data_type": data_type,
        }

    def __check_dimension_field_types(self, field_type) -> None:
        """
        Checks if field type is either dimension or service_key
        Ensures that service key is one

        :param field_type:
        :raises OlapCreateException:
        :return:
        """
        if field_type not in [OlapFieldTypes.SERVICE_KEY.value, OlapFieldTypes.DIMENSION.value]:
            raise OlapCreationException(f"Field type '{field_type}' should be one of ["
                                        f"{OlapFieldTypes.SERVICE_KEY.value}, {OlapFieldTypes.DIMENSION.value}]")

        if field_type == OlapFieldTypes.SERVICE_KEY.value:
            for field_name in self.data["fields"]:
                if self.data["fields"][field_name]["field_type"] == OlapFieldTypes.SERVICE_KEY.value:
                    raise OlapCreationException(SERVICE_KEY_EXISTS_ERROR_MESSAGE)

    def get_field_names(self) -> list[str]:
        """
        Returns a list of field names
        :return:
        """
        return list(self.data["fields"].keys())

    def get_name(self) -> str:
        """Return table name"""
        return self.data["table_name"]

    def get_fields(self) -> list:
        """
        Returns a list of fields in table
        :return:
        """
        return self.data["fields"]

    def get_service_key(self) -> str:
        """
        Returns the service key for this table
        :return:
        """
        for field in self.data["fields"]:
            if self.data["fields"][field]["field_type"] == OlapFieldTypes.SERVICE_KEY.value:
                return field

        raise OlapCreationException("Service key is not defined")


class OlapTablesCollection(UserDict):
    """
    Contains all data about OLAP tables

    Has structure:
        {

                    "data_tables":
                        {
                            name_of_calculated_table: OLAPDataTable,
                            ...
                        }
                    "dimension_tables":
                        {
                            name_of_dimension_table: OLAPDimensionTable,
                            ...
                        }
                }
        }
    """

    def __init__(self):
        super().__init__({"data_tables": {}, "dimension_tables": {}})

    def add_data_table(self, data_table: OlapDataTable) -> None:
        """
        Inserts data table
        :param data_table: OLAPDataTable
        :return: None
        """

        if data_table.get_name() in self.data["data_tables"].keys():
            raise OlapTableExists(data_table.get_name(), "data_tables")

        self.data["data_tables"][data_table.get_name()] = data_table

    def add_dimension_table(self, dimension_table: OlapDimensionTable) -> None:
        """
        Inserts data table
        :param dimension_table: OLAPDimensionTable
        :return: None
        """

        if dimension_table.get_name() in self.data["dimension_tables"].keys():
            raise OlapTableExists(dimension_table.get_name(), "dimension_tables")

        self.data["dimension_tables"][dimension_table.get_name()] = dimension_table

    def get_data_table_names(self) -> list[str]:
        """Returns list of data tables"""
        return list(self.data["data_tables"].keys())

    def get_dimension_table_names(self) -> list[str]:
        """Returns list of dimension tables"""

        return list(self.data["dimension_tables"].keys())

    def get_dimension_table_with_field(self, field_alias_name: str) -> list | None:
        """
        Returns all dimension with alias_name

        If you have multiple dimension tables with same alias-field, you have done something wrong

        :param field_alias_name:
        :return: dictionary with structure [table_name, service_key_name]
        """

        for table_name in self.get_dimension_table_names():

            dimension_table: OlapDimensionTable = self.data["dimension_tables"][table_name]

            has_field: bool
            service_key_name: str = dimension_table.get_service_key()

            for field in dimension_table.get_fields():

                has_field = False

                if field == field_alias_name:
                    has_field = True

                if has_field:
                    dimension_table_for_return: list = [table_name, service_key_name]

                    return dimension_table_for_return

        return None

    def is_field_in_data_table(self, field_alias_name: str, table_name: str, calculation: str | None = None) -> bool:
        """
        Checks if field is in data table
        :param calculation:
        :param field_alias_name: alias_name
        :param table_name:
        :return:
        """
        has_field: bool = False

        if calculation is not None:
            field_alias_name = f"{field_alias_name}__{calculation}"

        if field_alias_name in self.data["data_tables"][table_name]["fields"]:

            if calculation == self.data["data_tables"][table_name]["fields"][field_alias_name]["calculation_type"]:
                has_field = True

        return has_field

    def get_dimension_table_service_key(self, table_name: str) -> str:
        """
        Returns service key for dimension table
        :param table_name:
        :return: service_key
        """
        if table_name not in self.data["dimension_tables"]:
            raise OlapException(f"Table {table_name} not in dimension tables")

        for field in self.data["dimension_tables"][table_name]["fields"]:
            if self.data["dimension_tables"][table_name]["fields"][field]["field_type"] == OlapFieldTypes.SERVICE_KEY:
                return field

        raise OlapException(f"No service key in table {table_name}")

    def get_is_sk_for_count(self, table_name: str, field_name_alias: str) -> bool:
        """
        Returns if you can use service key or not
        :param table_name:
        :param field_name_alias:
        :return:
        """

        return self.data["dimension_tables"][table_name]["fields"][field_name_alias]["use_sk_for_count"]

    def get_data_table_calculation(self, table_name: str, field_name_alias: str) -> str | None:
        """
        Returns calculation for field in table
        :param table_name:
        :param field_name_alias:
        :return:
        """

        calculation: str | None = None

        for field in self.data["data_tables"][table_name]["fields"]:

            field_name, current_calculation = get_calculation_from_field_name(field)

            if (field_name == field_name_alias) & (current_calculation is not None):
                calculation = current_calculation

        return calculation

    def get_data_table_further_calculation(self, table_name: str, field_name_alias: str, calculation: str) \
            -> str | None:
        """
        Returns can it be used for further calculation
        :param calculation:
        :param table_name:
        :param field_name_alias:
        :return:
        """

        field_name_alias = create_field_with_calculation(field_name_alias, calculation)

        return self.data["data_tables"][table_name]["fields"][field_name_alias]["following_calculation"]

    def get_fact_tables_collection(self) -> dict:
        """
        Returns tables collection of fact tables
        :return:
        """
        return self.data["data_tables"]

    def get_backend_field_name(self, table_name, alias_backend_name) -> str | None:
        """
        Gets backend field name from table
        :param table_name:
        :param alias_backend_name:
        :return:
        """

        if table_name in self.get_fact_tables_collection().keys():
            if alias_backend_name in self.data["data_tables"][table_name]["fields"]:
                return self.data["data_tables"][table_name]["fields"][alias_backend_name]["field_name"]

        if table_name in self.get_dimension_table_names():
            if alias_backend_name in self.data["dimension_tables"][table_name]["fields"]:
                return self.data["dimension_tables"][table_name]["fields"][alias_backend_name]["field_name"]

        return None

    def get_frontend_fields(self, table_name: str) -> list:

        frontend_fields: list = []

        if table_name in self.get_fact_tables_collection().keys():
            for field in self.get_fact_tables_collection()[table_name]["fields"]:
                frontend_fields.append(self.data["data_tables"][table_name]["fields"][field]["field_name"])

            return frontend_fields

        if table_name in self.get_dimension_table_names():
            for field in self.data["dimension_tables"][table_name]["fields"]:
                frontend_fields.append(self.data["dimension_tables"][table_name]["fields"][field]["field_name"])

            return frontend_fields

        raise OlapException(f"Table {table_name} not found")

    def get_fact_table_fields(self, table_name: str) -> dict:
        """
        Returns fields from fact table
        :param table_name:
        :return:
        """
        return self.data["data_tables"][table_name]["fields"]

    def get_data_tables_with_field(self, alias_field_name) -> None | list[str]:
        """
        Get all fact table
        :param alias_field_name:
        :return:
        """
        table_names: list[str] = []

        for table_name in self.get_data_table_names():
            if alias_field_name in self.data["data_tables"][table_name]["fields"]:
                table_names.append(table_name)

        if len(table_names) == 0:
            return None

        return table_names

    def get_number_of_fields(self, table_name: str) -> int:
        if table_name in self.get_data_table_names():
            return len(self.data["data_tables"][table_name]["fields"])

        if table_name in self.get_dimension_table_names():
            return len(self.data["dimension_tables"][table_name]["fields"])

        raise OlapException(f"No table {table_name}")


class OlapFrontend(UserDict):
    """
    Dictionary containing fields for frontend

    Structure:
    {
        "alias_name":
            {
                "field_type": field_type,
                "front_name": front_name,
            },
        "alias_name":
            {
                "field_type": field_type,
                "front_name": front_name,
            },
    }

    """

    def add_field(self, alias: str, field_type: str, front_name: str, data_type: str) -> None:
        """
        Add field to show on frontend
        :param data_type: data type of field
        :param field_type:
        :param alias:
        :param front_name:
        :return:
        """
        self.data[alias] = {
            "field_type": field_type,
            "front_name": front_name,
            "data_type": data_type,
        }

    def get_data_type(self, alias: str) -> str:
        """
        Get field_type of alias
        :param alias: field alias
        :return: string of field-type
        """
        return self.data[alias]["data_type"]


class OlapFrontendToBackend(UserDict):
    """
    Backend structure created from user frontend query input

    Generates structure
    {'SELECT': ['field_name', 'field_name',],
    'CALCULATION': [{'fieldName': 'field_name', 'calculation': 'CalculationType'},
                     {'fieldName': 'field_name', 'calculation': 'CalculationType'},
                   ],
    'WHERE': [{
                'fieldName': 'field_name', 'where': 'where_type (>, <, =, ...)', 'condition': 'condition_string'
                },
              ]}

    """

    #TODO: Add classes for Inner Data Structures

    def __init__(self) -> None:

        backend: dict = {"SELECT": [], "CALCULATION": [], "WHERE": []}

        super().__init__(backend)

    def add_select(self, select: dict) -> None:
        """
        Adds select fields
        :param select: dictionary with prepared select structure
        :return: None
        """
        for item in select:
            self.data["SELECT"].append(item)

    def add_calculation(self, calculation: dict):
        """
        Adds select fields
        :param calculation: dictionary with prepared calculation structure
        :return: None
        """
        for item in calculation:
            self.data["CALCULATION"].append(item)

    def add_where(self, where: list):
        """
        Adds select fields
        :param where: dictionary with prepared where structure
        :return: None
        """
        for item in where:
            self.data["WHERE"].append(item)

    def get_select(self) -> list:
        """
        Returns list of select fields
        :return:
        """
        return self.data["SELECT"]

    def get_calculation(self) -> list:
        """
        Returns list of calculated fields
        :return:
        """
        return self.data["CALCULATION"]

    def get_where(self) -> list:
        """
        Returns list of where fields
        :return:
        """
        return self.data["WHERE"]


class ShortTablesCollectionForSelect(UserDict):
    """
    HelperType for short collection of tables that contain all fields that you need

    # TODO: Define final structure

    """

    def __init__(self) -> None:
        short_tables_collection_for_select: dict = {}
        super().__init__(short_tables_collection_for_select)

    def create_basic_structure(self, table_name: str, table_properties: dict) -> None:
        """
        Creates basic structure for table

            self.data[table_name] = {
                "select": [
                            {"backend_field": backend_name,
                            "backend_alias": backend_alias_name,
                            "frontend_field": select_field_alias,
                            "frontend_calculation": calculation }
                          ],
                "aggregation": [], # aggregations that should be made with existing fields
                "join_select": {"joined_table_name": {
                    "service_key": "field to join table",
                    "fields": [fields of joined table]}},
                "join_where": {},
                "self_where": {},
                "all_selects": [],
            }

        :param table_properties:
        :param table_name:
        :return: None
        """
        self.data[table_name] = {
            "select": [],
            "aggregation": [],
            "join_select": {},
            "aggregation_joins": {},
            "join_where": {},
            "self_where": {},
            "all_selects": [],

        }

        for field_alias in table_properties["fields"]:
            if table_properties["fields"][field_alias]["calculation_type"] is None:
                self.data[table_name]["all_selects"].append(field_alias)

    def add_select_field(self, table_name: str, select_field_alias: str, backend_name: str,
                         calculation: str | None = None, ) -> None:
        """
        Adds select field to table

        If it's data table without calculation, then it will add select
        If data table has calculation on select field, it will use correct alias "{calculation}__{field_alias}"

        :param backend_name:
        :param calculation:
        :param table_name:
        :param select_field_alias:
        :return:
        """

        self.data[table_name]["select"].append({"backend_field": backend_name,
                                                "backend_alias": select_field_alias,
                                                "frontend_field": select_field_alias,
                                                "frontend_calculation": calculation, })

        self.__remove_select_field(table_name, select_field_alias)

    def __remove_select_field(self, table_name: str, select_field_alias: str) -> None:
        """

        :param table_name:
        :param select_field_alias:
        :return:
        """
        if select_field_alias in self.data[table_name]["all_selects"]:
            self.data[table_name]["all_selects"].remove(select_field_alias)

    def add_aggregation_field(self, table_name: str, calculation: str, frontend_field_name: str,
                              frontend_aggregation: str, field_name_alias_with_calc: str) -> None:

        self.data[table_name]["aggregation"].append({"backend_field": field_name_alias_with_calc,
                                                     "frontend_field": frontend_field_name,
                                                     "backend_calculation": calculation,
                                                     "frontend_calculation": frontend_aggregation, })

    def remove_table(self, select_table_name) -> None:
        """
        Removes table from collection
        :param select_table_name:
        :return:
        """
        del self.data[select_table_name]

    def add_join_field_for_select(self, table_name: str, field_alias_name: str, backend_field: str,
                                  join_table_name: str, service_key_alias: str, service_key_dimension_table: str,
                                  service_key_fact_table: str) -> None:
        """
        Adds join field to table
        :param service_key_fact_table:
        :param service_key_dimension_table:
        :param backend_field:
        :param table_name:
        :param field_alias_name:
        :param join_table_name:
        :param service_key_alias:
        :return:
        """

        if join_table_name not in self.data[table_name]["join_select"]:
            self.data[table_name]["join_select"][join_table_name] = \
                {"service_key_fact_table": service_key_fact_table,
                 "service_key_dimension_table": service_key_dimension_table,
                 "fields": []}

        self.data[table_name]["join_select"][join_table_name]["fields"].append(
            {"backend_field": backend_field,
             "backend_alias": field_alias_name,
             "frontend_field": field_alias_name,
             "frontend_calculation": None, }
        )

        self.__remove_select_field(table_name, service_key_alias)

    def add_where_with_join(self, table_name: str, backend_field: str, join_table_name: str,
                            condition: dict, service_key_dimension_table: str, service_key_fact_table: str) -> None:
        """
        Adds join with where field
        :param service_key_fact_table:
        :param service_key_dimension_table:
        :param table_name: fact table name
        :param backend_field:
        :param join_table_name:
        :param condition:
        :return:
        """

        if join_table_name not in self.data[table_name]["join_where"]:
            self.data[table_name]["join_where"][join_table_name] = \
                {"service_key_fact_table": service_key_fact_table,
                 "service_key_dimension_table": service_key_dimension_table,
                 "conditions": []}

        self.data[table_name]["join_where"][join_table_name]["conditions"].append({backend_field: condition})

    def add_where(self, table_name: str, backend_field_name: str, condition: dict) -> None:
        """
        Adds where field to table
        If where is in table without join
        :param backend_field_name:
        :param table_name:
        :param condition:
        :return:
        """
        if backend_field_name not in self.data[table_name]["self_where"]:
            self.data[table_name]["self_where"][backend_field_name] = []

        self.data[table_name]["self_where"][backend_field_name].append(condition)

    def add_join_field_for_aggregation(self, table_name: str, field_name_alias: str, current_calculation: str,
                                       join_table_name: str, service_key_alias: str, service_key_dimension_table: str,
                                       service_key_fact_table: str, backend_field: str) -> None:

        if table_name not in self.data[table_name]["aggregation_joins"]:
            self.data[table_name]["aggregation_joins"][join_table_name] = {
                "service_key_fact_table": service_key_fact_table,
                "service_key_dimension_table": service_key_dimension_table,
                "fields": [],
            }

            self.data[table_name]["aggregation_joins"][join_table_name]["fields"].append({
                "frontend_field": field_name_alias,
                "frontend_calculation": current_calculation,
                "backend_field": backend_field,
                "backend_alias": field_name_alias,
                "backend_calculation": current_calculation,
            })

            self.__remove_select_field(table_name, service_key_alias)

    def get_all_selects(self, table_name) -> list:
        """

        :param table_name:
        :return:
        """
        return self.data[table_name]["all_selects"]

    def generate_complete_structure(self, fact_tables: dict) -> None:
        """
        Generates base for all tables for select
        :param fact_tables: 
        :return: 
        """
        for fact_table_name in fact_tables:
            self.create_basic_structure(fact_table_name, fact_tables[fact_table_name])

    def get_aggregations_without_join(self, table_name: str):
        """
        Get aggregations without join
        :param table_name: 
        :return: 
        """
        return self.data[table_name]["aggregation"]

    def get_join_select(self, table_name: str):
        """
        Get join select field
        :param table_name:
        :return:
        """
        return self.data[table_name]["join_select"]

    def get_selects(self, table_name: str):
        """

        :param table_name:
        :return:
        """
        return self.data[table_name]["select"]

    def get_aggregation_joins(self, table_name: str):
        """
        Get aggregation join fields
        :param table_name:
        :return:
        """
        return self.data[table_name]["aggregation_joins"]

    def get_join_where(self, table_name: str):
        """
        Get join where fields
        :param table_name:
        :return:
        """
        return self.data[table_name]["join_where"]

    def get_self_where(self, table_name: str):
        """
        Get where fields without join
        :param table_name:
        :return:
        """
        return self.data[table_name]["self_where"]


class TableForFilter(UserDict):
    """
    Structure to create select for filters
    """
    def __init__(self, table_name: str, field_alias_name: str, is_distinct: bool, number_of_select_fields: int):
        """

        :param table_name:
        :param field_alias_name:
        :param is_distinct:
        :param number_of_select_fields:
        """
        structure = {
            "table_name": table_name,
            "field_alias": field_alias_name,
            "is_distinct": is_distinct,
            "fields_number": number_of_select_fields,
        }
        super().__init__(structure)

    def get_table_name(self):
        return self.data["table_name"]

    def get_number_of_fields(self):
        return self.data["fields_number"]



class OlapFilterFrontend(UserDict):
    """
    Frontend to backend converter for filter-helpers
    """
    def __init__(self, frontend_data: dict):
        if "SELECT_DISTINCT" not in frontend_data:
            raise OlapException("No SELECT DISTINCT field in frontend")

        field_name = frontend_data["SELECT_DISTINCT"]["field_name"]

        if "type" not in frontend_data["SELECT_DISTINCT"]:
            raise OlapException("No valid FilterTypes in frontend")

        select_type = frontend_data["SELECT_DISTINCT"]["type"]

        all_select_types = [e.value for e in FilterTypes]

        if select_type not in all_select_types:
            raise OlapException("No valid FilterTypes in frontend")
        
        super().__init__({"field_alias": field_name, "select_type": select_type})

    def get_select_type(self):
        return self.data["select_type"]

    def get_field_alias_name(self):
        return self.data["field_alias"]


class SelectFilter(UserDict):
    """
    Class for select filter sql
    Format:
    }
        "table_name": {
            "sql": SQL_STRING,
            "fields_no": number of fields in table
        }
    }
    """

    def add_table(self, table_name: str, sql: str, field_no: int) -> None:
        """
        Adds table to structure
        :param table_name:
        :param sql:
        :param field_no:
        :return:
        """
        self.data[table_name] = {
            "sql": sql,
            "all_fields": field_no,
        }

    def get_sql(self, table_name: str) -> str:
        return self.data[table_name]["sql"]

    def get_not_selected_fields(self, table_name: str) -> int:
        return self.data[table_name]["all_fields"]



class SelectCollection(UserDict):
    """
    Class for selects

    Structure:
    {
        table: {
            "sql": sql_query
            "not_selected_fields_no": int_not_selected_fields,
            "has_group_by": bool,
        }
    }

    """

    def add_table(self, table_name: str, sql_query: str, not_selected_fields_no: int, has_group_by: bool) -> None:
        self.data[table_name] = {
            "sql": sql_query,
            "not_selected_fields_no": not_selected_fields_no,
            "has_group_by": has_group_by,
        }

    def get_sql(self, table_name) -> str:
        return self.data[table_name]["sql"]

    def get_not_selected_fields_no(self, table_name) -> int:
        return self.data[table_name]["not_selected_fields_no"]

    def get_has_group_by(self, table_name) -> bool:
        return self.data[table_name]["has_group_by"]
