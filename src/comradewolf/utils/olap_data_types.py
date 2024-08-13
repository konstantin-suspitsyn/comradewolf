from collections import UserDict

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes, OlapFollowingCalculations, OlapCalculations
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
                  following_calculation: str | None, front_name: str | None = None) \
            -> None:
        """
        Adds a field to this object
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

    def add_field(self, field_name: str, field_type: str, alias_name: str, front_name: str | None = None,
                  use_sk_for_count: bool = False) -> None:
        """
        Creates new field
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
                            {"backend_field": select_field_alias,
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
            if table_properties["fields"][field_alias]["field_type"] in [OlapFieldTypes.DIMENSION.value,
                                                                         OlapFieldTypes.SERVICE_KEY.value, ]:
                self.data[table_name]["all_selects"].append(field_alias)

    def add_select_field(self, table_name: str, select_field_alias: str, calculation: str | None = None) -> None:
        """
        Adds select field to table
        :param calculation:
        :param table_name:
        :param select_field_alias:
        :return:
        """

        field_name: str = select_field_alias

        if calculation is not None:
            field_name = create_field_with_calculation(select_field_alias, calculation)

        self.data[table_name]["select"].append({"backend_field": field_name,
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

    def add_aggregation_field(self, table_name: str, field_name_alias: str, calculation: str, frontend_field_name: str,
                              frontend_aggregation: str) -> None:

        field_name_alias_with_calc = create_field_with_calculation(field_name_alias, calculation)

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

    def add_join_field_for_select(self, table_name: str, field_alias_name: str, join_table_name: str,
                                  service_key_for_join: str) -> None:
        """
        Adds join field to table
        :param table_name:
        :param field_alias_name:
        :param join_table_name:
        :param service_key_for_join:
        :return:
        """

        if join_table_name not in self.data[table_name]["join_select"]:
            self.data[table_name]["join_select"][join_table_name] = {"service_key": service_key_for_join,
                                                               "fields": []}

        self.data[table_name]["join_select"][join_table_name]["fields"].append(
            {"backend_field": field_alias_name,
             "frontend_field": field_alias_name,
             "frontend_calculation": None, }
        )

        self.__remove_select_field(table_name, service_key_for_join)

    def add_where_with_join(self, table_name: str, field_alias_name: str, join_table_name: str, sk_join_field: str,
                            condition: dict) -> None:
        """
        Adds join with where field
        :param table_name:
        :param field_alias_name:
        :param join_table_name:
        :param sk_join_field:
        :param condition:
        :return:
        """
        if join_table_name not in self.data[table_name]["join_where"]:
            self.data[table_name]["join_where"][join_table_name] = {"service_key": sk_join_field, "conditions": []}

        self.data[table_name]["join_where"][join_table_name]["conditions"].append({field_alias_name: condition})

    def add_where(self, table_name: str, field_name: str, condition: dict) -> None:
        """
        Adds where field to table
        If where is in table without join
        :param table_name:
        :param field_name:
        :param condition:
        :return:
        """
        if field_name not in self.data[table_name]["self_where"]:
            self.data[table_name]["self_where"][field_name] = []

        self.data[table_name]["self_where"][field_name].append(condition)

    def add_join_field_for_aggregation(self, table_name: str, field_name_alias: str, current_calculation: str,
                                       join_table_name: str, service_key: str) -> None:

        if table_name not in self.data[table_name]["aggregation_joins"]:
            self.data[table_name]["aggregation_joins"][join_table_name] = {
                "service_key": service_key,
                "fields": [],
            }

            self.data[table_name]["aggregation_joins"][join_table_name]["fields"].append({
                "frontend_field": field_name_alias,
                "frontend_calculation": current_calculation,
            })

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
        :return: dictionary with structure {table_name: service_key_name}
        """

        for table_name in self.get_dimension_table_names():

            dimension_table: OlapDimensionTable = self.data["dimension_tables"][table_name]

            has_service_key: bool = False
            has_field: bool = False
            service_key_name: str = ""

            for field in dimension_table.get_fields():
                if field == field_alias_name:
                    has_field = True

                if dimension_table["fields"][field]["field_type"] == OlapFieldTypes.SERVICE_KEY.value:
                    service_key_name = field
                    has_service_key = True

            if has_service_key & has_field:
                dimension_table: list = [table_name, service_key_name]

                return dimension_table

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

    def get_data_table_calculation(self, table_name: str, field_name_alias: str) -> list[str]:
        """
        Returns calculation for field in table
        :param table_name:
        :param field_name_alias:
        :return:
        """

        calculation: list[str] = []

        for field in self.data["data_tables"][table_name]["fields"]:

            field_name, current_calculation = get_calculation_from_field_name(field)

            if (field_name == field_name_alias) & (current_calculation is not None):
                calculation.append(current_calculation)

        return calculation

    def get_data_table_further_calculation(self, table_name: str, field_name_alias: str, calculation: str) -> str | None:
        """
        Returns can it be used for further calculation
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

    def add_field(self, alias: str, field_type: str, front_name: str) -> None:
        """
        Add field to show on frontend
        :param field_type:
        :param alias:
        :param front_name:
        :return:
        """
        self.data[alias] = {
            "field_type": field_type,
            "front_name": front_name,
        }


class OlapFrontendToBackend(UserDict):
    """
    Type converts from Frontend to Backend for Olap to create SELECT
    """

    #TODO: Add classes for Inner Data Structures

    def __init__(self, frontend: dict) -> None:
        """

        :param frontend:
            {'SELECT': ['field_name', 'field_name',],
            'CALCULATION': [{'fieldName': 'field_name', 'calculation': 'CalculationType'},
                             {'fieldName': 'field_name', 'calculation': 'CalculationType'},
                           ],
            'WHERE': [{
                        'fieldName': 'field_name', 'where': 'where_type (>, <, =, ...)', 'condition': 'condition_string'
                        },
                      ]}
        """

        backend: dict = {"SELECT": [], "CALCULATION": [], "WHERE": []}

        if "SELECT" in frontend.keys():
            backend["SELECT"] = frontend["SELECT"]

        if "CALCULATION" in frontend.keys():
            backend["CALCULATION"] = frontend["CALCULATION"]

        if "SELECT" in frontend.keys():
            backend["WHERE"] = frontend["WHERE"]

        super().__init__(frontend)

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
