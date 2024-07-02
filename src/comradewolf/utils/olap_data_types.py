from collections import UserDict

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes, OlapFollowingCalculations, OlapCalculations
from comradewolf.utils.exceptions import OlapCreationException, OlapTableExists

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
                field_name:
                    {
                        field_type: field_type
                        alias_name: "alias_name",
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

    def add_field(self, field_name: str, alias_name: str, field_type: str, calculation_type: str,
                  following_calculation: str, front_name: str | None = None) -> None:
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
        self.__check_following_calculation(calculation_type, following_calculation)
        self.__check_alias_name(alias_name)
        if calculation_type == "none":
            self.__check_front_name(field_type, front_name)

        self.data["fields"][field_name] = {
            "alias_name": alias_name,
            "field_type": field_type,
            "calculation_type": calculation_type,
            "following_calculation": following_calculation,
            "front_name": front_name
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
    def __check_following_calculation(calculation_type: str, following_calculation: str) -> None:
        """
        Should be one of OlapFollowingCalculations
        :param calculation_type:
        :param following_calculation:
        :return:
        """
        following_calculations: list[str] = [f.value for f in OlapFollowingCalculations]

        following_calculation_for_error: str = ", ".join(following_calculations)

        if calculation_type is None:
            return

        if following_calculation not in following_calculations:
            raise OlapCreationException(f"{following_calculation} is not one of [{following_calculation_for_error}]")

    @staticmethod
    def __check_calculation_type(calculation_type: str) -> None:
        """
        Should be one of OlapCalculations
        :param calculation_type:
        :return:
        """

        olap_calculations: list[str] = [f.value for f in OlapCalculations]
        olap_calculations_for_error: str = ", ".join(olap_calculations)

        if (calculation_type not in olap_calculations) or (calculation_type is None):
            raise OlapCreationException(f"{olap_calculations} is not one of [{olap_calculations_for_error}]")

    def __check_alias_name(self, alias_name: str) -> None:
        """
        Alias name should not exist in a table
        :param alias_name:
        :return:
        """
        for field in self.data["fields"]:
            if alias_name == self.data["fields"][field]["alias_name"]:
                raise OlapCreationException(f"Repeated alias inside OlapDataTable: {alias_name}")

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
                field_name:
                    {
                        "alias_name": alias_name,
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

    def add_field(self, field_name: str, field_type: str, alias_name: str, front_name: str | None = None) -> None:
        """
        Creates new field
        :param field_name: table name of field
        :param field_type: either OlapFieldTypes.DIMENSION.value or OlapFieldTypes.SERVICE_KEY.value
        :param front_name: should be not None if field_type == OlapFieldTypes.DIMENSION.value
        :param alias_name: will be used to join tables
        :return:
        """

        self.__check_dimension_field_types(field_type)

        if field_type == OlapFieldTypes.SERVICE_KEY.value:
            self.__check_alias(alias_name)

        if (field_type == OlapFieldTypes.DIMENSION.value) & (front_name is None):
            raise OlapCreationException(NO_FRONT_NAME_ERROR)

        self.data["fields"][field_name] = {
            "alias_name": alias_name,
            "field_type": field_type,
            "front_name": front_name
        }

    def __check_alias(self, alias_name) -> None:
        """
        Checks if alias does not exist in this object
        :param alias_name:
        :return:
        """
        for field_name in self.data["fields"]:
            if self.data["fields"][field_name]["alias_name"] == alias_name:
                raise OlapCreationException(f"Alias '{alias_name}' already exists")

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


class OlapFrontend(UserDict):
    """
    Dictionary containing fields for frontend
    """

    def add_field(self, table_name: str, field_name: str, field_type: str, alias: str, front_name: str) -> None:
        """
        Add field to show on frontend
        :param table_name:
        :param field_name:
        :param field_type:
        :param alias:
        :param front_name:
        :return:
        """
        self.data[field_name] = {
            "table_name": table_name,
            "field_type": field_type,
            "alias": alias,
            "front_name": front_name,
        }


class OlapFrontendToBackend(UserDict):
    """
    Type converts from Frontend to Backend for Olap to create SELECT
    """
    def __init__(self, frontend: dict) -> None:
        """

        :param frontend:
            {'SELECT': [{'tableName': "database.schema.table", 'fieldName': 'field_name'},
                        {'tableName': "database.schema.table", 'fieldName': 'field_name'},
                       ],
            'CALCULATION': [{'tableName': 'database.schema.table',
                             'fieldName': 'field_name', 'calculation': 'CalculationType'},
                             {'tableName': 'database.schema.table',
                             'fieldName': 'field_name', 'calculation': 'CalculationType'},
                           ],
            'WHERE': [{'tableName': 'database.schema.table', 'fieldName': 'field_name',
                       'where': 'where_type (>, <, =, ...)', 'condition': 'condition_string'},
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
