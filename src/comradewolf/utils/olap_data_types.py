from collections import UserDict

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes, OlapFollowingCalculations, OlapCalculations
from comradewolf.utils.exceptions import OlapCreationException


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

        if following_calculations not in following_calculations:
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

        if calculation_type not in olap_calculations:
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
            raise OlapCreationException("Front name should be specified only when field_type is dimension")

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

    @staticmethod
    def __check_dimension_field_types(field_type):
        if field_type not in [OlapFieldTypes.SERVICE_KEY.value, OlapFieldTypes.DIMENSION.value]:
            raise OlapCreationException(f"Field type '{field_type}' should be one of ["
                                        f"{OlapFieldTypes.SERVICE_KEY.value}, {OlapFieldTypes.DIMENSION.value}]")


class OlapTablesCollection(UserDict):
    """
    Contains all data about OLAP tables

    Has structure:
        {
            main_olap_table_name:
                {

                    "main_table": OLAPDataTable,
                    "calculated_tables":
                        {
                            name_of_calculated_table: OLAPDataTable
                        }
                    "dimension_tables":
                        {
                            name_of_dimension_table: OLAPDimensionTable
                        }
                }
        }
    """

    def __create_main_table(self, main_table_name: str) -> None:
        """
        Inserts main table if not exists
        :param main_table_name:
        :return:
        """
        if main_table_name not in self.data:
            self.data[main_table_name] = {}

    def add_data_table(self, data_table: OlapDataTable) -> None:
        """
        Inserts data table
        :param data_table: OLAPDataTable
        :return: None
        """

        # Should check if table is main ot not
        pass

    def add_dimension_table(self, dimension_table: OlapDimensionTable) -> None:
        """
        Inserts data table
        :param dimension_table: OLAPDimensionTable
        :return: None
        """

        pass
