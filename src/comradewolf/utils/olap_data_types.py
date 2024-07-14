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
                  following_calculation: str, front_name: str | None = None, alias_for_count: list[str] | None = None) \
            -> None:
        """
        Adds a field to this object
        :param alias_for_count: field that could be used for count
                                and count distinct for field that uses original alias
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
            "front_name": front_name,
            "alias_for_count": alias_for_count,
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

    def get_fields(self) -> list:
        """
        Returns a list of fields in table
        :return:
        """
        return self.data["fields"]


class ShortTablesCollectionForSelect(UserDict):
    """
    HelperType for short collection of tables that contain all fields that you need

    # TODO: Define final strcture

    """

    def __init__(self) -> None:
        short_tables_collection_for_select: dict = {}
        super().__init__(short_tables_collection_for_select)

    def add_select_field(self, table_name: str, select_field: str) -> None:
        """
        Adds select field to table
        :param table_name:
        :param select_field:
        :return:
        """

        if table_name not in self.data.keys():
            self.create_basic_structure(table_name)

        self.data[table_name]["select"].append(select_field)

        self.__remove_select_field(table_name, select_field)

    def __remove_select_field(self, table_name: str, select_field: str) -> None:
        """

        :param table_name:
        :param select_field:
        :return:
        """
        if select_field in self.data[table_name]["select"]:
            self.data[table_name]["all_selects"].remove(select_field)

    def create_basic_structure(self, table_name) -> None:
        """
        Creates basic structure for table
        :param table_name:
        :return:
        """
        self.data[table_name] = {
            "select": [],
            "joins": {},
            "join_where": {},
            "self_where": {},
            "all_selects": [],

        }

        for field in self.data[table_name]["fields"]:
            if self.data[table_name]["fields"][field]["field_type"] in [OlapFieldTypes.DIMENSION.value,
                                                                        OlapFieldTypes.SERVICE_KEY.value,]:
                self.data[table_name]["all_selects"].append(field)

    def remove_table(self, select_table_name) -> None:
        """
        Removes table from collection
        :param select_table_name:
        :return:
        """
        del self.data[select_table_name]

    def add_join_field(self, table_name: str, field_name: str, join_table_name: str, service_key_for_join: str) \
            -> None:
        """
        Adds join field to table
        :param table_name:
        :param field_name:
        :param join_table_name:
        :param service_key_for_join:
        :return:
        """

        if join_table_name not in self.data[table_name]["joins"]:
            self.data[table_name]["joins"][join_table_name] = {"service_key": service_key_for_join,
                                                               "fields": []}

        self.data[table_name]["joins"][join_table_name]["fields"].append(field_name)

        self.__remove_select_field(table_name, service_key_for_join)

    def add_where_with_join(self, table_name: str, field_name: str, join_table_name: str, sk_join_field: str,
                            condition: dict) -> None:
        """
        Adds join with where field
        :param table_name:
        :param field_name:
        :param join_table_name:
        :param sk_join_field:
        :param condition:
        :return:
        """
        if join_table_name not in self.data[table_name]["join_where"]:
            self.data[table_name]["join_where"][join_table_name] = {"service_key": sk_join_field, "conditions": []}

        self.data[table_name]["join_where"][join_table_name]["conditions"].append({field_name: condition})

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

    def get_dimension_table_with_field(self, field_name) -> list | None:
        """
        Returns all dimension with alias_name ==field name

        If you have multiple dimension tables with same alias-field, you have done something wrong

        :param field_name:
        :return: dictionary with structure {table_name: service_key_name}
        """

        for table_name in self.get_dimension_table_names():

            dimension_table: OlapDimensionTable = self.data["dimension_tables"][table_name]

            has_service_key: bool = False
            has_field: bool = False
            service_key_name: str = ""

            for field in dimension_table.get_fields():
                if dimension_table["fields"][field]["alias_name"] == field_name:
                    has_field = True

                if dimension_table["fields"][field]["field_type"] == OlapFieldTypes.SERVICE_KEY.value:
                    service_key_name = dimension_table["fields"][field]["alias_name"]
                    has_service_key = True

            if has_service_key & has_field:
                dimension_table: list = [table_name, service_key_name]

                return dimension_table

        return None

    def get_data_tables_with_select_fields(self, field_name: str, is_where: bool = False, condition: dict | None = None,
                                           shorter_table_collection: ShortTablesCollectionForSelect | None = None) \
            -> ShortTablesCollectionForSelect:
        """
        Shows field that have select field

        If we have provided shorter_table_collection, it will use collection and iterate over it
        If select field was not found, it will remove field

        :param condition: where condition. On;y needed when is_where == True
        :param is_where: if True than it's not select. It's where
        :param field_name:
        :param shorter_table_collection:
        :return: updated ShortTablesCollectionForSelect
        """

        if is_where and (condition is None or len(condition.keys()) == 0):
            # TODO raise an error
            pass

        dimension_table: dict | None
        list_of_fact_tables: list[str]
        table_collection: ShortTablesCollectionForSelect

        if shorter_table_collection is None:
            table_collection = self.__create_short_table_collection()

        else:
            table_collection = shorter_table_collection.copy()

        dimension_table_and_service_key = self.get_dimension_table_with_field(field_name)
        list_of_fact_tables = list(table_collection.keys())

        for select_table_name in list_of_fact_tables:

            is_field_in_table: bool
            is_service_key_in_table: bool = False

            is_field_in_table = self.field_in_data_table(field_name, select_table_name)

            if dimension_table_and_service_key is not None and is_field_in_table:
                is_service_key_in_table = self.field_in_data_table(dimension_table_and_service_key[1],
                                                                   select_table_name)

            if is_service_key_in_table and not is_field_in_table:
                # Field is not in fact table, but you can join dimension table
                if is_where:
                    table_collection.add_where_with_join(select_table_name, field_name,
                                                         dimension_table_and_service_key[0],
                                                         dimension_table_and_service_key[1], condition)
                else:
                    table_collection.add_join_field(select_table_name, field_name, dimension_table_and_service_key[0],
                                                    dimension_table_and_service_key[1])

            if is_field_in_table:
                # Field is not in fact table
                if is_where:
                    table_collection.add_where(select_table_name, field_name, condition)
                else:
                    table_collection.add_select_field(select_table_name, field_name)

            if not is_field_in_table and not is_service_key_in_table:
                # If no field and no possible joins, we remove table from ShortTablesCollectionForSelect
                table_collection.remove_table(select_table_name)

        return table_collection

    def __create_short_table_collection(self):
        table_collection = ShortTablesCollectionForSelect()
        for table_name_temp in self.get_data_table_names():
            table_collection.create_basic_structure(table_name_temp)
        return table_collection

    def field_in_data_table(self, field_name, table_name) -> bool:
        """
        Checks if field is in data table
        :param field_name: alias_name
        :param table_name:
        :return:
        """
        has_field: bool = False

        if field_name in self.data["data_tables"][table_name]["fields"]:
            has_field = True

        return has_field

    def get_tables_with_calculation(self, field_name: str, calculation: str,
                                    shorter_table_collection: ShortTablesCollectionForSelect | None = None):
        """
        Checks if field can be calculated in table

        There are 3 types of possible outcomes:
            1. Field without calculation found - we can use it with the lowest priority
            2. Field with calculation found, but not all select fields were used. Need check if calculation can be used
                over calculation. If yes - we can use it with the priority higher than p. 1
            3. Field with calculation found, all select fields were used - highest priority
        :param field_name:
        :param calculation:
        :param shorter_table_collection:
        :return:
        """
        if shorter_table_collection is None:
            table_collection = self.__create_short_table_collection()
        else:
            table_collection = shorter_table_collection.copy()

        is_select_field: bool = False

        for table in table_collection.keys():
            # TODO: Проверить есть ли поле, нужна ли калькуляция.
            #  Если поле уже с калькуляцией, проверить, что можно делать вторично.
            #  Если поле в dim_словаре, посмотреть можно ли использовать sk

            # Field without calculation found
            pass
            # Field with calculation found, but not all select fields were used
            pass
            # Field with calculation found, all select fields were used
            pass

        if is_select_field:
            return

        # TODO do same thing with join fields.
        #  If you can use sk, try using it
        #  If you can use sk, try using it

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
