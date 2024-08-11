import os

import toml

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes, OlapCalculations
from comradewolf.utils.olap_data_types import OlapTablesCollection, OlapDimensionTable, OlapDataTable, OlapFrontend
from comradewolf.utils.utils import list_toml_files_in_directory, return_none_on_text, true_false_converter, \
    return_bool_on_text


class OlapStructureGenerator:
    """
    Generates one structural environment for olap
    """

    # Collection of all tables
    tables_collection: OlapTablesCollection
    # Main table
    main_data_table: OlapDataTable
    # List of fields to show on frontend
    frontend_fields: OlapFrontend

    def __init__(self, path_to_olap_structure: str) -> None:
        """
        Initializes OlapStructureGenerator
        :param path_to_olap_structure: Path to folder with toml structure
        """
        path_for_data_tables = os.path.join(path_to_olap_structure, r"data")
        path_for_dimension_tables = os.path.join(path_to_olap_structure, r"dimension")

        self.tables_collection: OlapTablesCollection = OlapTablesCollection()

        for dimension_file in list_toml_files_in_directory(path_for_dimension_tables):
            self.__import_dimension_olap_table(dimension_file)

        for data_file in list_toml_files_in_directory(path_for_data_tables):
            self.__import_data_olap_table(data_file)

        self.__generate_front_data()

    def __import_dimension_olap_table(self, dimension_file_path: str) -> None:
        """
        Import data from dimension toml file
        Create OlapDimensionTable.class
        Add OlapDimensionTable to OlapTablesCollection
        :param dimension_file_path: link to dimension toml file
        :return: None
        """
        dimension_from_toml: dict = toml.load(dimension_file_path)

        table_name = "{}.{}.{}".format(dimension_from_toml["database"], dimension_from_toml["schema"],
                                       dimension_from_toml["table"])

        dimension_table: OlapDimensionTable = OlapDimensionTable(table_name)

        for field in dimension_from_toml["fields"]:

            use_sk_for_count: bool = False

            if "use_sk_for_count" in dimension_from_toml["fields"][field]:
                use_sk_for_count = return_bool_on_text(dimension_from_toml["fields"][field]["use_sk_for_count"])

            dimension_table.add_field(field, dimension_from_toml["fields"][field]["field_type"],
                                      return_none_on_text(dimension_from_toml["fields"][field]["alias"]),
                                      return_none_on_text(dimension_from_toml["fields"][field]["front_name"]),
                                      use_sk_for_count)

        self.tables_collection.add_dimension_table(dimension_table)

    def __import_data_olap_table(self, data_file_path: str) -> None:
        """
        Import data from data toml file
        Create OlapDataTable.class
        Add OlapDataTable to OlapTablesCollection
        :param data_file_path: path to toml file with dimension table
        :return:
        """
        data_from_toml: dict = toml.load(data_file_path)

        table_name = "{}.{}.{}".format(data_from_toml["database"], data_from_toml["schema"],
                                       data_from_toml["table"])

        data_table: OlapDataTable = OlapDataTable(table_name)

        for field in data_from_toml["fields"]:
            data_table.add_field(field,
                                 return_none_on_text(data_from_toml["fields"][field]["alias"]),
                                 return_none_on_text(data_from_toml["fields"][field]["field_type"]),
                                 data_from_toml["fields"][field]["calculation_type"].lower(),
                                 self.__transform_following_calculation(data_from_toml["fields"][field]
                                                                        ["following_calculation"]),
                                 return_none_on_text(data_from_toml["fields"][field]["front_name"])
                                 )

        if "base_table" in data_from_toml.keys():
            if true_false_converter(data_from_toml["base_table"]) is True:
                self.main_data_table = data_table

        self.tables_collection.add_data_table(data_table)

    def __generate_front_data(self) -> None:
        """
        Generates dictionary for frontend
        :return:
        """

        self.frontend_fields = OlapFrontend()

        table_name: str = self.main_data_table["table_name"]

        for field in self.main_data_table["fields"]:
            if self.main_data_table["fields"][field]["field_type"] != OlapFieldTypes.SERVICE_KEY.value:
                field_type: str = self.main_data_table["fields"][field]["field_type"]
                front_name: str = self.main_data_table["fields"][field]["front_name"]
                self.frontend_fields.add_field(field, field_type, front_name)

        for table in self.tables_collection["dimension_tables"]:
            for field in self.tables_collection["dimension_tables"][table]["fields"]:
                if self.tables_collection["dimension_tables"][table]["fields"][field]["field_type"] == \
                        OlapFieldTypes.DIMENSION.value:
                    field_type = self.tables_collection["dimension_tables"][table]["fields"][field]["field_type"]
                    front_name = self.tables_collection["dimension_tables"][table]["fields"][field]["front_name"]
                    self.frontend_fields.add_field(field, field_type, front_name)

    def get_front_fields(self) -> OlapFrontend:
        """
        Returns the frontend fields
        :return:
        """
        return self.frontend_fields

    def get_dimension_field_aliases(self) -> list[str]:
        """
        Returns all dimension field aliases
        :return:
        """
        all_fields: list[str] = []

        for table in self.tables_collection["dimension_tables"]:
            for field_alias in self.tables_collection["dimension_tables"][table]["fields"]:
                if field_alias not in all_fields:
                    all_fields.append(field_alias)

        return all_fields

    def get_dimension_table_list(self) -> list[str]:
        """Returns a list of dimension table names"""
        return self.tables_collection.get_dimension_table_names()

    def get_tables_with_field_and_optional_calculation(self,
                                                       field_name: str,
                                                       calculation: str | None,
                                                       white_tables: dict | None = None) -> dict:
        """
        Returns tables with specified fields and optional calculations

        If calculated field is not directly in table (for example in dimension table), it will suggest join

        :param field_name: name of field
        :param calculation: optional calculation
        :param white_tables: optional if it's not none, iterate only through those tables
        :return:
        """
        tables_with_fields: list[str] = []

        d = {}

        # if white_tables is None or len(white_tables.keys()) == 0:
        #     tables_with_fields = self.get_data_tables()
        # else:
        #     tables_with_fields = list(white_tables.keys())

        # dimension_table_name: {service_key: service_key, fields: []}

        # if not None, returns dict with {table_name: service_key_name}
        dimension_table_with_alias: dict | None = self.tables_collection.get_dimension_table_with_field(field_name)
        table_name: str = ""
        c = self.tables_collection.get_data_tables_with_select_fields(field_name)
        self.tables_collection.get_tables_with_calculation(field_name, calculation, None)
        # 1. Выбрать таблицы, которые имеют все для select и where (причем считать оставшиеся поля без расчетов только
        # для select)
        # 2. Если есть калькуляции
        # 2.1. Проверить таблицы, где осталось 0 полей без калькуляций, есть ли все поля для расчета
        # 2.2. Проверить таблицы начиная с меньшего кол-ва оставшихся полей, можно ли в дальнейшем делать калькуляцию
        # 2.3. Делать калькуляцию на таблицах без калькуляций

        for table in tables_with_fields:
            pass

        return d

    def get_all_tables(self) -> list[str]:
        """
        Returns all tables
        :return: list of all tables
        """
        all_tables: list[str] = []

        all_tables.extend(self.get_dimension_table_list())
        all_tables.extend(self.get_data_tables())

        return all_tables

    def get_data_tables(self) -> list[str]:
        """
        Returns fact tables
        :return:
        """
        return list(self.tables_collection.get_data_table_names())

    @staticmethod
    def __transform_following_calculation(following_calculation: str) -> str | None:
        """
        Convert following_calculation to correct value
        :param following_calculation:
        :return:
        """

        if following_calculation is None:
            return None

        if following_calculation.lower() == "none":
            return None

        possible_calculations: list[str] = [f.value for f in OlapCalculations]

        if following_calculation.lower() not in possible_calculations:
            raise ValueError(f"Invalid following_calculation: {following_calculation}")

        return following_calculation.lower()


if __name__ == "__main__":
    osg = OlapStructureGenerator(path_to_olap_structure=r"C:\Users\Const\working\comradewolf\tests"
                                                        r"\test_olap_structure\olap_games")
    print(osg.get_front_fields())
