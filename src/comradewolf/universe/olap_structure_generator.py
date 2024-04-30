import os

import toml

from comradewolf.utils.enums_and_field_dicts import OlapFieldTypes
from comradewolf.utils.olap_data_types import OlapTablesCollection, OlapDimensionTable, OlapDataTable, OlapFrontend
from comradewolf.utils.utils import list_toml_files_in_directory, return_none_on_text, true_false_converter


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
        :return:
        """
        dimension_from_toml: dict = toml.load(dimension_file_path)

        table_name = "{}.{}.{}".format(dimension_from_toml["database"], dimension_from_toml["schema"],
                                       dimension_from_toml["table"])

        dimension_table: OlapDimensionTable = OlapDimensionTable(table_name)

        for field in dimension_from_toml["fields"]:
            dimension_table.add_field(field, dimension_from_toml["fields"][field]["field_type"],
                                      return_none_on_text(dimension_from_toml["fields"][field]["alias"]),
                                      return_none_on_text(dimension_from_toml["fields"][field]["front_name"]),)

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
                                 data_from_toml["fields"][field]["following_calculation"].lower(),
                                 return_none_on_text(data_from_toml["fields"][field]["front_name"]),)

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
                alias: str = self.main_data_table["fields"][field]["alias_name"]
                front_name: str = self.main_data_table["fields"][field]["front_name"]
                self.frontend_fields.add_field(table_name, field, field_type, alias, front_name)

        for table in self.tables_collection["dimension_tables"]:
            table_name = self.tables_collection["dimension_tables"][table]["table_name"]
            for field in self.tables_collection["dimension_tables"][table]["fields"]:
                if self.tables_collection["dimension_tables"][table]["fields"][field]["field_type"] == \
                        OlapFieldTypes.DIMENSION.value:
                    field_type = self.tables_collection["dimension_tables"][table]["fields"][field]["field_type"]
                    alias = self.tables_collection["dimension_tables"][table]["fields"][field]["alias_name"]
                    front_name = self.tables_collection["dimension_tables"][table]["fields"][field]["front_name"]
                    self.frontend_fields.add_field(table_name, field, field_type, alias, front_name)

    def get_front_fields(self) -> OlapFrontend:
        """
        Returns the frontend fields
        :return:
        """
        return self.frontend_fields


if __name__ == "__main__":
    osg = OlapStructureGenerator(path_to_olap_structure=r"C:\Users\Const\working\comradewolf\tests"
                                                        r"\test_olap_structure\olap_games")
    print(osg.get_front_fields())
