from comradewolf.utils.data_types import WhereFields, AllFields, AllTables, AllJoins, FactTableJoins
from comradewolf.utils.enums_and_field_dicts import ImportTypes, WhereFieldsProperties
from comradewolf.utils.utils import gather_data_from_toml_files_into_big_dictionary, list_toml_files_in_directory, \
    true_false_converter


class StructureGenerator:
    """
    Generates dictionary-like objects with table-structures and joins
    """

    # Dictionary with structure {"table_name": "table_type"}
    __all_tables_short: AllTables
    __joins_by_table: AllJoins
    # All fields with properties
    __all_fields: AllFields
    # Pre-defined where fields
    __where_predefined: WhereFields
    # What tables have to be used to join them
    __fact_joins: FactTableJoins

    # Table name structure database.scheme.table
    TABLE_NAME_STRUCTURE: str = "{}.{}.{}"
    # Table name structure database.scheme.table.field_name
    FIELD_NAME: str = "{}.{}.{}.{}"

    def __init__(self, tables_folder_link: str, joins_folder_link: str, filters_folder_link: str) -> None:
        """
        Gets data from all toml files
        Checks for duplicates and other errors
        :param tables_folder_link: link to folder with .toml files, containing table references
        :param filters_folder_link: link to folder with .toml files, containing filters references
        :param joins_folder_link: link to folder with .toml files, containing joins references
        """

        self.__all_tables_short = AllTables()
        self.__joins_by_table = AllJoins()
        self.__all_fields = AllFields()
        self.__where_predefined = WhereFields()
        self.__fact_joins = FactTableJoins()

        toml_tables: dict = gather_data_from_toml_files_into_big_dictionary(
            list_toml_files_in_directory(tables_folder_link), ImportTypes.TABLE.value)
        toml_joins_dict: dict = gather_data_from_toml_files_into_big_dictionary(
            list_toml_files_in_directory(joins_folder_link), ImportTypes.JOINS.value)
        toml_filters_dict: dict = gather_data_from_toml_files_into_big_dictionary(
            list_toml_files_in_directory(filters_folder_link), ImportTypes.FILTERS.value)

        self.__generate_short_tables(toml_tables)
        self.__create_all_fields(toml_tables)
        self.__generate_all_joins(toml_joins_dict)
        self.__generate_predefined_where_fields(toml_filters_dict)

    def __generate_short_tables(self, toml_tables: dict) -> None:
        """
        Generates __short_tables_dictionary
        :param toml_tables: dictionary created from toml files containing tables
        :return: None
        """
        for file_name in toml_tables:
            table_type = toml_tables[file_name]["table_type"]
            complete_table_name = self.TABLE_NAME_STRUCTURE.format(
                toml_tables[file_name]["database"],
                toml_tables[file_name]["schema"],
                toml_tables[file_name]["table"])
            self.__all_tables_short.add_table(complete_table_name, table_type)

    def __create_all_fields(self, toml_tables: dict) -> None:
        """
        Creates self.__all_fields
        :param toml_tables: created in self.__init__
        :return:
        """

        for file_name in toml_tables:

            # Working with usual fields
            for field in toml_tables[file_name]["fields"]:

                field_name = self.FIELD_NAME.format(
                    toml_tables[file_name]["database"],
                    toml_tables[file_name]["schema"],
                    toml_tables[file_name]["table"],
                    field)
                field_type: str = toml_tables[file_name]["fields"][field]["type"]

                field_show: bool = true_false_converter(toml_tables[file_name]["fields"][field]["show"])

                front_field_type: str | None = None

                if "front_type" in toml_tables[file_name]["fields"][field]:
                    # Check if front_field type exists and in enum
                    front_field_type = toml_tables[file_name]["fields"][field]["front_type"]

                show_group: str | None = None
                if "show_group" in toml_tables[file_name]["fields"][field]:
                    show_group = toml_tables[file_name]["fields"][field]["show_group"]

                # There is possibility than Human-name does not exist
                field_human_name: str | None = None

                if "name" in toml_tables[file_name]["fields"][field]:
                    field_human_name = toml_tables[file_name]["fields"][field]["name"]

                field_calculation: str | None = None

                # Working with predefined calculations
                if "calculation" in toml_tables[file_name]["fields"][field]:
                    field_calculation = toml_tables[file_name]["fields"][field]["calculation"]

                field_where: str | None = None

                if "where" in toml_tables[file_name]["fields"][field]:
                    field_where = toml_tables[file_name]["fields"][field]["where"]

                included_fields: list[str] = []
                if "included_fields" in toml_tables[file_name]["fields"][field]:
                    included_fields.extend(toml_tables[file_name]["fields"][field]["included_fields"])

                self.__all_fields.add_field(field_name, field_show, field_type, field_human_name, front_field_type,
                                            show_group, field_calculation, included_fields, field_where)

    def __generate_all_joins(self, toml_joins_dict: dict) -> None:
        """
        Creates all joins by every table and fills. Fills in self.__joins_by_table
        :param toml_joins_dict: created in self.__init__
        :return: None
        """
        for file_name in toml_joins_dict:

            if len(toml_joins_dict[file_name]["second_table"].keys()) == 0:
                continue

            table_name = self.TABLE_NAME_STRUCTURE.format(
                toml_joins_dict[file_name]["database"],
                toml_joins_dict[file_name]["schema"],
                toml_joins_dict[file_name]["first_table"],
            )

            if table_name not in self.__joins_by_table:
                self.__joins_by_table[table_name] = {}

            for join_table in toml_joins_dict[file_name]["second_table"]:
                how = toml_joins_dict[file_name]["second_table"][join_table]["how"]
                on = toml_joins_dict[file_name]["second_table"][join_table]["between_tables"]
                first_table_on = toml_joins_dict[file_name]["second_table"][join_table]["first_table_on"]
                second_table_on = toml_joins_dict[file_name]["second_table"][join_table]["second_table_on"]

                self.__joins_by_table.add_join(table_name, join_table, how, on, first_table_on, second_table_on)

            if "fact_table_joins" in toml_joins_dict[file_name]:
                for join_table in toml_joins_dict[file_name]["fact_table_joins"]:
                    between_tables: list = toml_joins_dict[file_name]["fact_table_joins"][join_table]["join_tables"]

                    self.__fact_joins.add_join(table_name, join_table, between_tables)

    def __generate_predefined_where_fields(self, toml_filters_dict) -> None:
        """
        Generates predefined where fields
        :param toml_filters_dict:
        :return:
        """
        for back_filter_name in toml_filters_dict.keys():
            self.__where_predefined.add_where_field(back_filter_name,
                                                    toml_filters_dict[back_filter_name][
                                                        WhereFieldsProperties.FRONTEND_NAME.value],
                                                    toml_filters_dict[back_filter_name][
                                                        WhereFieldsProperties.WHERE_QUERY.value],
                                                    toml_filters_dict[back_filter_name][
                                                        WhereFieldsProperties.FIELDS_LIST.value],
                                                    toml_filters_dict[back_filter_name][
                                                        WhereFieldsProperties.SHOW_GROUP.value])

    def get_tables(self) -> AllTables:
        """
        Returns all tables
        :return: {"table_name": "table_type"}
        """
        return self.__all_tables_short

    def get_fields(self) -> AllFields:
        """
        Returns all fields
        :return:
        """
        return self.__all_fields

    def get_joins(self) -> AllJoins:
        """
        Returns all joins
        :return:
        """
        return self.__joins_by_table

    def get_where(self) -> WhereFields:
        """
        Returns pre-defined where fields
        :return:
        """
        return self.__where_predefined

    def get_fact_join(self) -> FactTableJoins:
        """
        Returns FactTableJoins
        :return:
        """
        return self.__fact_joins
