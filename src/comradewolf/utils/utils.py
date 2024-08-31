import os
import warnings
from collections import UserDict

import toml

from comradewolf.utils.enums_and_field_dicts import ImportTypes, TomlStructure, AllFieldsForImport
from comradewolf.utils.exceptions import RepeatingTableException, UnknownTypeOfImport, NoMandatoryKeyException


class TableImport(UserDict):
    """
    Base class for TOML import
    """

    def __init__(self, dictionary: dict, current_file_path: str, toml_fields: TomlStructure):
        """
        :param dictionary: what came from toml file
        :param current_file_path: path to toml file
        """

        if not isinstance(dictionary, dict):
            raise RuntimeError("Should be dictionary")

        for key in toml_fields.get_all_mandatory_fields():
            if key not in dictionary.keys():
                raise NoMandatoryKeyException(current_file_path, key)

        super().__init__(dictionary)


class TableTomlImport(TableImport):
    """
    Toml that is imported from tables folder for StructureGenerator class
    """

    def __init__(self, dictionary: dict, current_file_path: str):
        """
        :param dictionary: what came from toml file
        :param current_file_path: path to toml file
        """

        fields = AllFieldsForImport()
        toml_fields = TomlStructure(fields.get_table_fields())

        super().__init__(dictionary, current_file_path, toml_fields)


class JoinsTomlImport(TableImport):
    """
    Toml that is imported from tables folder for StructureGenerator class
    """

    def __init__(self, dictionary: dict, current_file_path: str):
        """
        :param dictionary: what came from toml file
        :param current_file_path: path to toml file
        """

        fields = AllFieldsForImport()
        toml_fields = TomlStructure(fields.get_join_fields())

        super().__init__(dictionary, current_file_path, toml_fields)


class FiltersTomlImport(TableImport):
    """
    Toml that is imported from tables folder for StructureGenerator class
    """

    def __init__(self, dictionary: dict, current_file_path: str):
        """
        :param dictionary: what came from toml file
        :param current_file_path: path to toml file
        """

        toml_fields = dictionary

        fields = AllFieldsForImport()
        super().__init__(toml_fields, current_file_path, fields.get_where_dictionary())


def list_toml_files_in_directory(directory: str) -> list:
    """
    Returns list of all toml files in directory
    :param directory: directory with toml files
    :return: list of toml files found
    """
    all_files = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if (os.path.isfile(f)) and (f.split(".")[-1] == "toml"):
            all_files.append(f)

    if len(all_files) == 0:
        warnings.warn(f"Нужные файлы в папке {directory} не найдены")

    return all_files


def true_false_converter(tf: str) -> bool:
    """
    Converter to return true or false from string
    :param tf: true of false string
    :return: True or False
    """
    if tf.lower() == "true":
        return True
    return False


def gather_data_from_toml_files_into_big_dictionary(list_of_files: list,
                                                    check_for_duplicate_key: str) -> dict:
    """
    Gathers data from toml files and check for duplicates and mandatory fields
    :param list_of_files: list with paths to toml files and one of ImportTypes.values
    :param check_for_duplicate_key: name of key to check for duplicates. So we would not have two same tables
    :return: dictionary from all files
    """

    # Check if outer_type exists in type field
    all_types = [f.value for f in ImportTypes]

    if check_for_duplicate_key not in all_types:
        raise UnknownTypeOfImport(check_for_duplicate_key)

    result: dict = {}

    for file in list_of_files:
        temp_toml: dict = toml.load(file)

        non_duplicate_key = temp_toml[check_for_duplicate_key]

        if non_duplicate_key in result:
            raise RepeatingTableException(file, non_duplicate_key, check_for_duplicate_key)

        if check_for_duplicate_key == ImportTypes.TABLE.value:
            result[non_duplicate_key] = TableTomlImport(temp_toml, file)

        if check_for_duplicate_key == ImportTypes.JOINS.value:
            result[non_duplicate_key] = JoinsTomlImport(temp_toml, file)

        if check_for_duplicate_key == ImportTypes.FILTERS.value:
            result[non_duplicate_key] = temp_toml

    return result


def get_table_from_field(field_name) -> str:
    """
    Get table name from field name
    :param field_name:
    :return:
    """
    return field_name[:-len(field_name.split(".")[-1]) - 1]


def get_field_name_only(field_name) -> str:
    """
    Get field name from field name with table
    :param field_name:
    :return:
    """
    return field_name.split(".")[-1]


def get_fields(sql_expression: str) -> set:
    sql_expression = sql_expression.lower()
    remove_from_expression = ["sum", "count", "and", "where", ">", "<", "=", "+", "-", "avg", "(", ")", "*"]
    for item in remove_from_expression:
        sql_expression = sql_expression.replace(item, " ")

    set_of_fields = set(sql_expression.split(" "))
    if "" in set_of_fields:
        set_of_fields.remove("")

    return set_of_fields


def singleton(class_):
    """
    Decorator for singleton class
    :param class_:
    :return:
    """
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


def join_on_to_string(on_dictionary: dict) -> str:
    """
    Joins tables with and statement between fields
    :param on_dictionary:
    :return:
    """
    on = []
    # TODO: make enum
    for i in range(len(on_dictionary["first_table_on"])):
        on.append("{} {} {}".format(on_dictionary["first_table_on"][i], on_dictionary["between_tables"][i],
                                    on_dictionary["second_table_on"][i]))

    return " AND ".join(on)


def return_none_on_text(text: str) -> str | None:
    """
    If "none" is given, returns None
    :param text:
    :return:
    """
    if text.lower() == "none":
        return None
    return text


def return_bool_on_text(text: str) -> bool:
    """
    If "none" is given, returns None
    :param text:
    :return:
    """

    if text.lower() == "true":
        return True

    if text.lower() == "false":
        return False

    raise ValueError("Not a bool like value")


def create_field_with_calculation(field: str, calculation: str) -> str:
    """

    :param field:
    :param calculation:
    :return:
    """
    return f"{field}__{calculation}"


def get_calculation_from_field_name(field_name: str) -> tuple[str, str | None]:
    """

    :param field_name:
    :return: field_name_without_calculation, calculation
    """

    calculation = None
    field_name = field_name

    if "__" in field_name:
        calculation = field_name.split("__")[-1]
        field_name = field_name[:-len(calculation) - 2]

    return field_name, calculation
