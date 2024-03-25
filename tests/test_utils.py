import pytest

from comradewolf.utils.enums_and_field_dicts import ImportTypes
from comradewolf.utils.exceptions import RepeatingTableException, UnknownTypeOfImport
from comradewolf.utils.utils import list_toml_files_in_directory, true_false_converter, \
    gather_data_from_toml_files_into_big_dictionary, TableTomlImport, JoinsTomlImport
from tests.constants_for_testing import get_empty_folder, get_tables_folder, get_repeated_tables_folder, \
    get_joins_folder, get_standard_filters_folder


def test_list_toml_files_in_directory_should_return_user_warning():
    """
    Should return user warning
    :return:
    """
    with pytest.warns(UserWarning) as record:
        list_toml_files_in_directory(get_empty_folder())

        assert len(record) == 1


def test_list_toml_files_in_directory_should_return_list_of_files_in_directory():
    """
    Test should return list of files in directory
    """
    list_of_files_in_folder: list = list_toml_files_in_directory(get_tables_folder())

    assert len(list_of_files_in_folder) == 6


def test_should_convert_string_true_false_converter():
    """
    Should return True or False
    """
    true_string: str = "True"
    false_string: str = "False"

    assert true_false_converter(true_string)
    assert not true_false_converter(false_string)


def test_gather_data_from_toml_files_into_big_dictionary_should_raise_exception():
    """
    Should raise RepeatingTableException
    :return:
    """
    list_of_files_in_folder: list = list_toml_files_in_directory(get_repeated_tables_folder())
    with pytest.raises(RepeatingTableException) as raised:
        gather_data_from_toml_files_into_big_dictionary(list_of_files_in_folder, ImportTypes.TABLE.value)

    assert "дубликат" in raised.__str__()


def test_gather_data_from_toml_files_into_big_dictionary_should_raise_exception_unknown_type():
    """
    Should raise RepeatingTableException
    :return:
    """
    list_of_files_in_folder: list = list_toml_files_in_directory(get_repeated_tables_folder())
    with pytest.raises(UnknownTypeOfImport) as raised:
        gather_data_from_toml_files_into_big_dictionary(list_of_files_in_folder, "UNKNOWN_TYPE")

    assert "UNKNOWN_TYPE" in raised.__str__()


def test_gather_data_from_toml_files_into_big_dictionary_should_return_tables():
    """
    Should create table structure
    :return:
    """

    table_names: list = ["dim_calendar", "dim_item", "dim_store", "dim_warehouse", "fact_sales", "fact_stock"]

    list_of_files_in_folder: list = list_toml_files_in_directory(get_tables_folder())
    tables = gather_data_from_toml_files_into_big_dictionary(list_of_files_in_folder, ImportTypes.TABLE.value)
    # Created a dict
    assert type(tables) is dict

    for table_name in tables:
        # Every element of special type
        assert type(tables[table_name]) is TableTomlImport
        assert table_name in table_names


def test_gather_data_from_toml_files_into_big_dictionary_should_return_joins():
    """
    Should create join structure
    :return:
    """

    list_of_files_in_folder: list = list_toml_files_in_directory(get_joins_folder())
    joins = gather_data_from_toml_files_into_big_dictionary(list_of_files_in_folder, ImportTypes.JOINS.value)
    # Created a dict
    assert type(joins) is dict

    for table_name in joins:
        # Every element of special type
        assert type(joins[table_name]) is JoinsTomlImport


def test_gather_data_from_toml_files_into_big_dictionary_should_return_filters():
    """
    Should create filter structure
    :return:
    """

    list_of_files_in_folder: list = list_toml_files_in_directory(get_standard_filters_folder())
    where = gather_data_from_toml_files_into_big_dictionary(list_of_files_in_folder, ImportTypes.FILTERS.value)
    # Created a dict
    assert type(where) is dict

    for where_name in where:
        # Every element of special type
        assert type(where[where_name]) is dict
