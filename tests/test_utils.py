import pytest

from comradewolf.utils.enums_and_field_dicts import ImportTypes
from comradewolf.utils.exceptions import RepeatingTableException, UnknownTypeOfImport
from comradewolf.utils.olap_data_types import ShortTablesCollectionForSelect
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


def helper_for_select(short_table_for_select, table_name) -> dict:
    """
    Helper method creates structure from hort_table_for_select.get_selects(table_name)
    {
        "backend_field": {field_name: count},
        "frontend_field": {frontend_field: count},
        "frontend_calculation": {frontend_calculation: count},
    }
    :param short_table_for_select:
    :param table_name:
    :return:
    """
    test_select: dict = {}
    for field in short_table_for_select.get_selects(table_name):
        backend_field = field["backend_field"]
        frontend_field = field["frontend_field"]
        frontend_calculation = field["frontend_calculation"]

        test_select = add_one_to_key(backend_field, "backend_field", test_select)
        test_select = add_one_to_key(frontend_field, "frontend_field", test_select)
        test_select = add_one_to_key(frontend_calculation, "frontend_calculation", test_select)

    return test_select


def add_one_to_key(field_name: str, key_type: str, test_dict: dict) -> dict:
    """
    Adds one to key

    with structure
    test_dict =
    {
        "key_type": {field_name: count, },
    }

    :param field_name:
    :param key_type:
    :param test_dict: empty dictionary or dictionary from this method
    :return: changed test_dict
    """

    if key_type not in test_dict:
        test_dict[key_type] = {}

    if field_name in test_dict[key_type]:
        test_dict[key_type][field_name] = test_dict[key_type][field_name] + 1
    else:
        test_dict[key_type][field_name] = 1

    return test_dict


def helper_for_aggregation(short_table_for_select: ShortTablesCollectionForSelect, table_name: str) -> dict:
    test_aggregation = {}

    for field in short_table_for_select.get_aggregations_without_join(table_name):
        test_aggregation = generate_aggregation_fields(field, test_aggregation)

    return test_aggregation


def generate_aggregation_fields(fields: dict, test_aggregation):
    backend_field = fields["backend_field"]
    backend_calculation = fields["backend_calculation"]
    frontend_calculation = fields["frontend_calculation"]
    frontend_field = fields["frontend_field"]
    test_aggregation = add_one_to_key(backend_field, "backend_field", test_aggregation)
    test_aggregation = add_one_to_key(backend_calculation, "backend_calculation", test_aggregation)
    test_aggregation = add_one_to_key(frontend_calculation, "frontend_calculation", test_aggregation)
    test_aggregation = add_one_to_key(frontend_field, "frontend_field", test_aggregation)
    return test_aggregation


def helper_for_join_aggregation(short_table_for_select, table_name):
    test_join_aggregation = {}

    for table_name in short_table_for_select.get_aggregation_joins(table_name):
        test_join_aggregation[table_name] = generate_aggregation_fields(
            short_table_for_select.get_aggregation_joins(table_name)[table_name], {})

    return test_join_aggregation


def helper_for_join_select(short_table_for_select, table_name) -> dict:
    join_select = {}

    get_join_select_dict = short_table_for_select.get_join_select(table_name)

    for table_name in get_join_select_dict:
        if table_name not in join_select:
            temp_select = {}
        else:
            temp_select = join_select[table_name]

        service_key = get_join_select_dict[table_name]["service_key"]
        join_select[table_name] = add_one_to_key(service_key, "service_key", temp_select)

        for _dict in get_join_select_dict[table_name]["fields"]:

            backend_field = _dict["backend_field"]
            frontend_calculation = _dict["frontend_calculation"]
            frontend_field = _dict["frontend_field"]

            join_select[table_name] = add_one_to_key(backend_field, "backend_field", temp_select)
            join_select[table_name] = add_one_to_key(frontend_calculation, "frontend_calculation", temp_select)
            join_select[table_name] = add_one_to_key(frontend_field, "frontend_field", temp_select)


    return join_select


def helper_for_join_where(short_table_for_select, table_name):

    join_where = {}

    return join_where
