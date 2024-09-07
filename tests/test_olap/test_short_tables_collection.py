import pytest

from comradewolf.universe.olap_service import OlapService, NO_FACT_TABLES
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.exceptions import OlapException
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import base_table_with_join_no_where_no_calc, base_table_with_join_no_where, \
    group_by_read_no_where, group_by_also_in_agg, one_agg_value, one_dimension, base_table_with_join_no_gb

BASE_TABLE_NAME = "olap_test.games_olap.base_sales"
G_BY_Y_YM_TABLE_NAME = "olap_test.games_olap.g_by_y_ym"
G_BY_Y_TABLE_NAME = "olap_test.games_olap.g_by_y"
G_BY_Y_YM_P_TABLE_NAME = "olap_test.games_olap.g_by_y_ym_p"
G_BY_Y_P_TABLE_NAME = "olap_test.games_olap.g_by_y_p"

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_service: OlapService = OlapService()


def gather_dict_data(fields_with_front_and_back: list) -> list:
    """
    Returns list of fields from select field of table
    :param fields_with_front_and_back:
    :return:
    """

    fields = []

    for table in fields_with_front_and_back:
        fields.append(table["backend_field"])

    return fields


def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(base_table_with_join_no_where_no_calc)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 1
    assert BASE_TABLE_NAME in short_table_only_base
    assert len(short_table_only_base.get_selects(BASE_TABLE_NAME)) == 2
    assert len(short_table_only_base.get_all_selects(BASE_TABLE_NAME)) == 10

    assert len(short_table_only_base.get_self_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_select(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(BASE_TABLE_NAME)) == 0

    fields = gather_dict_data(short_table_only_base.get_selects(BASE_TABLE_NAME))

    assert "year" in fields
    assert "pcs" in fields


def test_should_be_only_base_table_with_group_by():
    # Поля, которые есть в базовой таблице с group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(base_table_with_join_no_where)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 1
    assert BASE_TABLE_NAME in short_table_only_base

    assert len(short_table_only_base.get_selects(BASE_TABLE_NAME)) == 2
    assert len(short_table_only_base.get_all_selects(BASE_TABLE_NAME)) == 9

    assert len(short_table_only_base.get_self_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_select(BASE_TABLE_NAME)) == 1

    join_tables = short_table_only_base.get_join_select(BASE_TABLE_NAME)
    assert len(join_tables) == 1
    assert "olap_test.games_olap.dim_game" in join_tables
    assert "bk_game_id" in gather_dict_data(join_tables["olap_test.games_olap.dim_game"]["fields"])

    assert len(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME)) == 3
    assert len(short_table_only_base.get_join_where(BASE_TABLE_NAME)) == 0


def test_base_table_wth_gb_agg_no_gb():
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(group_by_read_no_where)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 2
    assert BASE_TABLE_NAME in short_table_only_base
    assert G_BY_Y_YM_TABLE_NAME in short_table_only_base

    # Assert G_BY_Y_YM_TABLE_NAME
    assert len(short_table_only_base.get_selects(G_BY_Y_YM_TABLE_NAME)) == 4

    fields_g_by_y_m = gather_dict_data(short_table_only_base.get_selects(G_BY_Y_YM_TABLE_NAME))

    assert len(fields_g_by_y_m) == 4
    assert "year" in fields_g_by_y_m
    assert "yearmonth" in fields_g_by_y_m
    assert "avg_price" in fields_g_by_y_m
    assert "sum_pcs" in fields_g_by_y_m

    assert len(short_table_only_base.get_aggregations_without_join(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_select(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(G_BY_Y_YM_TABLE_NAME)) == 0

    # Assert BASE_TABLE_NAME
    assert len(short_table_only_base.get_selects(BASE_TABLE_NAME)) == 2

    fields_base_table = gather_dict_data(short_table_only_base.get_selects(BASE_TABLE_NAME))
    assert len(fields_base_table) == 2
    assert "year" in fields_base_table
    assert "yearmonth" in fields_base_table

    assert len(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME)) == 2

    aggregation_base_table = gather_dict_data(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME))

    assert len(aggregation_base_table) == 2
    assert "price" in aggregation_base_table
    assert "pcs" in aggregation_base_table

    assert len(short_table_only_base.get_join_select(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(BASE_TABLE_NAME)) == 10


def test_base_agg_wth_agg():
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице с group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(group_by_also_in_agg)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # All  tables are in
    assert len(short_table_only_base) == 5

    # base_sales
    assert len(short_table_only_base.get_selects(BASE_TABLE_NAME)) == 1

    fields_base_table = gather_dict_data(short_table_only_base.get_selects(BASE_TABLE_NAME))
    assert len(fields_base_table) == 1
    assert "year" in fields_base_table

    assert len(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME)) == 2

    aggregation_base_table = gather_dict_data(short_table_only_base.get_aggregations_without_join(BASE_TABLE_NAME))

    assert len(aggregation_base_table) == 2
    assert "sales_rub" in aggregation_base_table
    assert "pcs" in aggregation_base_table

    assert len(short_table_only_base.get_join_select(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(BASE_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(BASE_TABLE_NAME)) == 11

    # g_by_y_ym
    assert len(short_table_only_base.get_selects(G_BY_Y_YM_TABLE_NAME)) == 1

    fields_g_by_y_ym_table = gather_dict_data(short_table_only_base.get_selects(G_BY_Y_YM_TABLE_NAME))
    assert len(fields_g_by_y_ym_table) == 1
    assert "year" in fields_g_by_y_ym_table

    assert len(short_table_only_base.get_aggregations_without_join(G_BY_Y_YM_TABLE_NAME)) == 2

    aggregation_g_by_y_ym_table = gather_dict_data(short_table_only_base.get_aggregations_without_join(
        G_BY_Y_YM_TABLE_NAME))

    assert len(aggregation_g_by_y_ym_table) == 2
    assert "sum_sales_rub" in aggregation_g_by_y_ym_table
    assert "sum_pcs" in aggregation_g_by_y_ym_table

    assert len(short_table_only_base.get_join_select(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(G_BY_Y_YM_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(G_BY_Y_YM_TABLE_NAME)) == 1

    # g_by_y
    assert len(short_table_only_base.get_selects(G_BY_Y_TABLE_NAME)) == 3

    fields_g_by_y_table = gather_dict_data(short_table_only_base.get_selects(G_BY_Y_TABLE_NAME))
    assert len(fields_g_by_y_table) == 3
    assert "year" in fields_g_by_y_table
    assert "sum_sales_rub" in fields_g_by_y_table
    assert "sum_pcs" in fields_g_by_y_table

    assert len(short_table_only_base.get_aggregations_without_join(G_BY_Y_TABLE_NAME)) == 0

    assert len(short_table_only_base.get_join_select(G_BY_Y_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(G_BY_Y_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(G_BY_Y_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(G_BY_Y_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(G_BY_Y_TABLE_NAME)) == 0

    # Assert g_by_y_ym_p
    assert len(short_table_only_base.get_selects(G_BY_Y_YM_P_TABLE_NAME)) == 1

    fields_g_by_y_ym_p_table = gather_dict_data(short_table_only_base.get_selects(G_BY_Y_YM_P_TABLE_NAME))
    assert len(fields_g_by_y_ym_p_table) == 1
    assert "year" in fields_g_by_y_ym_p_table
    assert len(short_table_only_base.get_aggregations_without_join(G_BY_Y_YM_P_TABLE_NAME)) == 2

    aggregation_g_by_y_ym_p_table = gather_dict_data(short_table_only_base.get_aggregations_without_join(
        G_BY_Y_YM_P_TABLE_NAME))

    assert len(aggregation_g_by_y_ym_p_table) == 2
    assert "sum_sales_rub" in aggregation_g_by_y_ym_p_table
    assert "sum_pcs" in aggregation_g_by_y_ym_p_table

    assert len(short_table_only_base.get_join_select(G_BY_Y_YM_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(G_BY_Y_YM_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(G_BY_Y_YM_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(G_BY_Y_YM_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(G_BY_Y_YM_P_TABLE_NAME)) == 2

    # g_by_y_p
    assert len(short_table_only_base.get_selects(G_BY_Y_P_TABLE_NAME)) == 1

    fields_g_by_y_p_table = gather_dict_data(short_table_only_base.get_selects(G_BY_Y_P_TABLE_NAME))
    assert len(fields_g_by_y_p_table) == 1
    assert "year" in fields_g_by_y_p_table
    assert len(short_table_only_base.get_aggregations_without_join(G_BY_Y_P_TABLE_NAME)) == 2

    aggregation_g_by_y_p_table = gather_dict_data(short_table_only_base.get_aggregations_without_join(
        G_BY_Y_P_TABLE_NAME))

    assert len(aggregation_g_by_y_p_table) == 2
    assert "sum_sales_rub" in aggregation_g_by_y_p_table
    assert "sum_pcs" in aggregation_g_by_y_p_table

    assert len(short_table_only_base.get_join_select(G_BY_Y_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_aggregation_joins(G_BY_Y_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_join_where(G_BY_Y_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_self_where(G_BY_Y_P_TABLE_NAME)) == 0
    assert len(short_table_only_base.get_all_selects(G_BY_Y_P_TABLE_NAME)) == 1


def test_one_value_in_aggregate():
    # Только одно поле value в агрегат
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(one_agg_value)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # All  tables are in
    assert len(short_table_only_base) == 5

    for table in short_table_only_base:
        assert len(short_table_only_base.get_aggregations_without_join(table)) == 1
        assert len(short_table_only_base.get_join_select(table)) == 0
        assert len(short_table_only_base.get_self_where(table)) == 0
        assert len(short_table_only_base.get_aggregation_joins(table)) == 0
        assert len(short_table_only_base.get_join_where(table)) == 0
        assert len(short_table_only_base.get_selects(table)) == 0


def test_one_dimension_in_aggregate():
    # Только одно поле dimension в агрегат
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(one_dimension)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 0

def test_one_dimension_no_aggregate():
    # Только одно поле из dimension table
    pass


def test_should_be_only_base_table_no_group_by_join():
    # Поля, которые есть только в базовой таблице без group by c join dimension table
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(base_table_with_join_no_gb)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 1

    assert 1 == 1


def test_should_be_only_base_table_with_group_by_join():
    # Поля, которые есть в базовой таблице с group by c join dimension table
    pass


def test_base_table_wth_gb_agg_no_gb_join():
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице без group by c join dimension table
    pass


def test_base_agg_wth_agg_join():
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице с group by c join dimension table
    pass


def test_agg_table_wth_join_no_agg():
    # Aggregate таблицу с join
    pass


def test_agg_table_wth_join_with_agg():
    # Aggregate таблицу с join c последующей агрегацией
    pass


if __name__ == "__main__":
    test_should_be_only_base_table_no_group_by()
    test_should_be_only_base_table_with_group_by()
    test_base_table_wth_gb_agg_no_gb()
    test_base_agg_wth_agg()
    test_one_value_in_aggregate()
    test_one_dimension_in_aggregate()
    test_should_be_only_base_table_no_group_by_join()
