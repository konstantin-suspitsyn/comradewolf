from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import base_table_with_join_no_where_no_calc, base_table_with_join_no_where, \
    group_by_read_no_where, group_by_also_in_agg, one_agg_value, one_dimension, base_table_with_join_no_gb, \
    base_table_with_and_agg_with_join, base_table_with_and_agg, base_table_with_and_agg_without_join, \
    base_table_with_no_join_wht_where, base_table_with_join_wht_where, base_table_with_join_with_where, \
    base_table_with_join_wth_where

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_service: OlapService = OlapService()

BASE_TABLE_NAME = "olap_test.games_olap.base_sales"
G_BY_Y_YM_TABLE_NAME = "olap_test.games_olap.g_by_y_ym"
G_BY_Y_TABLE_NAME = "olap_test.games_olap.g_by_y"
G_BY_Y_YM_P_TABLE_NAME = "olap_test.games_olap.g_by_y_ym_p"
G_BY_Y_P_TABLE_NAME = "olap_test.games_olap.g_by_y_p"
DIM_GAMES = "olap_test.games_olap.dim_game"
DIM_PUBLISHER = "olap_test.games_olap.dim_publisher"


def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(base_table_with_join_no_where_no_calc)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 2
    assert len(select_for_group_by) == 0
    assert len(joins) == 0
    assert len(where) == 0
    assert len(select_for_group_by) == 0
    assert has_calculation is False

    print(olap_service.generate_select_query(select_list, select_for_group_by, joins, where, has_calculation, BASE_TABLE_NAME, 0))

    assert 1 == 1


if __name__ == "__main__":
    test_should_be_only_base_table_no_group_by()
