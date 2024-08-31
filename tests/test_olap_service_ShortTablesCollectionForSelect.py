# Tests ShortTablesCollectionForSelect
# This is first step from Frontend to Select

from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from comradewolf.utils.utils_for_testing import short_select_collection_games_test
from tests.constants_for_testing import get_olap_games_folder
from tests.test_utils import helper_for_select, helper_for_aggregation, helper_for_join_aggregation, \
    helper_for_join_select, helper_for_join_where

# This should only use base_sales table
base_table_with_join: dict = {'SELECT': [{'field_name': 'year'},
                                         {'field_name': 'pcs'},
                                         {'field_name': 'bk_id_game'}],
                              'CALCULATION': [{'field_name': 'achievements', 'calculation': 'sum'},
                                              {'field_name': 'pcs', 'calculation': 'sum'},
                                              {'field_name': 'price', 'calculation': 'sum'}],
                              'WHERE': [{'field_name': 'achievements',
                                         'where': '>', 'condition': '5'},
                                        {'field_name': 'year',
                                         'where': '=', 'condition': '2024'}]}

#
sum_over_aggregate: dict = {'SELECT': [{'field_name': 'year'}],
                            'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                            {'field_name': 'pcs', 'calculation': 'sum'}, ],
                            'WHERE': []}

data_from_frontend_wth_join_calc: dict = {'SELECT': [{'field_name': 'year'}],
                                          'CALCULATION': [{'field_name': "developer_name", 'calculation': 'count'}],
                                          'WHERE': [{'field_name': 'year', 'where': '=', 'condition': '2024'}]}

# This should only use dimension table
only_dimension: dict = {'SELECT': [{'field_name': 'bk_id_game'}],
                        'CALCULATION': [],
                        'WHERE': []}

# Should not use aggregation with pcs
no_psc_ready_aggregation: dict = {'SELECT': [],
                                  'CALCULATION': [{'field_name': 'pcs', 'calculation': 'sum'}],
                                  'WHERE': [{'field_name': 'achievements',
                                             'where': '>', 'condition': '5'}]}


def test_short_tables_collection_for_select() -> None:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(base_table_with_join)
    frontend_to_backend_type_group: OlapFrontendToBackend = OlapFrontendToBackend(sum_over_aggregate)
    frontend_to_backend_type_join_calc: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_wth_join_calc)
    olap_service: OlapService = OlapService()

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 1
    assert len(short_table_only_base.get_selects("olap_test.games_olap.base_sales")) == 2
    assert len(short_table_only_base.get_aggregations_without_join("olap_test.games_olap.base_sales")) == 3
    assert len(short_table_only_base.get_join_select("olap_test.games_olap.base_sales")) == 1
    assert len(short_table_only_base.get_aggregation_joins("olap_test.games_olap.base_sales")) == 0
    assert len(short_table_only_base.get_join_where("olap_test.games_olap.base_sales")) == 0
    assert len(short_table_only_base.get_self_where("olap_test.games_olap.base_sales")) == 2
    assert len(short_table_only_base.get_all_selects("olap_test.games_olap.base_sales")) == 9

    short_table_only_base_group: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type_group,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base_group.keys()) == 5
    assert len(short_table_only_base_group.get_selects("olap_test.games_olap.base_sales")) == 1
    assert len(short_table_only_base_group.get_aggregations_without_join("olap_test.games_olap.base_sales")) == 2

    assert len(short_table_only_base_group.get_selects("olap_test.games_olap.g_by_y")) == 3
    assert len(short_table_only_base_group.get_aggregations_without_join("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_aggregation_joins("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_join_where("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_self_where("olap_test.games_olap.g_by_y")) == 0

    # Test for aggregation over aggregation
    assert len(short_table_only_base_group.get_aggregations_without_join("olap_test.games_olap.g_by_y_p")) == 2

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type_join_calc,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base.get_aggregation_joins("olap_test.games_olap.base_sales")) == 1
    assert len(short_table_only_base.keys()) == 1


def test_base_table() -> None:
    short_table_for_select = short_select_collection_games_test(base_table_with_join)

    table_name = "olap_test.games_olap.base_sales"

    assert len(short_table_for_select.keys()) == 1
    assert list(short_table_for_select.keys())[0] == table_name

    # Test simple select

    test_select = helper_for_select(short_table_for_select, table_name)

    assert len(test_select["backend_field"].keys()) == 2

    assert test_select["backend_field"]["pcs"] == 1
    assert test_select["backend_field"]["year"] == 1

    assert test_select["frontend_field"]["pcs"] == 1
    assert test_select["frontend_field"]["year"] == 1

    assert test_select["frontend_calculation"][None] == 2

    # Test aggregations without join

    test_aggregation: dict = helper_for_aggregation(short_table_for_select, table_name)

    assert len(test_aggregation["backend_field"].keys()) == 3
    assert len(test_aggregation["backend_calculation"].keys()) == 1
    assert len(test_aggregation["frontend_calculation"].keys()) == 1
    assert len(test_aggregation["frontend_field"].keys()) == 3

    assert test_aggregation["backend_field"]["achievements"] == 1
    assert test_aggregation["backend_field"]["pcs"] == 1
    assert test_aggregation["backend_field"]["price"] == 1

    assert test_aggregation["frontend_field"]["achievements"] == 1
    assert test_aggregation["frontend_field"]["pcs"] == 1
    assert test_aggregation["frontend_field"]["price"] == 1

    assert test_aggregation["frontend_calculation"]["sum"] == 3
    assert test_aggregation["backend_calculation"]["sum"] == 3

    # Test join select
    test_join_select: dict = helper_for_join_select(short_table_for_select, table_name)
    assert len(test_join_select.keys()) == 1
    assert test_join_select["olap_test.games_olap.dim_game"]["service_key"]["sk_id_game"] == 1
    assert test_join_select["olap_test.games_olap.dim_game"]["backend_field"]["bk_game_id"] == 1
    assert test_join_select["olap_test.games_olap.dim_game"]["frontend_field"]["bk_id_game"] == 1
    assert test_join_select["olap_test.games_olap.dim_game"]["frontend_calculation"][None] == 1

    assert len(test_join_select["olap_test.games_olap.dim_game"]["service_key"]) == 1
    assert len(test_join_select["olap_test.games_olap.dim_game"]["backend_field"]) == 1
    assert len(test_join_select["olap_test.games_olap.dim_game"]["frontend_field"]) == 1
    assert len(test_join_select["olap_test.games_olap.dim_game"]["frontend_calculation"]) == 1

    # Test aggregation joins

    test_aggregation_joins: dict = helper_for_join_aggregation(short_table_for_select, table_name)

    assert len(test_aggregation_joins.keys()) == 0

    # Test Join Where

    test_join_where: dict = helper_for_join_where(short_table_for_select, table_name)

    assert len(test_join_where.keys()) == 0

    # Test self were
    test_self_where = short_table_for_select.get_self_where(table_name)
    assert len(test_self_where.keys()) == 2
    assert "achievements" in test_self_where.keys()
    assert "year" in test_self_where.keys()

    # Test all selects
    assert len(short_table_for_select.get_all_selects(table_name)) == 9


def test_aggregate_over_aggregate() -> None:
    short_table_for_select = short_select_collection_games_test(sum_over_aggregate)

    assert len(short_table_for_select.keys()) == 5

    assert len(short_table_for_select["olap_test.games_olap.base_sales"]["select"]) == 1
    assert len(short_table_for_select["olap_test.games_olap.base_sales"]["aggregation"]) == 2

    assert len(short_table_for_select["olap_test.games_olap.g_by_y_p"]["select"]) == 1
    assert len(short_table_for_select["olap_test.games_olap.g_by_y_p"]["aggregation"]) == 2

    assert len(short_table_for_select["olap_test.games_olap.g_by_y_p"]["select"]) == 1
    assert len(short_table_for_select["olap_test.games_olap.g_by_y_p"]["aggregation"]) == 2

    assert len(short_table_for_select["olap_test.games_olap.g_by_y_ym"]["select"]) == 1
    assert len(short_table_for_select["olap_test.games_olap.g_by_y_ym"]["aggregation"]) == 2

    # olap_test.games_olap.g_by_y has already calculated fields and only will use select
    assert len(short_table_for_select["olap_test.games_olap.g_by_y"]["select"]) == 3
    assert len(short_table_for_select["olap_test.games_olap.g_by_y"]["aggregation"]) == 0


def test_only_dimension() -> None:
    short_table_for_select = short_select_collection_games_test(only_dimension)

    print(short_table_for_select)


def no_psc_ready_aggregation_test() -> None:
    short_table_for_select = short_select_collection_games_test(no_psc_ready_aggregation)

    assert len(short_table_for_select.keys()) == 1
    assert 'olap_test.games_olap.base_sales' in short_table_for_select.keys()


if __name__ == '__main__':
    no_psc_ready_aggregation_test()
