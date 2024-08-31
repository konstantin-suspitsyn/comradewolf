from comradewolf.universe.olap_service import OlapService
from comradewolf.utils.utils_for_testing import short_select_collection_games_test, \
    generate_structure_for_each_piece_of_join_test_games
from tests.test_olap_service_ShortTablesCollectionForSelect import base_table_with_join, sum_over_aggregate


def test_short_tables_collection_for_select() -> None:

    select_list, select_for_group_by, joins, where, has_calculation = \
        generate_structure_for_each_piece_of_join_test_games(base_table_with_join, "olap_test.games_olap.base_sales")

    should_be_in_select: list = ['base_sales.year as "year"', 'base_sales.pcs as "pcs"',
                                 'sum(base_sales.achievements) as "achievements"',
                                 'sum(base_sales.pcs) as "pcs"', 'sum(base_sales.price) as "price"',
                                 'dim_game.bk_game_id as bk_id_game']

    select_for_group_by_test: list = ["base_sales.year", "base_sales.pcs", "dim_game.bk_game_id"]

    where_list_test: list = ['base_sales.achievements > 5', 'base_sales.year = 2024']

    joins_test = {'olap_test.games_olap.dim_game': 'ON base_sales.sk_id_game = dim_game.sk_id_game'}

    for select_item in should_be_in_select:
        assert select_item in select_list

    for select_for_group_by_test_item in select_for_group_by_test:
        assert select_for_group_by_test_item in select_for_group_by

    for where_item in where_list_test:
        assert where_item in where

    for key in joins_test:
        assert key in joins
        assert joins_test[key] == joins[key]


def test_aggregate_over_aggregate() -> None:
    select_list, select_for_group_by, joins, where, has_calculation = \
        generate_structure_for_each_piece_of_join_test_games(sum_over_aggregate, "olap_test.games_olap.g_by_y")

    select_list_test = ['g_by_y.year as "year"', 'g_by_y.sum_sales_rub as "sales_rub__sum"',
                        'g_by_y.sum_pcs as "pcs__sum"']

    for select_item in select_list_test:
        assert select_item in select_list

    assert len(select_for_group_by) == 0
    assert len(joins) == 0

    select_list, select_for_group_by, joins, where, has_calculation = \
        generate_structure_for_each_piece_of_join_test_games(sum_over_aggregate, "olap_test.games_olap.g_by_y_ym")

    assert len(select_for_group_by) == 1
    select_list_test = ['g_by_y_ym.year as "year"', 'sum(g_by_y_ym.sales_rub) as "sales_rub"',
                        'sum(g_by_y_ym.pcs) as "pcs"']

    for select_item in select_list_test:
        assert select_item in select_list

    assert len(joins) == 0
    print(select_list)


if __name__ == '__main__':
    test_short_tables_collection_for_select()
