from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect, OlapFrontend
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import base_table_with_join_no_where_no_calc, base_table_with_join_no_where, \
    group_by_read_no_where, group_by_also_in_agg, one_agg_value, one_dimension, base_table_with_join_no_gb, \
    base_table_with_and_agg_with_join, base_table_with_and_agg, base_table_with_and_agg_without_join, \
    base_table_with_no_join_wht_where, base_table_with_join_wht_where, base_table_with_join_with_where, \
    base_table_with_join_wth_where

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)

BASE_TABLE_NAME = "olap_test.games_olap.base_sales"
G_BY_Y_YM_TABLE_NAME = "olap_test.games_olap.g_by_y_ym"
G_BY_Y_TABLE_NAME = "olap_test.games_olap.g_by_y"
G_BY_Y_YM_P_TABLE_NAME = "olap_test.games_olap.g_by_y_ym_p"
G_BY_Y_P_TABLE_NAME = "olap_test.games_olap.g_by_y_p"
DIM_GAMES = "olap_test.games_olap.dim_game"
DIM_PUBLISHER = "olap_test.games_olap.dim_publisher"


def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_no_where_no_calc, frontend_all_items_view)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 2
    assert 'base_sales.year_f as "year"' in select_list
    assert 'base_sales.pcs_f as "pcs"' in select_list
    assert len(select_for_group_by) == 0
    assert len(joins) == 0
    assert len(where) == 0
    assert len(select_for_group_by) == 0
    assert has_calculation is False


def test_should_be_only_base_table_with_group_by():
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_no_where, frontend_all_items_view)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 6

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"',
                 'sum(base_sales.achievements_f) as "achievements__sum"', 'sum(base_sales.pcs_f) as "pcs__sum"',
                 'sum(base_sales.price_f) as "price__sum"', 'dim_game.bk_game_id_f as "bk_id_game"']:
        assert item in select_list

    assert len(select_for_group_by) == 3

    for item in ['base_sales.year_f', 'base_sales.pcs_f', 'dim_game.bk_game_id_f']:
        assert item in select_for_group_by

    assert has_calculation is True
    assert len(joins) == 1

    assert {'olap_test.games_olap.dim_game': 'ON base_sales.sk_id_game_f = dim_game.sk_id_game_f'} == joins

    assert len(where) == 0

    assert 1 == 1


def test_base_agg_wth_agg():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        group_by_also_in_agg, frontend_all_items_view)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 3

    for item in ['base_sales.year_f as "year"', 'sum(base_sales.sales_rub_f) as "sales_rub__sum"',
                 'sum(base_sales.pcs_f) as "pcs__sum"']:
        assert item in select_list

    assert len(select_for_group_by) == 1

    for item in ['base_sales.year_f']:
        assert item in select_for_group_by

    assert has_calculation is True
    assert len(joins) == 0

    assert len(where) == 0

    # G_BY_Y_YM_TABLE_NAME
    select_list_g_by_y_ym, select_for_group_by_g_by_y_ym, joins_g_by_y_ym, where_g_by_y_ym, order_by, \
        has_calculation_g_by_y_ym \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_TABLE_NAME)

    assert len(select_list_g_by_y_ym) == 3

    for item in ['g_by_y_ym.year_f as "year"', 'sum(g_by_y_ym.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_ym.sum_pcs_f) as "pcs__sum"']:
        assert item in select_list_g_by_y_ym

    assert len(select_for_group_by_g_by_y_ym) == 1

    assert has_calculation_g_by_y_ym is True
    assert len(joins_g_by_y_ym) == 0

    assert len(where_g_by_y_ym) == 0

    # G_BY_Y_TABLE_NAME
    select_list_g_by_y, select_for_group_by_g_by_y, joins_g_by_y, where_g_by_y, order_by, has_calculation_g_by_y \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_TABLE_NAME)

    assert len(select_list_g_by_y) == 3

    for item in ['g_by_y.year_f as "year"', 'g_by_y.sum_sales_rub_f as "sales_rub__sum"',
                 'g_by_y.sum_pcs_f as "pcs__sum"']:
        assert item in select_list_g_by_y

    assert len(select_for_group_by_g_by_y) == 0

    assert has_calculation_g_by_y is False
    assert len(joins_g_by_y) == 0

    assert len(where_g_by_y) == 0

    # G_BY_Y_YM_P_TABLE_NAME
    select_list_y_ym_p, select_for_group_by_y_ym_p, joins_g_y_ym_p, where_g_y_ym_p, order_by, has_calculation_g_y_ym_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_P_TABLE_NAME)

    assert len(select_list_y_ym_p) == 3

    for item in ['g_by_y_ym_p.year_f as "year"', 'sum(g_by_y_ym_p.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_ym_p.sum_pcs_f) as "pcs__sum"']:
        assert item in select_list_y_ym_p

    assert len(select_for_group_by_y_ym_p) == 1

    assert has_calculation_g_y_ym_p is True
    assert len(joins_g_y_ym_p) == 0

    assert len(where_g_y_ym_p) == 0

    # G_BY_Y_P_TABLE_NAME
    select_list_y_p, select_for_group_by_y_p, joins_g_y_p, where_g_y_p, order_by, has_calculation_g_y_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_P_TABLE_NAME)

    assert len(select_list_y_p) == 3

    for item in ['g_by_y_p.year_f as "year"', 'sum(g_by_y_p.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_p.sum_pcs_f) as "pcs__sum"']:
        assert item in select_list_y_p

    assert len(select_for_group_by_y_p) == 1

    assert has_calculation_g_y_p is True
    assert len(joins_g_y_p) == 0

    assert len(where_g_y_ym_p) == 0


def test_one_value_in_aggregate():
    # Только одно поле value в агрегат
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        one_agg_value, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 1

    for item in ['sum(base_sales.sales_rub_f) as "sales_rub__sum"']:
        assert item in select_list

    assert len(select_for_group_by) == 0

    assert has_calculation is True
    assert len(joins) == 0

    assert len(where) == 0

    # G_BY_Y_YM_TABLE_NAME
    select_list_g_by_y_ym, select_for_group_by_g_by_y_ym, joins_g_by_y_ym, where_g_by_y_ym, order_by, \
        has_calculation_g_by_y_ym \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_TABLE_NAME)

    assert len(select_list_g_by_y_ym) == 1

    for item in [f'sum({G_BY_Y_YM_TABLE_NAME.split(".")[-1]}.sum_sales_rub_f) as "sales_rub__sum"']:
        assert item in select_list_g_by_y_ym

    assert len(select_for_group_by_g_by_y_ym) == 0

    assert has_calculation_g_by_y_ym is True
    assert len(joins_g_by_y_ym) == 0

    assert len(where_g_by_y_ym) == 0

    # G_BY_Y_TABLE_NAME
    select_list_g_by_y, select_for_group_by_g_by_y, joins_g_by_y, where_g_by_y, order_by, has_calculation_g_by_y \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_TABLE_NAME)

    assert len(select_list_g_by_y) == 1

    for item in [f'sum({G_BY_Y_TABLE_NAME.split(".")[-1]}.sum_sales_rub_f) as "sales_rub__sum"']:
        assert item in select_list_g_by_y

    assert len(select_for_group_by_g_by_y) == 0

    assert has_calculation_g_by_y is True
    assert len(joins_g_by_y) == 0

    assert len(where_g_by_y) == 0

    # G_BY_Y_YM_P_TABLE_NAME
    select_list_y_ym_p, select_for_group_by_y_ym_p, joins_g_y_ym_p, where_g_y_ym_p, order_by, has_calculation_g_y_ym_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_P_TABLE_NAME)

    assert len(select_list_y_ym_p) == 1

    for item in [f'sum({G_BY_Y_YM_P_TABLE_NAME.split(".")[-1]}.sum_sales_rub_f) as "sales_rub__sum"']:
        assert item in select_list_y_ym_p

    assert len(select_for_group_by_y_ym_p) == 0

    assert has_calculation_g_y_ym_p is True
    assert len(joins_g_y_ym_p) == 0

    assert len(where_g_y_ym_p) == 0

    # G_BY_Y_P_TABLE_NAME
    select_list_y_p, select_for_group_by_y_p, joins_g_y_p, where_g_y_p, order_by, has_calculation_g_y_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_P_TABLE_NAME)

    assert len(select_list_y_p) == 1

    for item in [f'sum({G_BY_Y_P_TABLE_NAME.split(".")[-1]}.sum_sales_rub_f) as "sales_rub__sum"']:
        assert item in select_list_y_p

    assert len(select_for_group_by_y_p) == 0

    assert has_calculation_g_y_p is True
    assert len(joins_g_y_p) == 0

    assert len(where_g_y_ym_p) == 0


def test_should_be_only_base_table_no_group_by_join():
    # Поля, которые есть только в базовой таблице без group by c join dimension table
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_no_gb, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 4

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"', 'dim_game.bk_game_id_f as "bk_id_game"',
                 'dim_publisher.publisher_name_field_f as "publisher_name"']:
        assert item in select_list

    assert len(select_for_group_by) == 0

    assert has_calculation is False
    assert len(joins) == 2
    assert {'olap_test.games_olap.dim_game': 'ON base_sales.sk_id_game_f = dim_game.sk_id_game_f',
            'olap_test.games_olap.dim_publisher': 'ON base_sales.sk_id_publisher_f = dim_publisher.id_f'} == joins

    assert len(where) == 0


def test_base_table_wth_gb_agg_no_gb_join():
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице без group by c join dimension table
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_and_agg, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 4

    for item in ['base_sales.year_f as "year"', 'sum(base_sales.sales_rub_f) as "sales_rub__sum"',
                 'sum(base_sales.pcs_f) as "pcs__sum"', 'dim_publisher.publisher_name_field_f as "publisher_name"']:
        assert item in select_list

    assert len(select_for_group_by) == 2

    assert has_calculation is True
    assert len(joins) == 1

    assert {'olap_test.games_olap.dim_publisher': 'ON base_sales.sk_id_publisher_f = dim_publisher.id_f'} == joins

    assert len(where) == 0

    # G_BY_Y_P_TABLE_NAME
    select_list_y_p, select_for_group_by_y_p, joins_g_y_p, where_g_y_p, order_by, has_calculation_g_y_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_P_TABLE_NAME)

    assert len(select_list_y_p) == 4

    for item in ['g_by_y_p.year_f as "year"', 'g_by_y_p.sum_sales_rub_f as "sales_rub__sum"',
                 'g_by_y_p.sum_pcs_f as "pcs__sum"', 'dim_publisher.publisher_name_field_f as "publisher_name"']:
        assert item in select_list_y_p

    assert len(select_for_group_by_y_p) == 0

    assert has_calculation_g_y_p is False
    assert len(joins_g_y_p) == 1

    assert {'olap_test.games_olap.dim_publisher': 'ON g_by_y_p.sk_id_publisher_f = dim_publisher.id_f'} == joins_g_y_p

    assert len(where_g_y_p) == 0

    # G_BY_Y_YM_P_TABLE_NAME

    select_list_y_ym_p, select_for_group_by_y_ym_p, joins_g_y_ym_p, where_g_y_ym_p, order_by, has_calculation_g_y_ym_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_P_TABLE_NAME)

    assert len(select_list_y_ym_p) == 4

    for item in ['g_by_y_ym_p.year_f as "year"', 'sum(g_by_y_ym_p.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_ym_p.sum_pcs_f) as "pcs__sum"', 'dim_publisher.publisher_name_field_f as "publisher_name"']:
        assert item in select_list_y_ym_p

    assert len(select_for_group_by_y_ym_p) == 2

    for item in ['g_by_y_ym_p.year_f', 'dim_publisher.publisher_name_field_f']:
        assert item in select_for_group_by_y_ym_p

    assert has_calculation_g_y_ym_p is True
    assert len(joins_g_y_ym_p) == 1

    assert len(where_g_y_ym_p) == 0


def test_agg_table_wth_join_with_agg():
    # Aggregate таблицу с join c последующей агрегацией
    # Поля, которые есть в базовой таблице с group by и в агрегатной таблице без group by c join dimension table
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_and_agg_with_join, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 4

    for item in ['base_sales.year_f as "year"', 'sum(base_sales.sales_rub_f) as "sales_rub__sum"',
                 'sum(base_sales.pcs_f) as "pcs__sum"',
                 'count(dim_publisher.publisher_name_field_f) as publisher_name__count']:
        assert item in select_list

    assert len(select_for_group_by) == 1

    assert has_calculation is True
    assert len(joins) == 1

    assert {'olap_test.games_olap.dim_publisher': 'ON base_sales.sk_id_publisher_f = dim_publisher.id_f'} == joins

    assert len(where) == 0

    # G_BY_Y_P_TABLE_NAME

    select_list_y_p, select_for_group_by_y_p, joins_g_y_p, where_g_y_p, order_by, has_calculation_g_y_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_P_TABLE_NAME)

    assert len(select_list_y_p) == 4

    for item in ['g_by_y_p.year_f as "year"', 'sum(g_by_y_p.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_p.sum_pcs_f) as "pcs__sum"',
                 'count(dim_publisher.publisher_name_field_f) as publisher_name__count']:
        assert item in select_list_y_p

    assert len(select_for_group_by_y_p) == 1

    assert has_calculation_g_y_p is True
    assert len(joins_g_y_p) == 1

    assert {'olap_test.games_olap.dim_publisher': 'ON g_by_y_p.sk_id_publisher_f = dim_publisher.id_f'} == joins_g_y_p

    assert len(where_g_y_p) == 0

    # G_BY_Y_YM_P_TABLE_NAME

    select_list_y_ym_p, select_for_group_by_y_ym_p, joins_g_y_ym_p, where_g_y_ym_p, order_by, has_calculation_g_y_ym_p \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, G_BY_Y_YM_P_TABLE_NAME)

    assert len(select_list_y_ym_p) == 4

    for item in ['g_by_y_ym_p.year_f as "year"', 'sum(g_by_y_ym_p.sum_sales_rub_f) as "sales_rub__sum"',
                 'sum(g_by_y_ym_p.sum_pcs_f) as "pcs__sum"',
                 'count(dim_publisher.publisher_name_field_f) as publisher_name__count']:
        assert item in select_list_y_ym_p

    assert len(select_for_group_by_y_ym_p) == 1

    assert has_calculation_g_y_ym_p is True
    assert len(joins_g_y_ym_p) == 1

    assert {'olap_test.games_olap.dim_publisher': 'ON g_by_y_ym_p.sk_id_publisher_f = dim_publisher.id_f'} == \
           joins_g_y_ym_p

    assert len(where_g_y_ym_p) == 0


def test_service_key_count():
    # Тест для калькуляции count на service_key
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_and_agg_without_join, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    # BASE_TABLE_NAME
    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 3

    for item in ['base_sales.year_f as "year"', 'base_sales.english_f as "english"',
                 'count(base_sales.sk_id_game_f) as "sk_id_game__count"']:
        assert item in select_list

    assert len(select_for_group_by) == 2

    assert has_calculation is True
    assert len(joins) == 0

    assert len(where) == 0


def test_where_in_base_table():
    # where в базовой таблице
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_no_join_wht_where, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 2

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"']:
        assert item in select_list

    assert len(select_for_group_by) == 0

    assert has_calculation is False
    assert len(joins) == 0

    assert len(where) == 2
    for item in ['base_sales.release_date_f > \'2024-01-01\'', 'base_sales.price_f > 1000']:
        assert item in where


def test_where_in_join():
    # where в join
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_wht_where, frontend_all_items_view)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 2

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"']:
        assert item in select_list

    assert len(select_for_group_by) == 0

    assert has_calculation is False
    assert len(joins) == 1
    assert {'olap_test.games_olap.dim_game': 'ON base_sales.sk_id_game_f = dim_game.sk_id_game_f'} == joins

    assert len(where) == 3
    for item in ['base_sales.release_date_f > \'2024-01-01\'', 'base_sales.price_f > 1000',
                 'dim_game.game_name = \'The Best Game\'']:
        assert item in where


def test_where_with_agg_in_base_table():
    # where c агрегацией в базовой таблице
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_with_where, frontend_all_items_view)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 5

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"',
                 'sum(base_sales.achievements_f) as "achievements__sum"', 'sum(base_sales.pcs_f) as "pcs__sum"',
                 'sum(base_sales.price_f) as "price__sum"']:
        assert item in select_list

    assert len(select_for_group_by) == 2

    assert has_calculation is True
    assert len(joins) == 0

    assert len(where) == 2
    for item in ["base_sales.release_date_f > '2024-01-01'", "base_sales.price_f > 1000"]:
        assert item in where


def test_where_with_agg_in_join():
    # where c агрегацией в join
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_wth_where, frontend_all_items_view)

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    select_list, select_for_group_by, joins, where, order_by, has_calculation \
        = olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, BASE_TABLE_NAME)

    assert len(select_list) == 6

    for item in ['base_sales.year_f as "year"', 'base_sales.pcs_f as "pcs"',
                 'sum(base_sales.achievements_f) as "achievements__sum"', 'sum(base_sales.pcs_f) as "pcs__sum"',
                 'sum(base_sales.price_f) as "price__sum"', 'dim_game.bk_game_id_f as "bk_id_game"']:
        assert item in select_list

    assert len(select_for_group_by) == 3

    assert has_calculation is True
    assert len(joins) == 1

    assert {'olap_test.games_olap.dim_game': 'ON base_sales.sk_id_game_f = dim_game.sk_id_game_f'} == joins

    assert len(where) == 3
    for item in ['base_sales.release_date_f > \'2024-01-01\'', 'base_sales.price_f > 1000',
                 'dim_game.game_name = \'The Best Game\'']:
        assert item in where


if __name__ == "__main__":
    test_should_be_only_base_table_no_group_by()
    test_should_be_only_base_table_with_group_by()
    test_base_agg_wth_agg()
    test_one_value_in_aggregate()
    test_should_be_only_base_table_no_group_by_join()
    test_base_table_wth_gb_agg_no_gb_join()
    test_agg_table_wth_join_with_agg()
    test_service_key_count()
    test_where_in_base_table()
    test_where_in_join()
    test_where_with_agg_in_base_table()
    test_where_with_agg_in_join()
