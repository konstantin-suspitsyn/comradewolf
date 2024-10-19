from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapFrontend
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import one_dimension, one_dimension_count, one_dimension_count_where

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)

olap_select_builder = OlapPostgresSelectBuilder()
postgres_query_generator = OlapPostgresSelectBuilder()
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)


def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        one_dimension, frontend_all_items_view)

    table_name, select_list, select_for_group_by, where, has_calculation \
        = olap_service.generate_structure_for_dimension_table(frontend_to_backend_type,
                                                              olap_structure_generator.get_tables_collection())

    assert len(select_list) == 1
    assert has_calculation is False
    assert select_list[0] == 'dim_game.bk_game_id_f as "bk_id_game"'
    assert table_name == 'olap_test.games_olap.dim_game'
    assert len(where) == 0
    assert len(select_for_group_by) == 1
    assert select_for_group_by[0] == 'dim_game.bk_game_id_f'

def test_should_be_only_base_table_wth_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        one_dimension_count, frontend_all_items_view)

    table_name, select_list, select_for_group_by, where, has_calculation \
        = olap_service.generate_structure_for_dimension_table(frontend_to_backend_type,
                                                              olap_structure_generator.get_tables_collection())

    assert len(select_list) == 2
    assert has_calculation is True
    for item in ['dim_game.bk_game_id_f as "bk_id_game"', 'count(dim_game.bk_game_id_f) as "bk_id_game__count"']:
         assert item in select_list
    assert select_list[0] == 'dim_game.bk_game_id_f as "bk_id_game"'
    assert table_name == 'olap_test.games_olap.dim_game'
    assert len(where) == 0
    assert len(select_for_group_by) == 1
    assert select_for_group_by[0] == 'dim_game.bk_game_id_f'

def test_should_be_only_base_table_wth_where() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        one_dimension_count_where, frontend_all_items_view)

    table_name, select_list, select_for_group_by, where, has_calculation \
        = olap_service.generate_structure_for_dimension_table(frontend_to_backend_type,
                                                              olap_structure_generator.get_tables_collection())

    assert len(select_list) == 2
    assert has_calculation is True
    for item in ['dim_game.bk_game_id_f as "bk_id_game"', 'count(dim_game.bk_game_id_f) as "bk_id_game__count"']:
         assert item in select_list
    assert select_list[0] == 'dim_game.bk_game_id_f as "bk_id_game"'
    assert table_name == 'olap_test.games_olap.dim_game'
    assert len(where) == 1
    for item in ['dim_game.game_name_f like \'a%\'']:
        assert item in where
    assert len(select_for_group_by) == 1
    assert select_for_group_by[0] == 'dim_game.bk_game_id_f'


if __name__ == '__main__':
    test_should_be_only_base_table_no_group_by()
    test_should_be_only_base_table_wth_group_by()
    test_should_be_only_base_table_wth_where()
