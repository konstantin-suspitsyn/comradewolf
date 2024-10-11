from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend, OlapFrontendToBackend
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import where_in_string, where_in_number, base_table_with_join_wth_where, \
    where_not_in_string, where_not_in_number, where_between_string, where_between_numbers

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
olap_select_builder = OlapPostgresSelectBuilder()
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields

def test_in_string():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_in_string, frontend_all_items_view)
    assert [{'field_name': 'game_name', 'where': 'IN', 'condition': "('Uno','Dos')"}] == \
        frontend_to_backend_type["WHERE"]

def test_in_number():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_in_number, frontend_all_items_view)

    assert [{'field_name': 'bk_id_game', 'where': 'IN', 'condition': '(12,25)'}] == \
           frontend_to_backend_type["WHERE"]

def test_in_date():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_wth_where, frontend_all_items_view)

    assert [{'field_name': 'release_date', 'where': '>', 'condition': "'2024-01-01'"},
            {'field_name': 'price', 'where': '>', 'condition': '1000'},
            {'field_name': 'game_name', 'where': '=', 'condition': "'The Best Game'"}] == \
           frontend_to_backend_type["WHERE"]

def test_not_in_string():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_not_in_string, frontend_all_items_view)
    assert [{'field_name': 'game_name', 'where': 'NOT IN', 'condition': "('Uno','Dos')"}] == \
           frontend_to_backend_type["WHERE"]

def test_not_in_number():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_not_in_number, frontend_all_items_view)

    assert [{'field_name': 'bk_id_game', 'where': 'NOT IN', 'condition': '(12,25)'}] == \
           frontend_to_backend_type["WHERE"]

def test_between_string():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_between_string, frontend_all_items_view)

    assert [{'field_name': 'game_name', 'where': 'BETWEEN', 'condition': "'Uno' AND 'Dos'"}] == \
           frontend_to_backend_type["WHERE"]

def test_between_number():
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        where_between_numbers, frontend_all_items_view)

    assert [{'field_name': 'bk_id_game', 'where': 'BETWEEN', 'condition': "1 AND 1000"}] == \
           frontend_to_backend_type["WHERE"]

if __name__ == "__main__":
    # test_in_number()
    # test_in_date()
    test_not_in_string()
    test_not_in_number()
    test_between_string()
    test_between_number()
