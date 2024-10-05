from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend
from tests.constants_for_testing import get_olap_games_folder

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)

def test_frontend_view() -> None:
    olap_frontend: OlapFrontend = olap_structure_generator.frontend_fields

    assert len(olap_frontend) == 13

    front_keys = ['release_date', 'english', 'achievements', 'price', 'pcs', 'sales_rub', 'year', 'yearmonth',
                  'developer_name', 'bk_id_game', 'game_name', 'platform_name', 'publisher_name']

    for front_key in front_keys:
        assert front_key in olap_frontend

    assert {'achievements': {'data_type': 'number', 'field_type': 'value', 'front_name': 'Amount of achievements'},
     'bk_id_game': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Game Id'},
     'developer_name': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Game Devloper'},
     'english': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Has english'},
     'game_name': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Game Name'},
     'pcs': {'data_type': 'number', 'field_type': 'value', 'front_name': 'Pieces'},
     'platform_name': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Platform Name'},
     'price': {'data_type': 'number', 'field_type': 'value', 'front_name': 'Price'},
     'publisher_name': {'data_type': 'text', 'field_type': 'dimension', 'front_name': 'Publisher Name'},
     'release_date': {'data_type': 'date', 'field_type': 'dimension', 'front_name': 'Release date'},
     'sales_rub': {'data_type': 'number', 'field_type': 'value', 'front_name': 'Sales Rub'},
     'year': {'data_type': 'number', 'field_type': 'dimension', 'front_name': 'Year'},
     'yearmonth': {'data_type': 'number', 'field_type': 'dimension', 'front_name': 'Year_Month'}} == olap_frontend


if __name__ == "__main__":
    test_frontend_view()
