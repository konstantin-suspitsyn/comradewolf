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

    assert {'release_date': {'field_type': 'dimension', 'front_name': 'Release date', 'data_type': 'date'},
            'english': {'field_type': 'dimension', 'front_name': 'Has english', 'data_type': 'text'},
            'achievements': {'field_type': 'value', 'front_name': 'Amount of achievements', 'data_type': 'number'},
            'price': {'field_type': 'value', 'front_name': 'Price', 'data_type': 'number'},
            'pcs': {'field_type': 'value', 'front_name': 'Pieces', 'data_type': 'number'},
            'sales_rub': {'field_type': 'value', 'front_name': 'Sales Rub', 'data_type': 'number'},
            'year': {'field_type': 'dimension', 'front_name': 'Year', 'data_type': 'number'},
            'yearmonth': {'field_type': 'dimension', 'front_name': 'Year_Month', 'data_type': 'number'},
            'developer_name': {'field_type': 'dimension', 'front_name': 'Game Devloper', 'data_type': 'text'},
            'bk_id_game': {'field_type': 'dimension', 'front_name': 'Game Id', 'data_type': 'number'},
            'game_name': {'field_type': 'dimension', 'front_name': 'Game Name', 'data_type': 'text'},
            'platform_name': {'field_type': 'dimension', 'front_name': 'Platform Name', 'data_type': 'text'},
            'publisher_name': {'field_type': 'dimension', 'front_name': 'Publisher Name', 'data_type': 'text'}} \
           == olap_frontend


if __name__ == "__main__":
    test_frontend_view()
