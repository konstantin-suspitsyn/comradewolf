from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend
from tests.constants_for_testing import get_olap_games_folder

data_from_frontend: dict = {'SELECT': [{'field_name': 'year'},
                                       {'field_name': 'pcs'},
                                       {'field_name': 'bk_id_game'}],
                            'CALCULATION': [{'field_name': 'achievements', 'calculation': 'SUM'},
                                            {'field_name': 'pcs', 'calculation': 'SUM'},
                                            {'field_name': 'price', 'calculation': 'SUM'}],
                            'WHERE': [{'field_name': 'achievements',
                                       'where': '>', 'condition': '5'},
                                      {'field_name': 'year',
                                       'where': '=', 'condition': '2024'}]}


# print(data_from_frontend)


def data_dimension_table() -> None:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend)
    olap_service: OlapService = OlapService()
    olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                olap_structure_generator.get_tables_collection())
    print(get_olap_games_folder())


if __name__ == '__main__':
    data_dimension_table()
