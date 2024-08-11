from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend
from tests.constants_for_testing import get_olap_games_folder

data_from_frontend: dict = {'SELECT': [{'fieldName': 'english'},
                                       {'fieldName': 'pcs'},
                                       {'fieldName': 'bk_id_game'}],
                            'CALCULATION': [{'fieldName': 'achievements', 'calculation': 'SUM'},
                                            {'fieldName': 'pcs', 'calculation': 'SUM'},
                                            {'fieldName': 'price', 'calculation': 'SUM'}],
                            'WHERE': [{'fieldName': 'achievements',
                                       'where': '>', 'condition': '5'},
                                      {'fieldName': 'year',
                                       'where': '=', 'condition': '2024'}]}


# print(data_from_frontend)


def data_dimension_table() -> None:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend)
    olap_service: OlapService = OlapService(frontend_to_backend_type, olap_structure_generator)
    olap_service.generate_select()
    print(get_olap_games_folder())


if __name__ == '__main__':
    data_dimension_table()
