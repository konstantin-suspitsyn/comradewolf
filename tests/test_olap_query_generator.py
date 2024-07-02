from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend
from tests.constants_for_testing import get_olap_games_folder

data_from_frontend: dict = {'SELECT': [{'tableName': 'olap_test.games_olap.base_sales', 'fieldName': 'english'},
                                       {'tableName': 'olap_test.games_olap.base_sales', 'fieldName': 'pcs'},
                                       {'tableName': 'olap_test.games_olap.dim_game', 'fieldName': 'bk_id_game'}],
                            'CALCULATION': [{'tableName': 'olap_test.games_olap.base_sales',
                                             'fieldName': 'achievements', 'calculation': 'SUM'},
                                            {'tableName': 'olap_test.games_olap.base_sales', 'fieldName': 'pcs',
                                             'calculation': 'SUM'}, {'tableName': 'olap_test.games_olap.base_sales',
                                                                     'fieldName': 'price', 'calculation': 'SUM'}],
                            'WHERE': [{'tableName': 'olap_test.games_olap.base_sales', 'fieldName': 'achievements',
                                       'where': '>', 'condition': '5'},
                                      {'tableName': 'olap_test.games_olap.base_sales', 'fieldName': 'year',
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
