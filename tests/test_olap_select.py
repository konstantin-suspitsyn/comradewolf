from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder

data_from_frontend_g: dict = {'SELECT': [{'field_name': 'year'}],
                              'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                              {'field_name': 'pcs', 'calculation': 'sum'},],
                              'WHERE': [{'field_name': 'year',
                                        'where': '=', 'condition': '2024'}]}

data_from_frontend_wth_join: dict = {'SELECT': [{'field_name': 'year'}, {'field_name': "developer_name"}],
                              'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                              {'field_name': 'pcs', 'calculation': 'sum'},],
                              'WHERE': [{'field_name': 'year',
                                        'where': '=', 'condition': '2024'}]}

data_from_frontend_wth_join_calc: dict = {'SELECT': [{'field_name': 'year'}],
                              'CALCULATION': [{'field_name': "developer_name", 'calculation': 'count'}],
                              'WHERE': [{'field_name': 'year',
                                        'where': '=', 'condition': '2024'}]}

data_from_frontend_where_join: dict = {'SELECT': [{'field_name': 'year'}],
                              'CALCULATION': [{'field_name': "developer_name", 'calculation': 'count'}],
                              'WHERE': [{'field_name': 'developer_name',
                                        'where': '=', 'condition': 'abc'}]}

def generate_data_for_test(data_from_frontend: dict) -> ShortTablesCollectionForSelect:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_wth_join)
    olap_service: OlapService = OlapService()
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend,
                                                      olap_structure_generator.get_tables_collection())


# if __name__ == "__main__":
#
#     frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_g)
#     frontend_to_backend_type_join: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_wth_join)
#     frontend_to_backend_type_join_calc: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_where_join)
#     olap_service: OlapService = OlapService()
#
#     # Should be only main field left
#     short_table_only_base: ShortTablesCollectionForSelect \
#         = olap_service.generate_pre_select_collection(frontend_to_backend_type_join_calc,
#                                                       olap_structure_generator.get_tables_collection())
#
#     olap_service.generate_selects_from_collection(short_table_only_base)
