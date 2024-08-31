from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap_service_ShortTablesCollectionForSelect import base_table_with_join, sum_over_aggregate, \
    data_from_frontend_wth_join_calc, no_psc_ready_aggregation


def test_short_tables_collection_for_select() -> None:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(no_psc_ready_aggregation)
    olap_service: OlapService = OlapService()

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    query_result = olap_service.generate_selects_from_collection(short_table_only_base)

    for table in query_result:
        print(table)
        print(query_result[table]["sql"])

    return


if __name__ == '__main__':
    test_short_tables_collection_for_select()
