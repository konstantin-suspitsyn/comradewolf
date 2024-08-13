from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder

data_from_frontend: dict = {'SELECT': [{'field_name': 'year'},
                                       {'field_name': 'pcs'},
                                       {'field_name': 'bk_id_game'}],
                            'CALCULATION': [{'field_name': 'achievements', 'calculation': 'sum'},
                                            {'field_name': 'pcs', 'calculation': 'sum'},
                                            {'field_name': 'price', 'calculation': 'sum'}],
                            'WHERE': [{'field_name': 'achievements',
                                       'where': '>', 'condition': '5'},
                                      {'field_name': 'year',
                                       'where': '=', 'condition': '2024'}]}

data_from_frontend_g: dict = {'SELECT': [{'field_name': 'year'}],
                              'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                              {'field_name': 'pcs', 'calculation': 'sum'},],
                              'WHERE': []}


def test_short_tables_collection_for_select() -> None:
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend)
    frontend_to_backend_type_group: OlapFrontendToBackend = OlapFrontendToBackend(data_from_frontend_g)
    olap_service: OlapService = OlapService()

    # Should be only main field left
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base) == 1
    assert len(short_table_only_base.get_selects("olap_test.games_olap.base_sales")) == 2
    assert len(short_table_only_base.get_aggregations_without_join("olap_test.games_olap.base_sales")) == 3
    assert len(short_table_only_base.get_join_select("olap_test.games_olap.base_sales")) == 1
    assert len(short_table_only_base.get_aggregation_joins("olap_test.games_olap.base_sales")) == 0
    assert len(short_table_only_base.get_join_where("olap_test.games_olap.base_sales")) == 0
    assert len(short_table_only_base.get_self_where("olap_test.games_olap.base_sales")) == 2
    assert len(short_table_only_base.get_all_selects("olap_test.games_olap.base_sales")) == 9

    short_table_only_base_group: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type_group,
                                                      olap_structure_generator.get_tables_collection())

    assert len(short_table_only_base_group.keys()) == 5
    assert len(short_table_only_base_group.get_selects("olap_test.games_olap.base_sales")) == 1
    assert len(short_table_only_base_group.get_aggregations_without_join("olap_test.games_olap.base_sales")) == 2

    assert len(short_table_only_base_group.get_selects("olap_test.games_olap.g_by_y")) == 3
    assert len(short_table_only_base_group.get_aggregations_without_join("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_aggregation_joins("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_join_where("olap_test.games_olap.g_by_y")) == 0
    assert len(short_table_only_base_group.get_self_where("olap_test.games_olap.g_by_y")) == 0
