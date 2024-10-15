from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend, OlapFilterFrontend, SelectFilter, TableForFilter
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.filter_type_data import one_bk_no_calc, year_field, one_bk_no_calc_max_min, year_field_max_min

BASE_TABLE_NAME = "olap_test.games_olap.base_sales"
G_BY_Y_YM_TABLE_NAME = "olap_test.games_olap.g_by_y_ym"
G_BY_Y_TABLE_NAME = "olap_test.games_olap.g_by_y"
G_BY_Y_YM_P_TABLE_NAME = "olap_test.games_olap.g_by_y_ym_p"
G_BY_Y_P_TABLE_NAME = "olap_test.games_olap.g_by_y_p"
DIM_GAMES = "olap_test.games_olap.dim_game"
DIM_PUBLISHER = "olap_test.games_olap.dim_publisher"

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)

def test_distinct_all_not_dimension():
    front_to_back = OlapFilterFrontend(year_field)
    all_tables_with_field = olap_service.get_tables_with_field(front_to_back.get_field_alias_name(),
                                                               olap_structure_generator.get_tables_collection())


    tables: list[TableForFilter] = olap_service.get_tables_for_filter(front_to_back.get_field_alias_name(),
                                                                      all_tables_with_field,
                                                                      olap_structure_generator.get_tables_collection())

    assert len(tables) == 5

    select_filter: SelectFilter = olap_service.generate_filter_select(tables, front_to_back.get_field_alias_name(),
                                                                      front_to_back.get_select_type(),
                                                                      olap_structure_generator.get_tables_collection())

    for item in [BASE_TABLE_NAME, G_BY_Y_P_TABLE_NAME, G_BY_Y_TABLE_NAME, G_BY_Y_P_TABLE_NAME, G_BY_Y_YM_TABLE_NAME]:
        assert item in select_filter
        assert "SELECT DISTINCT" in select_filter.get_sql(item)


def test_all_dimension():
    front_to_back = OlapFilterFrontend(one_bk_no_calc)
    all_tables_with_field = olap_service.get_tables_with_field(front_to_back.get_field_alias_name(),
                                                               olap_structure_generator.get_tables_collection())

    tables: list[TableForFilter] = olap_service.get_tables_for_filter(front_to_back.get_field_alias_name(),
                                                                      all_tables_with_field,
                                                                      olap_structure_generator.get_tables_collection())

    assert len(tables) == 1

    select_filter: SelectFilter = olap_service.generate_filter_select(tables, front_to_back.get_field_alias_name(),
                                                                      front_to_back.get_select_type(),
                                                                      olap_structure_generator.get_tables_collection())

    assert DIM_GAMES in select_filter

    assert "SELECT DISTINCT" in select_filter.get_sql(DIM_GAMES)


def test_max_min_not_dimension():
    front_to_back = OlapFilterFrontend(year_field_max_min)
    all_tables_with_field = olap_service.get_tables_with_field(front_to_back.get_field_alias_name(),
                                                               olap_structure_generator.get_tables_collection())

    tables: list[TableForFilter] = olap_service.get_tables_for_filter(front_to_back.get_field_alias_name(),
                                                                      all_tables_with_field,
                                                                      olap_structure_generator.get_tables_collection())

    assert len(tables) == 5

    select_filter: SelectFilter = olap_service.generate_filter_select(tables, front_to_back.get_field_alias_name(),
                                                                      front_to_back.get_select_type(),
                                                                      olap_structure_generator.get_tables_collection())

    for item in [BASE_TABLE_NAME, G_BY_Y_P_TABLE_NAME, G_BY_Y_TABLE_NAME, G_BY_Y_P_TABLE_NAME, G_BY_Y_YM_TABLE_NAME]:
        assert item in select_filter
        assert "MAX(" in select_filter.get_sql(item)
        assert "MIN(" in select_filter.get_sql(item)

def test_max_min_dimension():
    front_to_back = OlapFilterFrontend(one_bk_no_calc_max_min)
    all_tables_with_field = olap_service.get_tables_with_field(front_to_back.get_field_alias_name(),
                                                               olap_structure_generator.get_tables_collection())

    tables: list[TableForFilter] = olap_service.get_tables_for_filter(front_to_back.get_field_alias_name(),
                                                                      all_tables_with_field,
                                                                      olap_structure_generator.get_tables_collection())

    assert len(tables) == 1

    select_filter: SelectFilter = olap_service.generate_filter_select(tables, front_to_back.get_field_alias_name(),
                                                                      front_to_back.get_select_type(),
                                                                      olap_structure_generator.get_tables_collection())

    assert DIM_GAMES in select_filter

    assert "MAX(" in select_filter.get_sql(DIM_GAMES)
    assert "MIN(" in select_filter.get_sql(DIM_GAMES)


if __name__ == "__main__":
    test_distinct_all_not_dimension()
    test_all_dimension()
    test_max_min_dimension()
    test_max_min_not_dimension()
