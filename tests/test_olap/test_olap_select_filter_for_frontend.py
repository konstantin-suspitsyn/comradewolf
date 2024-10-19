from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend, OlapFilterFrontend
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.filter_type_data import one_bk_no_calc, one_bk_no_calc_max_min, year_field, year_field_max_min

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
olap_select_builder = OlapPostgresSelectBuilder()
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields

def test_one_bk_no_calc() -> None:
    front_to_back = OlapFilterFrontend(one_bk_no_calc)

    s = olap_service.select_filter_for_frontend(front_to_back, olap_structure_generator.get_tables_collection())

    assert len(s) == 1

    for key in s:
        assert "SELECT DISTINCT" in s.get_sql(key)
        assert key in s.get_sql(key)
        assert "game_name_f" in s.get_sql(key)


def test_one_bk_no_calc_max_min() -> None:
    front_to_back = OlapFilterFrontend(one_bk_no_calc_max_min)

    s = olap_service.select_filter_for_frontend(front_to_back, olap_structure_generator.get_tables_collection())

    assert len(s) == 1

    for key in s:
        assert "MAX(" in s.get_sql(key)
        assert "MIN(" in s.get_sql(key)
        assert key in s.get_sql(key)
        assert "game_name_f" in s.get_sql(key)

def test_year_field_max_min() -> None:
    front_to_back = OlapFilterFrontend(year_field_max_min)

    s = olap_service.select_filter_for_frontend(front_to_back, olap_structure_generator.get_tables_collection())

    assert len(s) == 5

    for key in s:
        assert "MAX(" in s.get_sql(key)
        assert "MIN(" in s.get_sql(key)
        assert key in s.get_sql(key)
        assert "year_f" in s.get_sql(key)

def test_year_field() -> None:
    front_to_back = OlapFilterFrontend(year_field)

    s = olap_service.select_filter_for_frontend(front_to_back, olap_structure_generator.get_tables_collection())

    assert len(s) == 5

    for key in s:
        assert "SELECT DISTINCT" in s.get_sql(key)
        assert key in s.get_sql(key)
        assert "year_f" in s.get_sql(key)

if __name__ == "__main__":
    test_one_bk_no_calc()
    test_one_bk_no_calc_max_min()
    test_year_field()
    test_year_field_max_min()
