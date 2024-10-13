from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend, OlapFilterFrontend
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.filter_type_data import one_bk_no_calc, year_field

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)

def test_distinct_all():
    front_to_back = OlapFilterFrontend(year_field)
    all_tables_with_field = olap_service.get_tables_with_field(front_to_back.get_field_alias_name(),
                                                               olap_structure_generator.get_tables_collection())

    tables = olap_service.get_tables_for_filter(front_to_back.get_field_alias_name(), all_tables_with_field,
                                       olap_structure_generator.get_tables_collection())

    assert len(tables) == 5


if __name__ == "__main__":
    test_distinct_all()
