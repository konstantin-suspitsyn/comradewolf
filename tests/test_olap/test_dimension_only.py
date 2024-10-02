from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import one_dimension

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)


def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(one_dimension)

    select_list, select_for_group_by, joins, where, has_calculation \
        = olap_service.generate_structure_for_dimension_table(frontend_to_backend_type,
                                                              olap_structure_generator.get_tables_collection())

    print(select_list)


if __name__ == '__main__':
    test_should_be_only_base_table_no_group_by()
