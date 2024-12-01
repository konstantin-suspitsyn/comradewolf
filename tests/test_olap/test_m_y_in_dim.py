from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontend, OlapFrontendToBackend
from tests.constants_for_testing import get_olap_sales_folder
from tests.test_olap.test_frontend_data import sales_olap_date_and_pcs

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_sales_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
olap_select_builder = OlapPostgresSelectBuilder()
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields

def test_date_in_dimension() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        sales_olap_date_and_pcs, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert  "GROUP BY" not in s.get_sql("sales.sales.date_sku")
    assert  "GROUP BY" in s.get_sql("sales.sales.reciepts")

if __name__ == "__main__":
    test_date_in_dimension()