import unittest

from comradewolf.universe.frontend_backend_converter import FrontendBackendConverter
from comradewolf.universe.joins_generator import GenerateJoins
from comradewolf.universe.possible_joins import AllPossibleJoins
from comradewolf.universe.query_generator import QueryGenerator
from comradewolf.universe.structure_generator import StructureGenerator
from comradewolf.utils.language_specific_builders import PostgresCalculationBuilder


class TestQueryGenerator(unittest.TestCase):
    def test_convert_from_frontend_to_backend(self):
        table_structure = StructureGenerator(
            r"./../tests/test_db_structure/test_tables",
            r"./../tests/test_db_structure/test_joins",
            r"./../tests/test_db_structure/test_standard_filters",
        )

        frontend_json = {
            'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no',
                       'query_builder.public.dim_warehouse.address'],
            'calculation': [{'query_builder.public.fact_stock.value': 'sum'},
                            {'query_builder.public.fact_stock.last_day_of_week_pcs': 'PREDEFINED'}], 'where': {
                'and': [{'query_builder.public.dim_calendar.date': {'operator': '=', 'condition': ['2024-03-05']}}]}}

        postgres_generator = PostgresCalculationBuilder()

        print(table_structure.get_fact_join())

        front_to_back = FrontendBackendConverter(table_structure.get_fields(), table_structure.get_tables(),
                                                 table_structure.get_where(), postgres_generator)

        fields_rebuild = front_to_back.convert_from_frontend_to_backend(frontend_json)
        # print(fields_rebuild)
        query_generator = QueryGenerator(table_structure.get_tables(), table_structure.get_fields(), table_structure.get_where(), postgres_generator)

        j = table_structure.get_joins()

        GenerateJoins(table_structure.get_joins(), table_structure.get_tables())
        possible_joins = AllPossibleJoins()
        print(query_generator.generate_select_for_one_data_table(fields_rebuild))

    def test_convert_from_frontend_to_backend_two_tables(self):
        table_structure = StructureGenerator(
            r"./../tests/test_db_structure/test_tables",
            r"./../tests/test_db_structure/test_joins",
            r"./../tests/test_db_structure/test_standard_filters",
        )

        frontend_json = {
            'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no',
                       'query_builder.public.dim_calendar.first_day_of_month'],
            'calculation': [{'query_builder.public.fact_sales.value': 'sum'},
                            {'query_builder.public.fact_sales.money': 'sum'},
                            {'query_builder.public.fact_stock.value': 'sum'}], 'where': {
                'and': [{'query_builder.public.dim_item.name': {'operator': '=', 'condition': ['Товар']}},
                        {'query_builder.public.dim_calendar.date': {'operator': '=', 'condition': ['2024-03-06']}}]}}

        postgres_generator = PostgresCalculationBuilder()

        front_to_back = FrontendBackendConverter(table_structure.get_fields(), table_structure.get_tables(),
                                                 table_structure.get_where(), postgres_generator)

        fields_rebuild = front_to_back.convert_from_frontend_to_backend(frontend_json)

        query_generator = QueryGenerator(table_structure.get_tables(), table_structure.get_fields(), postgres_generator)

        j = table_structure.get_joins()

        GenerateJoins(table_structure.get_joins(), table_structure.get_tables())
        possible_joins = AllPossibleJoins()
        print(query_generator.generate_select_for_multiple_data_tables(fields_rebuild))

