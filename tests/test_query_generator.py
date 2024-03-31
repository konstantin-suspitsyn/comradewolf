from comradewolf.universe.frontend_backend_converter import FrontendBackendConverter
from comradewolf.universe.joins_generator import GenerateJoins
from comradewolf.universe.possible_joins import AllPossibleJoins
from comradewolf.universe.query_generator import QueryGenerator
from comradewolf.universe.structure_generator import StructureGenerator
from comradewolf.utils.language_specific_builders import PostgresCalculationBuilder
from tests.constants_for_testing import get_tables_folder, get_joins_folder, get_standard_filters_folder


def create_generator():
    table_structure = StructureGenerator(
        get_tables_folder(),
        get_joins_folder(),
        get_standard_filters_folder()
    )
    postgres_builder = PostgresCalculationBuilder()
    converter = FrontendBackendConverter(table_structure.get_fields(),
                                         table_structure.get_tables(),
                                         table_structure.get_where(),
                                         postgres_builder)
    query_generator = QueryGenerator(table_structure.get_tables(), table_structure.get_fields(),
                                     table_structure.get_where(), postgres_builder)
    GenerateJoins(table_structure.get_joins(), table_structure.get_tables())
    AllPossibleJoins()
    return converter, query_generator


def test_query_generator():
    converter, query_generator = create_generator()

    front_json = {
        'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no'],
        'calculation': [{'query_builder.public.fact_sales.value': 'sum'},
                        {'query_builder.public.fact_sales.money': 'count'}],
        'where': {'query_builder.public.dim_calendar.date': {'operator': 'between', 'condition': ['2024-03-25',
                                                                                                  '2024-03-31']}}}

    converted_fields = converter.convert_from_frontend_to_backend(front_json)

    big_select = query_generator.generate_select_for_one_data_table(converted_fields).lower()

    assert "query_builder.public.dim_calendar.date".lower() in big_select
    assert "query_builder.public.dim_calendar.week_no".lower() in big_select
    assert "SUM(query_builder.public.fact_sales.value)".lower() in big_select
    assert "COUNT(query_builder.public.fact_sales.money)".lower() in big_select
    assert "left join query_builder.public.dim_calendar" in big_select
    assert "on query_builder.public.fact_sales.date = query_builder.public.dim_calendar.date" in big_select
    assert "query_builder.public.dim_calendar.date  between '2024-03-25' and '2024-03-31'" in big_select
    assert "group by" in big_select
    assert big_select.count("query_builder.public.dim_calendar.week_no") == 2
    assert big_select.count("query_builder.public.dim_calendar.date") == 4


def test_for_no_join_exception():
    """
    Test for No Join Exception
    Predefined filter is built on non-joinable field to query_builder.public.fact_stock.last_day_of_week_pcs
    :return:
    """
    front_json = {'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no'],
                  'calculation': [{'query_builder.public.fact_stock.last_day_of_week_pcs': 'PREDEFINED'}], 'where': {
            'and': [{'query_builder.public.dim_calendar.date': {'operator': '', 'condition': ['2024-03-04']}}, {
                'filter_one': {'operator': 'predefined'}}]}}

    converter, query_generator = create_generator()

    converted_fields = converter.convert_from_frontend_to_backend(front_json)

    try:
        query_generator.generate_select_for_one_data_table(converted_fields).lower()
        assert False
    except RuntimeError:
        assert True


def test_and_where():
    front_json = {'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no'],
                  'calculation': [{'query_builder.public.fact_stock.last_day_of_week_pcs': 'PREDEFINED'}], 'where': {
            'and': [{'query_builder.public.dim_calendar.date': {'operator': '=', 'condition': ['2024-03-04']}},
                    {'query_builder.public.dim_item.price': {'operator': '>', 'condition': ['1000']}}]}}

    converter, query_generator = create_generator()

    converted_fields = converter.convert_from_frontend_to_backend(front_json)

    big_select = query_generator.generate_select_for_one_data_table(converted_fields).lower()

    assert "query_builder.public.dim_calendar.week_no" in big_select
    assert "query_builder.public.dim_calendar.date" in big_select
    assert "sum(query_builder.public.fact_stock.value)" in big_select
    assert "from query_builder.public.fact_stock" in big_select
    assert "inner join query_builder.public.dim_item" in big_select
    assert "query_builder.public.dim_calendar.week_no" in big_select
    assert "on query_builder.public.fact_stock.sk_item_id = query_builder.public.dim_item.sk_item_id" in big_select
    assert "left join query_builder.public.dim_calendar" in big_select
    assert "on query_builder.public.fact_stock.date = query_builder.public.dim_calendar.date" in big_select
    assert "((query_builder.public.dim_calendar.date  = '2024-03-04' )" in big_select
    assert "and (query_builder.public.dim_item.price  > 1000 )) " in big_select
    assert "and (query_builder.public.dim_calendar.last_day_of_week = 1)" in big_select
    assert big_select.count("query_builder.public.dim_calendar.week_no") == 2
    assert big_select.count("query_builder.public.dim_calendar.date") == 4


def test_multiple_fact_tables():
    """
    Test multiple fact tables
    Should throw Runtime Error on generate_select_for_one_data_table() function call
    :return:
    """
    frontend_json = {'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no',
                                'query_builder.public.dim_item.name'], 'calculation': [{
                                    'query_builder.public.fact_stock.last_day_of_week_pcs': 'PREDEFINED'}, {
                                    'query_builder.public.fact_sales.value': 'sum'}], 'where': {'and': [{
                                        'query_builder.public.dim_calendar.date': {
                                                'operator': '=', 'condition': ['2024-03-18']}}, {
                                                'query_builder.public.dim_item.price': {'operator': '>', 'condition': [
                                                                                                     '1000']}}]}}

    converter, query_generator = create_generator()

    converted_fields = converter.convert_from_frontend_to_backend(frontend_json)

    try:
        query_generator.generate_select_for_one_data_table(converted_fields)
        assert False
    except RuntimeError:
        assert True

    big_select = query_generator.generate_select_for_multiple_data_tables(converted_fields).lower()

    assert "cte_0" in big_select
    assert "cte_1" in big_select
    assert "main_cte" in big_select

    assert big_select.count("query_builder.public.dim_calendar.date") == 8
    assert big_select.count("query_builder.public.dim_calendar.week_no") == 4
    assert big_select.count("query_builder.public.dim_item.name") == 4
    assert big_select.count("query_builder.public.fact_sales.value") == 1

    assert "left join cte_0" in big_select
    assert "left join cte_1" in big_select

    assert "cte_0.name = main_cte.name" in big_select
    assert "cte_0.date = main_cte.date" in big_select
    assert "cte_0.week_no = main_cte.week_no" in big_select
    assert "cte_1.name = main_cte.name" in big_select
    assert "cte_1.date = main_cte.date" in big_select
    assert "cte_1.week_no = main_cte.week_no" in big_select

    assert "coalesce(" in big_select
