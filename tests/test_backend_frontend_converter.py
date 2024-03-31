from comradewolf.universe.frontend_backend_converter import FrontendBackendConverter
from comradewolf.universe.structure_generator import StructureGenerator
from comradewolf.utils.language_specific_builders import PostgresCalculationBuilder
from tests.constants_for_testing import get_tables_folder, get_joins_folder, get_standard_filters_folder


def structure_preparation():
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
    return converter


def test_frontend_backend_converter():
    """
    Check conversion without grouping
    :return:
    """
    converter = structure_preparation()

    frontend_fields = {
        'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no',
                   'query_builder.public.dim_store.bk_address'], 'calculation': [], 'where': {
            'and': [{'filter_one': {'operator': 'predefined'}},
                    {'query_builder.public.dim_item.name': {'operator': '=', 'condition': ['111']}}]}}

    converted_fields = converter.convert_from_frontend_to_backend(frontend_fields)

    assert converted_fields["data"] == {}
    assert len(converted_fields["dimension"].keys()) == 3
    assert len(converted_fields["dimension"]["query_builder.public.dim_calendar"]["select"]) == 2
    assert len(converted_fields["dimension"]["query_builder.public.dim_calendar"]["calculation"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_calendar"]["not_for_select"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_calendar"]["where"]) == 0

    assert len(converted_fields["dimension"]["query_builder.public.dim_store"]["select"]) == 1
    assert len(converted_fields["dimension"]["query_builder.public.dim_store"]["calculation"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_store"]["not_for_select"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_store"]["where"]) == 0

    assert len(converted_fields["dimension"]["query_builder.public.dim_item"]["select"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_item"]["calculation"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_item"]["not_for_select"]) == 0
    assert len(converted_fields["dimension"]["query_builder.public.dim_item"]["where"]) == 0

    assert len(converted_fields["overall_where"]["and"]) == 2

    for item in converted_fields["overall_where"]["and"]:
        assert "filter_one" in item.keys() or "query_builder.public.dim_item.name" in item.keys()

        if "filter_one" in item.keys():
            assert item["filter_one"] == {"operator": "predefined"}

        if "query_builder.public.dim_item.name" in item.keys():
            assert item["query_builder.public.dim_item.name"]["operator"] == "="
            assert item["query_builder.public.dim_item.name"]["condition"] == ["111"]


def test_between_and_calculation():
    """
    Test between and calculations from frontend
    :return:
    """

    the_select = {'select': ['query_builder.public.dim_calendar.date', 'query_builder.public.dim_calendar.week_no'],
                  'calculation': [{'query_builder.public.fact_sales.value': 'sum'},
                                  {'query_builder.public.fact_sales.money': 'count'}],
                  'where': {'query_builder.public.dim_calendar.date': {'operator': 'between', 'condition': [
                      '2024-03-25', '2024-03-31']}}}

    converter = structure_preparation()

    converted_fields = converter.convert_from_frontend_to_backend(the_select)

    assert len(converted_fields["data"]) == 1
    assert converted_fields["data"]["query_builder.public.fact_sales"]["select"] == []
    assert "SUM(query_builder.public.fact_sales.value)" in converted_fields["data"]["query_builder.public.fact_sales"][
        "calculation"]
    assert "COUNT(query_builder.public.fact_sales.money)" in converted_fields["data"][
        "query_builder.public.fact_sales"]["calculation"]
    assert converted_fields["data"]["query_builder.public.fact_sales"]["not_for_select"] == []
    assert converted_fields["data"]["query_builder.public.fact_sales"]["where"] == []

    assert len(converted_fields["dimension"]) == 1
    assert "query_builder.public.dim_calendar" in converted_fields["dimension"].keys()
    assert "query_builder.public.dim_calendar.date" in converted_fields["dimension"][
        "query_builder.public.dim_calendar"]["select"]
    assert "query_builder.public.dim_calendar.week_no" in converted_fields["dimension"][
        "query_builder.public.dim_calendar"]["select"]
    assert converted_fields["dimension"]["query_builder.public.dim_calendar"]["calculation"] == []
    assert converted_fields["dimension"]["query_builder.public.dim_calendar"]["where"] == []

    assert len(converted_fields["overall_where"]) == 1
    assert converted_fields["overall_where"]["query_builder.public.dim_calendar.date"]["operator"] == "between"
    assert converted_fields["overall_where"]["query_builder.public.dim_calendar.date"]["condition"] == ['2024-03-25',
                                                                                                        '2024-03-31']


def test_precalculated_field():
    predefined_calculation = {'select': ['query_builder.public.dim_calendar.date',
                                         'query_builder.public.dim_calendar.week_no'],
                              'calculation': [{'query_builder.public.fact_sales.value': 'sum'},
                                              {'query_builder.public.fact_sales.money': 'count'},
                                              {'query_builder.public.fact_stock.last_day_of_week_pcs': 'PREDEFINED'}],
                              'where': {
                                  'query_builder.public.dim_calendar.date': {'operator': 'between',
                                                                             'condition': ['2024-03-25',
                                                                                           '2024-03-31']}}}

    converter = structure_preparation()

    converted_fields = converter.convert_from_frontend_to_backend(predefined_calculation)

    assert len(converted_fields["data"]) == 2
    assert converted_fields["data"]["query_builder.public.fact_stock"]["calculation"] == [
        'sum(query_builder.public.fact_stock.value)']

    assert converted_fields["data"]["query_builder.public.fact_stock"]["where"] == [
        'query_builder.public.dim_calendar.last_day_of_week = 1']

    print(converted_fields)


if __name__ == '__main__':
    test_precalculated_field()
