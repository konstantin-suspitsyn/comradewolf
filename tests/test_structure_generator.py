from comradewolf.universe.structure_generator import StructureGenerator
from comradewolf.utils.enums_and_field_dicts import TableTypes
from tests.constants_for_testing import get_tables_folder, get_joins_folder, get_standard_filters_folder


def create_generator() -> StructureGenerator:
    table_structure = StructureGenerator(
        get_tables_folder(),
        get_joins_folder(),
        get_standard_filters_folder()
    )
    return table_structure


def test_structure_tables() -> None:
    table_structure = create_generator()
    tables = table_structure.get_tables()

    assert 'query_builder.public.dim_calendar' in tables.keys()
    assert 'query_builder.public.dim_item' in tables.keys()
    assert 'query_builder.public.dim_store' in tables.keys()
    assert 'query_builder.public.dim_warehouse' in tables.keys()
    assert 'query_builder.public.fact_sales' in tables.keys()
    assert 'query_builder.public.fact_stock' in tables.keys()

    assert tables['query_builder.public.dim_calendar'] == TableTypes.DIMENSION.value
    assert tables['query_builder.public.dim_item'] == TableTypes.DIMENSION.value
    assert tables['query_builder.public.dim_store'] == TableTypes.DIMENSION.value
    assert tables['query_builder.public.dim_warehouse'] == TableTypes.DIMENSION.value
    assert tables['query_builder.public.fact_sales'] == TableTypes.DATA.value
    assert tables['query_builder.public.fact_stock'] == TableTypes.DATA.value


def test_structure_fields() -> None:
    table_structure = create_generator()
    fields = table_structure.get_fields()

    list_of_fields = ["query_builder.public.dim_calendar.id",
                      "query_builder.public.dim_calendar.date",
                      "query_builder.public.dim_calendar.week_no",
                      "query_builder.public.dim_calendar.first_day_of_week",
                      "query_builder.public.dim_calendar.last_day_of_week",
                      "query_builder.public.dim_calendar.first_day_of_month",
                      "query_builder.public.dim_calendar.last_day_of_month",
                      "query_builder.public.dim_calendar.month",
                      "query_builder.public.dim_calendar.year",
                      "query_builder.public.dim_item.id",
                      "query_builder.public.dim_item.sk_item_id",
                      "query_builder.public.dim_item.name",
                      "query_builder.public.dim_item.volume",
                      "query_builder.public.dim_item.price",
                      "query_builder.public.dim_store.id",
                      "query_builder.public.dim_store.sk_store_id",
                      "query_builder.public.dim_store.bk_address",
                      "query_builder.public.dim_store.address",
                      "query_builder.public.dim_warehouse.id",
                      "query_builder.public.dim_warehouse.sk_warehouse_id",
                      "query_builder.public.dim_warehouse.address",
                      "query_builder.public.fact_sales.id",
                      "query_builder.public.fact_sales.sk_item_id",
                      "query_builder.public.fact_sales.sk_store_id",
                      "query_builder.public.fact_sales.value",
                      "query_builder.public.fact_sales.money",
                      "query_builder.public.fact_sales.date",
                      "query_builder.public.fact_stock.id",
                      "query_builder.public.fact_stock.sk_item_id",
                      "query_builder.public.fact_stock.sk_warehouse_id",
                      "query_builder.public.fact_stock.value",
                      "query_builder.public.fact_stock.date",
                      "query_builder.public.fact_stock.last_day_of_week_pcs",
                      "query_builder.public.fact_stock.first_day_of_week_pcs",
                      "query_builder.public.fact_stock.last_day_of_week_rub",
                      "query_builder.public.fact_stock.first_day_of_week_rub", ]

    for field in list_of_fields:
        assert field in fields.keys()

    assert fields["query_builder.public.dim_calendar.id"]["show"] is False
    assert fields["query_builder.public.dim_calendar.id"]["field_type"] == "select"

    assert fields["query_builder.public.dim_calendar.date"]["show"] is True
    assert fields["query_builder.public.dim_calendar.date"]["field_type"] == "select"
    assert fields["query_builder.public.dim_calendar.date"]["frontend_name"] == "Дата"
    assert fields["query_builder.public.dim_calendar.date"]["front_field_type"] == "date"
    assert fields["query_builder.public.dim_calendar.date"]["show_group"] == "Календарь"


def test_structure_where():
    table_structure = create_generator()
    where = table_structure.get_where()

    assert "filter_one" in where.keys()
    assert where["filter_one"]["front_name"] == "Первый настроенный фильтр"
    assert where["filter_one"]["query"] == ("query_builder.public.dim_store.store_id = 1 and "
                                            "query_builder.public.dim_item.price > 1000")
    assert "query_builder.public.dim_store.store_id" in where["filter_one"]["fields"]
    assert "query_builder.public.dim_item.price" in where["filter_one"]["fields"]
    assert where["filter_one"]["show_group"] == "Склад"


def test_structure_joins():
    table_structure = create_generator()
    joins = table_structure.get_joins()

    assert len(joins.keys()) == 2
    assert "query_builder.public.fact_sales" in joins.keys()
    assert "query_builder.public.fact_stock" in joins.keys()

    assert joins["query_builder.public.fact_sales"]["query_builder.public.dim_store"]["how"] == "inner"
    assert joins["query_builder.public.fact_sales"]["query_builder.public.dim_store"]["on"]["between_tables"] == ["="]
    assert joins["query_builder.public.fact_sales"]["query_builder.public.dim_store"]["on"]["first_table_on"] == [
        'query_builder.public.fact_sales.sk_store_id']
    assert joins["query_builder.public.fact_sales"]["query_builder.public.dim_store"]["on"]["second_table_on"] == [
        'query_builder.public.dim_store.sk_store_id']
