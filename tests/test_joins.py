from comradewolf.universe.joins_generator import GenerateJoins
from comradewolf.universe.possible_joins import AllPossibleJoins
from comradewolf.universe.structure_generator import StructureGenerator
from tests.constants_for_testing import get_tables_folder, get_joins_folder, get_standard_filters_folder


def test_joins():
    table_structure = StructureGenerator(
        get_tables_folder(),
        get_joins_folder(),
        get_standard_filters_folder()
    )
    GenerateJoins(table_structure.get_joins(), table_structure.get_tables())
    possible_joins = AllPossibleJoins()
    joins: AllPossibleJoins = possible_joins.get_all_joins()

    assert len(joins.keys()) == 2
    assert "query_builder.public.fact_sales" in joins.keys()
    assert "query_builder.public.fact_stock" in joins.keys()

    assert len(joins["query_builder.public.fact_sales"].keys()) == 3
    assert "query_builder.public.dim_item" in joins["query_builder.public.fact_sales"].keys()
    assert "query_builder.public.dim_store" in joins["query_builder.public.fact_sales"].keys()
    assert "query_builder.public.dim_calendar" in joins["query_builder.public.fact_sales"].keys()

    assert joins.has_join("query_builder.public.fact_sales", "query_builder.public.dim_calendar") is True
    assert joins.has_join("query_builder.public.fact_sales", "no_table") is False

    assert joins.has_table_with_joins("query_builder.public.fact_sales") is True
    assert joins.has_table_with_joins("no_table") is False
