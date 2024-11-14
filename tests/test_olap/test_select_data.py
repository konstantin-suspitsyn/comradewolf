from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_prompt_converter_service import OlapPromptConverterService
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapFrontend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder
from tests.test_olap.test_frontend_data import base_table_with_join_no_where_no_calc, base_table_with_join_no_where, \
    group_by_read_no_where, base_table_with_join_wth_where, one_dimension_count, group_by_also_in_agg
from tests.test_olap.test_short_tables_collection import DIM_GAMES

G_BY_Y_YM = "olap_test.games_olap.g_by_y_ym"

BASE_SALES = "olap_test.games_olap.base_sales"

olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(get_olap_games_folder())
olap_select_builder = OlapPostgresSelectBuilder()
olap_service: OlapService = OlapService(olap_select_builder)
postgres_query_generator = OlapPostgresSelectBuilder()
olap_select_builder = OlapPostgresSelectBuilder()
olap_prompt_service: OlapPromptConverterService = OlapPromptConverterService(postgres_query_generator)
frontend_all_items_view: OlapFrontend = olap_structure_generator.frontend_fields

def test_should_be_only_base_table_no_group_by() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_no_where_no_calc, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert len(s) == 1
    assert "base_sales.year_f" in s.get_sql("olap_test.games_olap.base_sales")
    assert "base_sales.pcs_f" in s.get_sql("olap_test.games_olap.base_sales")
    assert "olap_test.games_olap.base_sales" in s.get_sql("olap_test.games_olap.base_sales")
    assert "GROUP" not in s.get_sql("olap_test.games_olap.base_sales")
    assert not s.get_has_group_by("olap_test.games_olap.base_sales")
    assert "JOIN" not in s.get_sql("olap_test.games_olap.base_sales")


def test_base_table_with_join_no_where() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_no_where, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert len(s) == 1
    assert BASE_SALES in s
    assert BASE_SALES in s.get_sql(BASE_SALES)
    assert "base_sales.year_f" in s.get_sql(BASE_SALES)
    assert "base_sales.pcs_f" in s.get_sql(BASE_SALES)
    assert "dim_game.bk_game_id_f" in s.get_sql(BASE_SALES)

    assert s.get_sql(BASE_SALES).count("base_sales.year_f") == 2
    assert s.get_sql(BASE_SALES).count("base_sales.pcs_f") == 3
    assert s.get_sql(BASE_SALES).count("dim_game.bk_game_id_f") == 2

    assert "achievements__sum" in s.get_sql(BASE_SALES)
    assert "pcs__sum" in s.get_sql(BASE_SALES)
    assert "price__sum" in s.get_sql(BASE_SALES)
    assert "FROM olap_test.games_olap.base_sales" in s.get_sql(BASE_SALES)
    assert "sum(base_sales.price_f)" in s.get_sql(BASE_SALES)
    assert "sum(base_sales.pcs_f)" in s.get_sql(BASE_SALES)

    assert "GROUP" in s.get_sql(BASE_SALES)
    assert s.get_has_group_by(BASE_SALES)
    assert "JOIN" in s.get_sql(BASE_SALES)
    assert "olap_test.games_olap.dim_game " in s.get_sql(BASE_SALES)
    assert "ON base_sales.sk_id_game_f = dim_game.sk_id_game_f" in s.get_sql(BASE_SALES)


def test_group_by_read_no_where() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        group_by_read_no_where, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert len(s) == 2

    assert G_BY_Y_YM in s
    assert BASE_SALES in s

    for t in [G_BY_Y_YM, BASE_SALES]:
        assert '"year"' in s.get_sql(t)
        assert '"yearmonth"' in s.get_sql(t)
        assert '"price__avg"' in s.get_sql(t)
        assert '"pcs__sum"' in s.get_sql(t)

    assert "GROUP" in s.get_sql(BASE_SALES)
    assert s.get_has_group_by(BASE_SALES)
    assert "FROM olap_test.games_olap.base_sales" in s.get_sql(BASE_SALES)
    assert "avg(" in s.get_sql(BASE_SALES)
    assert "sum(" in s.get_sql(BASE_SALES)

    assert "GROUP" not in s.get_sql(G_BY_Y_YM)
    assert not s.get_has_group_by(G_BY_Y_YM)
    assert BASE_SALES not in s.get_sql(G_BY_Y_YM)
    assert "g_by_y_ym.avg_price_f" in s.get_sql(G_BY_Y_YM)
    assert "g_by_y_ym.yearmonth_f" in s.get_sql(G_BY_Y_YM)
    assert "FROM olap_test.games_olap.g_by_y_ym" in s.get_sql(G_BY_Y_YM)


def test_base_table_with_join_wth_where() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        base_table_with_join_wth_where, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert '"year"' in s.get_sql(BASE_SALES)
    assert '"pcs"' in s.get_sql(BASE_SALES)
    assert '"achievements__sum"' in s.get_sql(BASE_SALES)
    assert '"pcs__sum"' in s.get_sql(BASE_SALES)
    assert '"price__sum"' in s.get_sql(BASE_SALES)
    assert '"bk_id_game"' in s.get_sql(BASE_SALES)

    assert "INNER JOIN " in s.get_sql(BASE_SALES)
    assert "olap_test.games_olap.dim_game" in s.get_sql(BASE_SALES)
    assert "WHERE" in s.get_sql(BASE_SALES)
    assert "base_sales.release_date_f > '2024-01-01'" in s.get_sql(BASE_SALES)
    assert "base_sales.price_f > 1000" in s.get_sql(BASE_SALES)
    assert "dim_game.game_name = 'The Best Game'" in s.get_sql(BASE_SALES)

    assert s.get_sql(BASE_SALES).count("base_sales.year_f") == 2
    assert s.get_sql(BASE_SALES).count("base_sales.pcs_f") == 3
    assert s.get_sql(BASE_SALES).count("dim_game.bk_game_id_f") == 2


def test_one_dimension_count() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        one_dimension_count, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert "bk_id_game__count" in s.get_sql("olap_test.games_olap.dim_game")
    assert len(s) == 1

    assert '"bk_id_game"' in s.get_sql(DIM_GAMES)
    assert '"bk_id_game__count"' in s.get_sql(DIM_GAMES)
    assert '"bk_id_game__count"' in s.get_sql(DIM_GAMES)
    assert 'FROM olap_test.games_olap.dim_game' in s.get_sql(DIM_GAMES)
    assert 'GROUP' in s.get_sql(DIM_GAMES)


def test_group_by_also_in_agg() -> None:
    # Поля, которые есть только в базовой таблице без group by
    frontend_to_backend_type: OlapFrontendToBackend = olap_prompt_service.create_frontend_to_backend(
        group_by_also_in_agg, frontend_all_items_view)

    s = olap_service.select_data(frontend_to_backend_type, olap_structure_generator.get_tables_collection())

    assert len(s) == 5

    assert "GROUP" not in s.get_sql("olap_test.games_olap.g_by_y")


if __name__ == "__main__":
    test_should_be_only_base_table_no_group_by()
    # test_base_table_with_join_no_where()
    # test_group_by_read_no_where()
    # test_base_table_with_join_wth_where()
    # test_one_dimension_count()
    # test_group_by_also_in_agg()
