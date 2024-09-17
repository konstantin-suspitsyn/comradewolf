from comradewolf.universe.olap_language_select_builders import OlapPostgresSelectBuilder
from comradewolf.universe.olap_service import OlapService
from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, ShortTablesCollectionForSelect
from tests.constants_for_testing import get_olap_games_folder


def create_short_select_collection_to_test(path_to_folder: str, from_frontend: dict) -> ShortTablesCollectionForSelect:
    """
    Returns ShortTablesCollectionForSelect for olap tables and dict from frontend
    :param path_to_folder: path to toml files with olap information
    :param from_frontend: dictionary with same view that comes from frontend
    :return: ShortTablesCollectionForSelect
    """
    frontend_to_backend_type, olap_service, olap_structure_generator = create_olap_service_structure(from_frontend,
                                                                                                     path_to_folder)

    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    return short_table_only_base


def create_olap_service_structure(from_frontend, path_to_folder):
    olap_structure_generator: OlapStructureGenerator = OlapStructureGenerator(path_to_folder)
    frontend_to_backend_type: OlapFrontendToBackend = OlapFrontendToBackend(from_frontend)
    olap_select_builder = OlapPostgresSelectBuilder()
    olap_service: OlapService = OlapService(olap_select_builder)
    return frontend_to_backend_type, olap_service, olap_structure_generator


def short_select_collection_games_test(from_frontend: dict) -> ShortTablesCollectionForSelect:
    """
    Returns ShortTablesCollectionForSelect for Games olap tables and dict from frontend
    :param from_frontend: dictionary with same view that comes from frontend
    :return: ShortTablesCollectionForSelect
    """
    return create_short_select_collection_to_test(get_olap_games_folder(), from_frontend)


def generate_structure_for_each_piece_of_join_test(path_to_folder: str, from_frontend: dict, table_name: str):
    frontend_to_backend_type, olap_service, olap_structure_generator = create_olap_service_structure(from_frontend,
                                                                                                     path_to_folder)
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    return olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, table_name)


def generate_structure_for_each_piece_of_join_test_games(from_frontend: dict, table_name: str):
    frontend_to_backend_type, olap_service, olap_structure_generator = \
        create_olap_service_structure(from_frontend, get_olap_games_folder())
    short_table_only_base: ShortTablesCollectionForSelect \
        = olap_service.generate_pre_select_collection(frontend_to_backend_type,
                                                      olap_structure_generator.get_tables_collection())

    return olap_service.generate_structure_for_each_piece_of_join(short_table_only_base, table_name)
