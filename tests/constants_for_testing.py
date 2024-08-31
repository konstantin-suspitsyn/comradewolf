import os

testing_directory: str = os.path.dirname(__file__)
test_db_structure_path: str = os.path.join(testing_directory, r"test_db_structure")
test_olap_structure_path: str = os.path.join(testing_directory, r"test_olap_structure")


def get_empty_folder() -> str:
    """Empty folder path"""
    return os.path.join(test_db_structure_path, r"empty_directory")


def get_tables_folder() -> str:
    """Table toml folder path"""
    return os.path.join(test_db_structure_path, r"test_tables")


def get_repeated_tables_folder() -> str:
    """Table toml folder path with toml with repeated table names"""
    return os.path.join(test_db_structure_path, r"tables_duplicate_key")


def get_joins_folder() -> str:
    """
    Table toml folder path with joins
    """
    return os.path.join(test_db_structure_path, r"joins_test_structure_generator")


def get_standard_filters_folder() -> str:
    """Table toml folder path with standard filters"""
    return os.path.join(test_db_structure_path, r"standard_filters_generator")


def get_olap_games_folder() -> str:
    """
    Table toml folder path with olap games
    """
    return os.path.join(test_olap_structure_path, r"olap_games")
