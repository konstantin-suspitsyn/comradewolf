from collections import UserDict

from comradewolf.universe.olap_structure_generator import OlapStructureGenerator
from comradewolf.utils.olap_data_types import OlapFrontendToBackend

SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP BY"
INNER_JOIN = "INNER_JOIN"


class OlapService:
    """
    Olap service
    Receives data from frontend and returns SQL-script
    """
    frontend_field: OlapFrontendToBackend
    olap_structure: OlapStructureGenerator

    def __init__(self, frontend_field: OlapFrontendToBackend, olap_structure: OlapStructureGenerator) -> None:
        """
        :param frontend_field:
        :param olap_structure:
        """
        self.frontend_field = frontend_field
        self.olap_structure = olap_structure

    def generate_select(self) -> str:
        """
        Generates Select from
        :return: SELECT string
        """

        select_fields: dict
        where_fields: dict
        calculation_fields: dict

        select_fields, calculation_fields,  where_fields = self.sort_frontend_into_categories()

        # This is priority list of tables
        all_tables: list = []

        field_name: str = ""
        calculation: str | None = None
        short_tables_list: list = []

        for table in self.olap_structure.get_tables_with_field_and_optional_calculation(field_name, calculation, short_tables_list):
            all_tables

        for calc_field_dict in calculation_fields["fact"]:
            for field in calc_field_dict.keys():
                pass




        sql: str = ""

        return sql

    def sort_frontend_into_categories(self) -> tuple[dict, dict, dict]:
        """
        Takes frontend data and sorts it into dimension and fact tables
        :return:
        """
        select_fields: dict = {"fact": [], "dimension": []}
        where_fields: dict = {"fact": [], "dimension": []}
        calculation_fields: dict = {"fact": [], "dimension": []}

        # Generate all fields for select only
        for field_from_frontend in self.frontend_field.get_select():
            if field_from_frontend["tableName"] in self.olap_structure.get_dimension_table_list():
                select_fields["dimension"].append(field_from_frontend["fieldName"])
            else:
                select_fields["fact"].append(field_from_frontend["fieldName"])
        # Generate fields for calculation
        for field_from_frontend in self.frontend_field.get_calculation():
            if field_from_frontend["tableName"] in self.olap_structure.get_dimension_table_list():
                calculation_fields["dimension"].append({field_from_frontend["fieldName"]: field_from_frontend[
                    "calculation"]})
            else:
                calculation_fields["fact"].append({field_from_frontend["fieldName"]: field_from_frontend[
                    "calculation"]})
        # Generate where
        for field_from_frontend in self.frontend_field.get_where():
            if field_from_frontend["tableName"] in self.olap_structure.get_dimension_table_list():
                where_fields["dimension"].append(field_from_frontend)
            else:
                where_fields["fact"].append(field_from_frontend)

        return select_fields, calculation_fields, where_fields
