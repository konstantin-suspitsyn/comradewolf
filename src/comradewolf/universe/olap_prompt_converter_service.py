from comradewolf.universe.olap_language_select_builders import OlapSelectBuilder
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapFrontend


class OlapPromptConverterService:
    """
    Converts dictionary that came from frontend to OlapFrontendToBackend.class
    """

    olap_select_builder: OlapSelectBuilder

    def __init__(self, olap_select_query_builder: OlapSelectBuilder):
        self.olap_select_builder = olap_select_query_builder

    def create_frontend_to_backend(self, frontend_dictionary: dict, all_fields: OlapFrontend) -> OlapFrontendToBackend:
        """
        Generates structure suitable for backend from frontend data
        :param frontend_dictionary: frontend data
        :param all_fields: all fields with data types
        :return: OlapFrontendToBackend
        """
        frontend_to_backend = OlapFrontendToBackend()

        if "SELECT" in frontend_dictionary.keys():
            frontend_to_backend.add_select(frontend_dictionary["SELECT"])

        if "CALCULATION" in frontend_dictionary.keys():
            frontend_to_backend.add_calculation(frontend_dictionary["CALCULATION"])

        backend_where: list[dict] = []

        if "WHERE" in frontend_dictionary.keys():
            for item in frontend_dictionary["WHERE"]:
                field_alias: str = item["field_name"]
                type_of_where: str = item["where"]
                front_condition: list | str | float | int = item["condition"]
                field_type: str = all_fields.get_data_type(field_alias)
                condition: str = self.olap_select_builder.generate_where_condition(field_alias, type_of_where,
                                                                                   front_condition, field_type)
                backend_where.append({"field_name": field_alias, "where": type_of_where, 'condition': condition})

        frontend_to_backend.add_where(backend_where)

        return frontend_to_backend
