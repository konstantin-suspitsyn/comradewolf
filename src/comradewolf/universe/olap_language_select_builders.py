from abc import ABC, abstractmethod

from comradewolf.utils.olap_data_types import ShortTablesCollectionForSelect
from comradewolf.utils.utils import create_field_with_calculation

FIELD_NAME_WITH_ALIAS = '{} as "{}"'

SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP BY"
INNER_JOIN = "INNER_JOIN"
FROM = "FROM"


class OlapSelectBuilder(ABC):
    """
    Base abstract class to create select query
    """

    @abstractmethod
    def generate_select_query(self, select_list: list, select_for_group_by: list, joins: dict, where: list,
                              has_calculation: bool, table_name: str, not_selected_fields_no: int) -> str:
        """
        Generates select statement ready for database query
        All parameters come from self.generate_structure_for_each_piece_of_join()
        :param has_calculation: If true, our select needs GROUP BY with select_for_group_by
        :param not_selected_fields_no:
        :param table_name: table name for FROM
        :param select_list: list of select pieces
        :param select_for_group_by: if there is any calculation we need to use this list in group by
        :param joins: tables to be joined
        :param where: list of where conditions
        :return: select statement
        """
        pass

    @abstractmethod
    def generate_structure_for_each_possible_table(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                   table_name: str) \
            -> tuple[list[str], list[str], dict, list[str], bool]:
        """
        Generates set of variables for self.generate_select_query()
        :param table_name:
        :param short_tables_collection:
        :return:
        """
        pass


class OlapPostgresSelectBuilder(OlapSelectBuilder):
    def generate_select_query(self, select_list: list, select_for_group_by: list, joins: dict, where: list,
                              has_calculation: bool, table_name: str, not_selected_fields_no: int) -> str:
        sql: str = SELECT
        select_string: str = ""
        join_string: str = ""
        where_string: str = ""
        group_by_string: str = ""

        select_string += "\n\t " + "\n\t,".join(select_list)
        if len(where) > 0:
            where_string += WHERE + " " + " AND ".join(where)

        if len(joins) > 0:
            for table in joins:
                join_string += f"\nINNER JOIN {table} \n\t{joins[table]}"

        if len(select_for_group_by) > 0:
            group_by_string += "\n\t" + "\n\t,".join(select_for_group_by)

        sql += select_string

        sql += f"\n{FROM} {table_name}"

        if len(join_string) > 0:
            sql += f"\n{join_string}"

        if len(where) > 0:
            sql += f"\n{where_string}"

        if len(group_by_string) > 0:
            sql += f"\n{GROUP_BY}{group_by_string}"

        return sql

    def generate_structure_for_each_possible_table(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                   table_name: str) \
            -> tuple[list[str], list[str], dict, list[str], bool]:
        # alias table name
        short_table_name: str = table_name.split(".")[-1]
        # Fields to put after select. Separate by comma
        select_list: list[str] = []
        # Fields to put after group by. Separate by comma
        select_for_group_by: list[str] = []
        # All field should be inner joined
        # Structure {join_table_name: sk}
        joins: dict = {}
        # Add where and put AND between fields
        where: list[str] = []
        # Has calculation
        has_calculation: bool = False

        selects_inner_structure: list = short_tables_collection.get_selects(table_name)
        aggregation_structure: list = short_tables_collection.get_aggregations_without_join(table_name)
        select_join: dict = short_tables_collection.get_join_select(table_name)
        aggregation_join: dict = short_tables_collection.get_aggregation_joins(table_name)
        join_where: dict = short_tables_collection.get_join_where(table_name)
        where_list: dict = short_tables_collection.get_self_where(table_name)

        # Simple selects

        for field in selects_inner_structure:
            backend_name: str = "{}.{}".format(short_table_name, field["backend_field"])
            frontend_name: str = field["frontend_field"]
            if field["frontend_calculation"] is not None:
                frontend_name = create_field_with_calculation(frontend_name, field["frontend_calculation"])
            select_list.append(FIELD_NAME_WITH_ALIAS.format(backend_name, frontend_name))

            if (len(aggregation_structure) > 0) or (len(aggregation_join) > 0):
                select_for_group_by.append(backend_name)

        # Calculations

        for field in aggregation_structure:
            backend_name: str = "{}({}.{})".format(field["backend_calculation"], short_table_name,
                                                   field["backend_field"])
            frontend_name: str = field["frontend_field"]

            has_calculation = True

            select_list.append(FIELD_NAME_WITH_ALIAS.format(backend_name, frontend_name))

        # Join selects

        for join_table_name in select_join:
            short_join_table_name: str = join_table_name.split(".")[-1]

            dimension_service_key: str = select_join[join_table_name]["service_key_dimension_table"]
            fact_service_key: str = select_join[join_table_name]["service_key_fact_table"]

            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, fact_service_key,
                                                          short_join_table_name,
                                                          dimension_service_key)

            for join_field in select_join[join_table_name]["fields"]:
                backend_name: str = "{}.{}".format(short_join_table_name, join_field["backend_field"])
                frontend_name: str = join_field["frontend_field"]

                select_list.append(f"{backend_name} as {frontend_name}")
                select_for_group_by.append(backend_name)

            if join_table_name not in joins:
                joins[join_table_name] = service_join

        # Aggregation join

        for join_table_name in aggregation_join:
            short_join_table_name: str = join_table_name.split(".")[-1]
            dimension_service_key: str = select_join[join_table_name]["service_key_dimension_table"]
            fact_service_key: str = select_join[join_table_name]["service_key_fact_table"]

            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, fact_service_key,
                                                          short_join_table_name,
                                                          dimension_service_key)

            for field in aggregation_join[join_table_name]["fields"]:
                backend_name: str = "{}({}.{})" \
                    .format(field["backend_calculation"],
                            short_join_table_name,
                            field["backend_field"], )
                frontend_name: str = create_field_with_calculation(field["frontend_field"],
                                                                   field["frontend_calculation"])
                # frontend_name = "{}.{}".format(short_join_table_name, frontend_name)

                has_calculation = True

                select_list.append(f"{backend_name} as {frontend_name}")

            if join_table_name not in joins:
                joins[join_table_name] = service_join

        # Where without join

        for where_item in where_list:
            backend_name: str = "{}.{}".format(short_table_name, where_item)
            for where_field in where_list[where_item]:
                where.append("{} {} {}".format(backend_name, where_field["where"], where_field["condition"]))

        # Where with join

        for join_table_name in join_where:
            short_join_table_name: str = join_table_name.split(".")[-1]

            dimension_service_key: str = select_join[join_table_name]["service_key_dimension_table"]
            fact_service_key: str = select_join[join_table_name]["service_key_fact_table"]

            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, fact_service_key,
                                                          short_join_table_name,
                                                          dimension_service_key)

            if join_table_name not in joins:
                joins[join_table_name] = service_join

            for condition in join_where[join_table_name]["conditions"]:
                for field_name in condition:
                    backend_name: str = "{}.{}".format(short_join_table_name, condition[field_name]["field_name"])
                    where.append("{} {} {}".format(backend_name,
                                                   condition[field_name]["where"],
                                                   condition[field_name]["condition"]))

        return select_list, select_for_group_by, joins, where, has_calculation
