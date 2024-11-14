from abc import ABC, abstractmethod

from comradewolf.utils.enums_and_field_dicts import OlapDataType, WhereConditionType
from comradewolf.utils.exceptions import OlapException
from comradewolf.utils.olap_data_types import ShortTablesCollectionForSelect, OlapFrontendToBackend, \
    OlapTablesCollection, OlapFrontend
from comradewolf.utils.utils import create_field_with_calculation

FIELD_NAME_WITH_ALIAS = '{} as "{}"'

SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP BY"
INNER_JOIN = "INNER_JOIN"
FROM = "FROM"

MANY_DIMENSION_TABLES_ERR = ("Two or more dimension tables are without fact table are in query. There is no way to "
                             "join them")


class OlapSelectBuilder(ABC):
    """
    Base abstract class to create select query
    """

    @abstractmethod
    def generate_select_query(self, select_list: list, select_for_group_by: list, joins: dict, where: list,
                              has_calculation: bool, table_name: str, order_by: list[str], not_selected_fields_no: int,
                              add_order_by: bool) -> tuple[str, bool]:
        """
        Generates select statement ready for database query
        All parameters come from self.generate_structure_for_each_piece_of_join()
        :param order_by: list of order by fields
        :param has_calculation: If true, our select needs GROUP BY with select_for_group_by
        :param not_selected_fields_no:
        :param table_name: table name for FROM
        :param select_list: list of select pieces
        :param select_for_group_by: if there is any calculation we need to use this list in group by
        :param joins: tables to be joined
        :param where: list of where conditions
        add_order_by
        :return: select statement and bool if it has calculation; Has calculation bool is used for optimizer engine
        """
        pass

    @abstractmethod
    def generate_structure_for_each_fact_table(self, short_tables_collection: ShortTablesCollectionForSelect,
                                               table_name: str) \
            -> tuple[list[str], list[str], dict, list[str], list[str], bool]:
        """
        Generates set of variables for self.generate_select_query()
        :param table_name:
        :param short_tables_collection:
        :return:
        """
        pass

    @abstractmethod
    def generate_structure_for_dimension_table(self, frontend_fields: OlapFrontendToBackend,
                                               tables_collection: OlapTablesCollection) \
            -> tuple[str, list[str], list[str], list[str], bool]:
        """
        Generates set of variables for self.generate_select_query()
        But only if no fact tables in frontend fields
        :param tables_collection:
        :param frontend_fields:
        :return: table_name, select_list, select_for_group_by, where, has_calculation
        """
        pass

    def generate_where_condition(self, field_alias: str, type_of_where: str, front_condition: list | str | float | int,
                                 field_type: str) -> str:
        """
        Generate where based on field type
        :param field_type:
        :param field_alias:
        :param type_of_where:
        :param front_condition:
        :return:
        """
        pass

    @staticmethod
    def get_select_fiter_all(backend_name: str) -> str:
        """
        Returns select for all distinct values
        :param backend_name: field name
        :return:
        """
        pass

    @staticmethod
    def get_select_fiter_max_min(backend_name: str) -> str:
        """
        Returns select for max and min values
        :param backend_name: field name
        :return:
        """
        pass


class OlapPostgresSelectBuilder(OlapSelectBuilder):
    def generate_structure_for_dimension_table(self, frontend_fields: OlapFrontendToBackend,
                                               tables_collection: OlapTablesCollection) \
            -> tuple[str, list[str], list[str], list[str], bool]:
        short_tables_collection: ShortTablesCollectionForSelect = ShortTablesCollectionForSelect()

        short_table_name: str

        select_list: list[str] = []
        # Fields to put after group by. Separate by comma
        select_for_group_by: list[str] = []
        # All field should be inner joined
        # Structure {join_table_name: sk}
        where: list[str] = []
        # Has calculation
        has_calculation: bool = False

        current_table_name: str = ""

        for field in frontend_fields.get_select():
            current_table_name = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            short_table_name = current_table_name.split(".")[-1]
            backend_name: str = "{}.{}" \
                .format(short_table_name, tables_collection.get_backend_field_name(current_table_name, field["field_name"]))
            current_backend_name = FIELD_NAME_WITH_ALIAS.format(f"{backend_name}", field["field_name"])
            select_list.append(current_backend_name)
            select_for_group_by.append(backend_name)

        for field in frontend_fields.get_calculation():
            current_table_name = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            short_table_name = current_table_name.split(".")[-1]
            backend_name: str = "{}({}.{})" \
                .format(field["calculation"], short_table_name,
                        tables_collection.get_backend_field_name(current_table_name, field["field_name"]))

            frontend_name: str = create_field_with_calculation(field["field_name"], field["calculation"])

            select_list.append(FIELD_NAME_WITH_ALIAS.format(f"{backend_name}", frontend_name))

            has_calculation = True

        for field in frontend_fields.get_where():
            current_table_name = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            short_table_name = current_table_name.split(".")[-1]
            backend_field_name: str = tables_collection.get_backend_field_name(current_table_name, field["field_name"])
            backend_name: str = f"{short_table_name}.{backend_field_name}"
            where.append("{} {} {}".format(backend_name, field["where"], field["condition"]))

        return current_table_name, select_list, select_for_group_by, where, has_calculation

    def generate_select_query(self, select_list: list, select_for_group_by: list, joins: dict, where: list,
                              has_calculation: bool, table_name: str, order_by: list[str], not_selected_fields_no: int,
                              add_order_by: bool)  -> tuple[str, bool]:
        sql: str = SELECT
        select_string: str = ""
        join_string: str = ""
        where_string: str = ""
        group_by_string: str = ""

        has_group_by: bool = False

        select_string += "\n\t " + "\n\t,".join(select_list)
        if len(where) > 0:
            where_string += WHERE + " " + "\n\tAND ".join(where)

        if len(joins) > 0:
            for table in joins:
                join_string += f"\nINNER JOIN {table} \n\t{joins[table]}"

        if len(select_for_group_by) > 0:
            group_by_string += "\n\t " + "\n\t,".join(select_for_group_by)

        sql += select_string

        sql += f"\n{FROM} {table_name}"

        if len(join_string) > 0:
            sql += f"\n{join_string}"

        if len(where) > 0:
            sql += f"\n{where_string}"

        if len(group_by_string) > 0:
            sql += f"\n{GROUP_BY}{group_by_string}"
            has_group_by = True
        if add_order_by:
            order_by_string = ", ".join(order_by)
            sql += f"\nORDER BY {order_by_string}"

        return sql, has_group_by

    def generate_structure_for_each_fact_table(self, short_tables_collection: ShortTablesCollectionForSelect,
                                               table_name: str) \
            -> tuple[list[str], list[str], dict, list[str], list[str], bool]:
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
        # Order by list
        order_by: list[str] = []

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

            select_list.append(FIELD_NAME_WITH_ALIAS.format(backend_name, frontend_name))
            order_by.append(backend_name)

            if (len(aggregation_structure) > 0) or (len(aggregation_join) > 0):
                select_for_group_by.append(backend_name)

        # Calculations

        for field in aggregation_structure:
            backend_name: str = "{}({}.{})".format(field["backend_calculation"], short_table_name,
                                                   field["backend_field"])
            frontend_name: str = field["frontend_field"]
            if field["frontend_calculation"] is not None:
                frontend_name = create_field_with_calculation(frontend_name, field["frontend_calculation"])

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

                select_list.append(f"{backend_name} as \"{frontend_name}\"")
                if (len(aggregation_structure) > 0) or (len(aggregation_join) > 0):
                    select_for_group_by.append(backend_name)

            if join_table_name not in joins:
                joins[join_table_name] = service_join

        # Aggregation join

        for join_table_name in aggregation_join:
            short_join_table_name: str = join_table_name.split(".")[-1]
            dimension_service_key: str = aggregation_join[join_table_name]["service_key_dimension_table"]
            fact_service_key: str = aggregation_join[join_table_name]["service_key_fact_table"]

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

            dimension_service_key: str = join_where[join_table_name]["service_key_dimension_table"]
            fact_service_key: str = join_where[join_table_name]["service_key_fact_table"]

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

        return select_list, select_for_group_by, joins, where, order_by, has_calculation

    def generate_where_condition(self, field_alias: str, type_of_where: str, front_condition: list | str | float | int,
                                 data_type: str) -> str:
        """
        Generate where based on field type
        :param data_type:
        :param field_alias:
        :param type_of_where:
        :param front_condition:
        :return:
        """
        condition = ""

        type_of_where = type_of_where.upper()

        all_where_types = [e.value for e in WhereConditionType]

        if type_of_where not in all_where_types:
            raise OlapException(f"Check your where condition. {type_of_where} not in {','.join(all_where_types)}")

        current_placeholder: str

        string_placeholder = r"'{}'"
        date_placeholder = r"'{}'"
        in_placeholder = "({})"
        number_placeholder = "{}"
        and_placeholder = "{} AND {}"

        if data_type == OlapDataType.DATE.value:
            current_placeholder = date_placeholder
        elif data_type == OlapDataType.NUMBER.value:
            current_placeholder = number_placeholder
        elif data_type == OlapDataType.TEXT.value:
            current_placeholder = string_placeholder
        elif data_type == OlapDataType.DATE_TIME.value:
            current_placeholder = date_placeholder
        else:
            raise OlapException(f"Field type {data_type} is unknown")

        if type_of_where == WhereConditionType.BETWEEN.value:
            if not isinstance(front_condition, list):
                raise OlapException("front_condition should be list")

            if len(front_condition) != 2:
                raise OlapException("front_condition should have 2 elements")

            condition = and_placeholder.format(current_placeholder.format(front_condition[0]),
                                               current_placeholder.format(front_condition[1]))
        elif type_of_where in [WhereConditionType.IN.value, WhereConditionType.NOT_IN.value]:
            if isinstance(front_condition, list):
                temp_condition = []
                for item in front_condition:
                    temp_condition.append(current_placeholder.format(item))
                condition = in_placeholder.format(",".join(temp_condition))
        else:
            if isinstance(front_condition, list):
                OlapException("List instead of str or int or float")
            condition = current_placeholder.format(front_condition)

        return condition

    @staticmethod
    def get_select_fiter_all(backend_name: str) -> str:
        """
        Returns select for all distinct values
        :param backend_name: field name
        :return:
        """
        return f"SELECT DISTINCT {backend_name}"

    @staticmethod
    def get_select_fiter_max_min(backend_name: str) -> str:
        """
        Returns select for max and min values
        :param backend_name: field name
        :return:
        """
        return f"SELECT MIN({backend_name}) as min_value, MAX({backend_name}) as max_value"
