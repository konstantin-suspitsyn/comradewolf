from comradewolf.utils.enums_and_field_dicts import OlapCalculations
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapTablesCollection, \
    ShortTablesCollectionForSelect
from comradewolf.utils.utils import create_field_with_calculation

FIELD_NAME_WITH_ALIAS = '{} as "{}"'

SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP BY"
INNER_JOIN = "INNER_JOIN"
FROM = "FROM"


class OlapService:
    """
    Olap service
    Receives data from frontend and returns SQL-script
    """

    def generate_pre_select_collection(self, frontend_fields: OlapFrontendToBackend,
                                       tables_collection: OlapTablesCollection) -> ShortTablesCollectionForSelect:
        """
        Generates pre-select collection to create SQL query from
        :param frontend_fields: Comes from frontend. Should be converted to OlapFrontendToBackend
        :param tables_collection: Olap tables collection that contains OlapFrontendToBackend fields
        :return: ShortTablesCollectionForSelect
        """

        short_tables_collection = ShortTablesCollectionForSelect()

        # Filling ShortTablesCollectionForSelect with data
        short_tables_collection.generate_complete_structure(tables_collection.get_fact_tables_collection())

        # Exclude tables than don't have necessary fields and rebuild structure
        short_tables_collection = \
            self.add_select_fields_to_short_tables_collection(short_tables_collection, frontend_fields.get_select(),
                                                              tables_collection)

        short_tables_collection = \
            self.add_select_fields_to_short_tables_collection(short_tables_collection, frontend_fields.get_where(),
                                                              tables_collection, True)

        short_tables_collection = \
            self.add_calculation_fields_to_short_tables_collection(short_tables_collection,
                                                                   frontend_fields.get_calculation(),
                                                                   tables_collection)

        return short_tables_collection

    def add_calculation_fields_to_short_tables_collection(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                          calculations: list,
                                                          tables_collection: OlapTablesCollection) \
            -> ShortTablesCollectionForSelect:
        """
        Working with calculations from frontend fields
        It adds calculation structure (with and without join) to ShortTablesCollectionForSelect and deletes
        not suitable tables from ShortTablesCollectionForSelect
        :param short_tables_collection: ShortTablesCollectionForSelect
        :param calculations: list of calculations from frontend
        :param tables_collection: OlapTablesCollectionForSelect
        :return: ShortTablesCollectionForSelect but changed
        """

        short_tables_collection: ShortTablesCollectionForSelect = short_tables_collection.copy()

        list_of_fact_tables = list(short_tables_collection.keys())

        # If service key not in table, remove table from table_collection
        tables_to_delete_from_short_collection: list[str] = []

        can_use_sk: bool = False

        for calculation_field in calculations:
            current_field_name = calculation_field["field_name"]
            current_calculation = calculation_field["calculation"]

            dimension_fields: list | None = tables_collection.get_dimension_table_with_field(current_field_name)
            dimension_table: str = ""
            sk: str = ""

            # Check if field in dimension table
            if dimension_fields is not None:

                dimension_table = dimension_fields[0]
                sk = dimension_fields[1]

                # Can we use service key for count
                if (current_calculation in [OlapCalculations.COUNT, OlapCalculations.COUNT_DISTINCT]) & \
                        tables_collection.get_is_sk_for_count(dimension_table, current_field_name):
                    can_use_sk = True

            for table in list_of_fact_tables:

                if table in tables_to_delete_from_short_collection:
                    continue

                add_dimension: bool = False
                add_fact_field: bool
                add_sk_field: bool = False

                if dimension_fields is not None:
                    # Service key not in table
                    if tables_collection.is_field_in_data_table(sk, table, None) \
                            is False:
                        tables_to_delete_from_short_collection.append(table)
                        continue

                if can_use_sk:
                    short_tables_collection, add_sk_field = \
                        self.add_calculation_no_join(sk, current_calculation, table, short_tables_collection,
                                                     tables_collection)

                if add_sk_field:
                    continue

                short_tables_collection, add_fact_field = \
                    self.add_calculation_no_join(current_field_name, current_calculation, table,
                                                 short_tables_collection, tables_collection)

                if add_fact_field:
                    continue

                if not can_use_sk:
                    short_tables_collection, add_dimension = \
                        self.add_join_calculation(current_field_name, current_calculation, table, dimension_table,
                                                  sk, short_tables_collection, tables_collection)

                if add_dimension:
                    continue

                tables_to_delete_from_short_collection.append(table)

        for table in tables_to_delete_from_short_collection:
            del short_tables_collection[table]

        return short_tables_collection

    @staticmethod
    def add_calculation_no_join(current_field_name: str, current_calculation: str, table_name: str,
                                short_tables_collection: ShortTablesCollectionForSelect,
                                tables_collection: OlapTablesCollection) -> tuple[ShortTablesCollectionForSelect, bool]:

        """
        Adds calculation without join if possible
        Field is in table itself
        :param current_field_name: frontend field name
        :param current_calculation: frontend calculation
        :param table_name: backend table name
        :param short_tables_collection: ShortTablesCollectionForSelect
        :param tables_collection: tables from OlapTablesCollection
        :return: tuple[ShortTablesCollectionForSelect, True if dimension was added or False otherwise]
        """

        added_dimension: bool = False
        has_field_no_calculation: bool = tables_collection.is_field_in_data_table(current_field_name, table_name, None)
        has_ready_calculation: bool = tables_collection.is_field_in_data_table(current_field_name, table_name,
                                                                               current_calculation)

        # Field is not presented in table
        if (has_field_no_calculation is False) & (has_ready_calculation is False):
            return short_tables_collection, added_dimension

        # Field was not yet calculated
        if has_ready_calculation is False:
            short_tables_collection.add_aggregation_field(table_name, current_field_name, current_calculation,
                                                          current_field_name, current_calculation)

            added_dimension = True

            return short_tables_collection, added_dimension

        # Field was calculated
        if has_ready_calculation:
            if len(short_tables_collection[table_name]["all_selects"]) == 0:
                short_tables_collection.add_select_field(table_name, current_field_name, tables_collection,
                                                         current_calculation)
                added_dimension = True
                return short_tables_collection, added_dimension

            further_calculation: str | None = tables_collection.get_data_table_further_calculation(table_name,
                                                                                                   current_field_name,
                                                                                                   current_calculation)

            # You can NOT aggregate aggregated field
            if further_calculation is None:
                return short_tables_collection, added_dimension

            # You can aggregate aggregated field
            # TODO CHECK THIS STEP
            short_tables_collection.add_aggregation_field(table_name, current_field_name, current_calculation,
                                                          current_field_name, current_calculation)

            added_dimension = True

        return short_tables_collection, added_dimension

    @staticmethod
    def add_select_fields_to_short_tables_collection(short_tables_collection: ShortTablesCollectionForSelect,
                                                     frontend_fields_select_or_where: list,
                                                     tables_collection: OlapTablesCollection, is_where: bool = False) \
            -> ShortTablesCollectionForSelect:
        """
        Adds select and where structure to short tables collection
        :param is_where: True or False for where
        :param short_tables_collection:
        :param frontend_fields_select_or_where: list of frontend fields of select or where
        :param tables_collection: OlapTablesCollection
        :return: ShortTablesCollectionForSelect
        """

        table_collection_with_select = short_tables_collection.copy()

        list_of_fact_tables = list(table_collection_with_select.keys())

        tables_to_delete_from_short_collection: list[str] = []

        # Iterate through select fields
        for front_field_dict in frontend_fields_select_or_where:

            current_field: str = front_field_dict["field_name"]

            join_table_name: str = ""
            service_key: str = ""

            dimension_table_and_service_key: list | None = tables_collection.get_dimension_table_with_field(
                current_field)

            if dimension_table_and_service_key is not None:
                join_table_name = dimension_table_and_service_key[0]
                service_key = dimension_table_and_service_key[1]

            for fact_table_name in list_of_fact_tables:

                # This table already did not satisfy one of fields
                if fact_table_name in tables_to_delete_from_short_collection:
                    continue

                is_field_in_table: bool = tables_collection.is_field_in_data_table(current_field, fact_table_name, None)

                # Field is in the fact table. Add select or where move to next table in loop
                # Field is not in fact table
                if is_field_in_table:
                    if is_where is False:
                        table_collection_with_select.add_select_field(fact_table_name, current_field, tables_collection)
                    else:
                        table_collection_with_select.add_where(fact_table_name, current_field,
                                                               front_field_dict)
                    continue

                # Not dimension table and not in fact table
                # Just add table to delete later
                if dimension_table_and_service_key is None:
                    tables_to_delete_from_short_collection.append(fact_table_name)
                    continue

                # Checking if service key in fact table
                is_service_key_in_table: bool = tables_collection.is_field_in_data_table(
                    service_key, fact_table_name, None)

                if is_service_key_in_table is False:
                    tables_to_delete_from_short_collection.append(fact_table_name)
                    continue

                # Field is not in fact table, but you can join dimension table
                if is_where is False:
                    table_collection_with_select.add_join_field_for_select(fact_table_name, current_field,
                                                                           join_table_name,
                                                                           service_key,
                                                                           tables_collection)
                else:
                    table_collection_with_select.add_where_with_join(fact_table_name, current_field,
                                                                     join_table_name,
                                                                     service_key,
                                                                     front_field_dict)

        for delete_table in tables_to_delete_from_short_collection:
            del table_collection_with_select[delete_table]

        return table_collection_with_select

    @staticmethod
    def add_join_calculation(current_field_name: str, current_calculation: str, table_name: str, join_table: str,
                             service_key: str, short_tables_collection: ShortTablesCollectionForSelect,
                             table_collection: OlapTablesCollection) -> tuple[ShortTablesCollectionForSelect, bool]:
        """
        Adds calculation with join if possible
        Only works with dimension table calculations
        :param table_collection: OlapTablesCollection needed to get correct field name
        :param current_field_name: frontend field name
        :param current_calculation: frontend calculation
        :param table_name: backend table name
        :param join_table: backend join table
        :param service_key: service key to join dimension and fact table
        :param short_tables_collection: ShortTablesCollectionForSelect
        :return: tuple[ShortTablesCollectionForSelect, True if join was added]
        """
        short_tables_collection.add_join_field_for_aggregation(table_name, current_field_name, current_calculation,
                                                               join_table, service_key, table_collection)

        return short_tables_collection, True

    def generate_selects_from_collection(self, short_tables_collection: ShortTablesCollectionForSelect) -> dict:
        """
        Generates select structure from short tables collection
        :param short_tables_collection: should be created from self.generate_pre_select_collection()
        :return:
        """

        temp_structure: dict = {}

        for table in short_tables_collection:
            # Fields to put after select. Separate by comma
            select_list: list[str]
            # Fields to put after group by. Separate by comma
            select_for_group_by: list[str]
            # All field should be inner joined
            # Structure {join_table_name: sk}
            joins: dict
            # Add where and put AND between fields
            where: list[str]

            select_list, select_for_group_by, joins, where, has_calculation = self\
                .generate_structure_for_each_piece_of_join(short_tables_collection, table)

            not_selected_fields_no = len(short_tables_collection.get_all_selects(table))

            sql = self.generate_select_query(select_list, select_for_group_by, joins, where, has_calculation, table, not_selected_fields_no)

            temp_structure[table] = {}
            temp_structure[table]["sql"] = sql
            temp_structure[table]["not_selected_fields_no"] = not_selected_fields_no

        return temp_structure

    def generate_structure_for_each_piece_of_join(self, short_tables_collection, table):
        # TODO: REFACTOR IT

        # alias table name
        short_table_name: str = table.split(".")[-1]
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

        selects_inner_structure: list = short_tables_collection.get_selects(table)
        aggregation_structure: list = short_tables_collection.get_aggregations_without_join(table)
        select_join: dict = short_tables_collection.get_join_select(table)
        aggregation_join: dict = short_tables_collection.get_aggregation_joins(table)
        join_where: dict = short_tables_collection.get_join_where(table)
        where_list: dict = short_tables_collection.get_self_where(table)

        # Simple selects

        for field in selects_inner_structure:
            backend_name: str = "{}.{}".format(short_table_name, field["backend_field"])
            frontend_name: str = field["frontend_field"]
            if field["frontend_calculation"] is not None:
                frontend_name = create_field_with_calculation(frontend_name, field["frontend_calculation"])
            select_list.append(FIELD_NAME_WITH_ALIAS.format(backend_name, frontend_name))

            if (len(aggregation_structure) > 0) or (len(aggregation_join)>0):
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

            service_key: str = select_join[join_table_name]["service_key"]

            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, service_key, short_join_table_name,
                                                          service_key)

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
            service_key: str = aggregation_join[join_table_name]["service_key"]

            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, service_key, short_join_table_name,
                                                          service_key)

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
            service_key: str = join_where[join_table_name]["service_key"]
            service_join: str = "ON {}.{} = {}.{}".format(short_table_name, service_key, short_join_table_name,
                                                          service_key)

            if join_table_name not in joins:
                joins[join_table_name] = service_join

            for condition in join_where[join_table_name]["conditions"]:
                for field_name in condition:
                    backend_name: str = "{}.{}".format(short_join_table_name, condition[field_name]["field_name"])
                    where.append("{} {} {}".format(backend_name,
                                                   condition[field_name]["where"],
                                                   condition[field_name]["condition"]))

        return select_list, select_for_group_by, joins, where, has_calculation

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
