from comradewolf.universe.olap_language_select_builders import OlapSelectBuilder
from comradewolf.utils.enums_and_field_dicts import OlapCalculations
from comradewolf.utils.exceptions import OlapException
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapTablesCollection, \
    ShortTablesCollectionForSelect
from comradewolf.utils.utils import create_field_with_calculation

NO_FACT_TABLES = "No fact tables"

MANY_DIMENSION_TABLES_ERR = ("Two or more dimension tables are without fact table are in query. There is no way to "
                             "join them")

FIELD_NAME_WITH_ALIAS = '{} as "{}"'


class OlapService:
    """
    Olap service
    Receives data from frontend and returns SQL-script
    """

    def __init__(self, olap_select_builder: OlapSelectBuilder):
        self.olap_select_builder = olap_select_builder

    def generate_pre_select_collection(self, frontend_fields: OlapFrontendToBackend,
                                       tables_collection: OlapTablesCollection) -> ShortTablesCollectionForSelect:
        """
        Generates pre-select collection to create SQL query from
        :param frontend_fields: Comes from frontend. Should be converted to OlapFrontendToBackend
        :param tables_collection: Olap tables collection that contains OlapFrontendToBackend fields
        :return: ShortTablesCollectionForSelect
        """

        short_tables_collection: ShortTablesCollectionForSelect = ShortTablesCollectionForSelect()

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

        list_of_fact_tables: list = list(short_tables_collection.keys())

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
                if (current_calculation in [OlapCalculations.COUNT.value, OlapCalculations.COUNT_DISTINCT.value]) & \
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

                if (not can_use_sk) & (dimension_fields is not None):
                    # TODO: это новое, все проверить. Где и что
                    backend_service_key_fact: str = tables_collection.get_backend_field_name(table, sk)
                    backend_service_key_dimension: str = tables_collection.get_backend_field_name(dimension_table, sk)

                    short_tables_collection, add_dimension = \
                        self.add_join_calculation(current_field_name, current_calculation, table, dimension_table,
                                                  sk, backend_service_key_dimension, backend_service_key_fact,
                                                  short_tables_collection, tables_collection)

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
            field_name_alias_with_calc = tables_collection.get_backend_field_name(table_name, current_field_name)

            short_tables_collection.add_aggregation_field(table_name, current_calculation,
                                                          current_field_name, current_calculation,
                                                          field_name_alias_with_calc)

            added_dimension = True

            return short_tables_collection, added_dimension

        # Field was calculated
        if has_ready_calculation:
            if len(short_tables_collection[table_name]["all_selects"]) == 0:
                alias_backend_name = create_field_with_calculation(current_field_name, current_calculation)

                backend_name: str = tables_collection.get_backend_field_name(table_name, alias_backend_name)

                short_tables_collection.add_select_field(table_name, alias_backend_name, backend_name,
                                                         current_calculation)
                added_dimension = True
                return short_tables_collection, added_dimension

            further_calculation: str | None = tables_collection.get_data_table_further_calculation(table_name,
                                                                                                   current_field_name,
                                                                                                   current_calculation)

            # You can NOT aggregate aggregated field
            if further_calculation is None:
                return short_tables_collection, added_dimension

            field_name_alias_with_calc = tables_collection.get_backend_field_name(table_name, current_field_name)

            if field_name_alias_with_calc is None:
                field_name_alias_with_calc = tables_collection \
                    .get_backend_field_name(table_name, create_field_with_calculation(current_field_name,
                                                                                      current_calculation))

            # You can aggregate aggregated field
            short_tables_collection.add_aggregation_field(table_name, current_calculation,
                                                          current_field_name, current_calculation,
                                                          field_name_alias_with_calc)

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
                    backend_name: str = tables_collection.get_backend_field_name(fact_table_name, current_field)
                    if is_where is False:
                        table_collection_with_select.add_select_field(fact_table_name, current_field, backend_name)
                    else:
                        table_collection_with_select.add_where(fact_table_name, backend_name,
                                                               front_field_dict, )
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

                service_key_dimension_table: str = tables_collection.get_backend_field_name(join_table_name,
                                                                                            service_key)
                service_key_fact_table: str = tables_collection.get_backend_field_name(fact_table_name, service_key)

                current_backend_field: str = tables_collection.get_backend_field_name(join_table_name, current_field)

                if is_where is False:
                    table_collection_with_select \
                        .add_join_field_for_select(table_name=fact_table_name,
                                                   field_alias_name=current_field,
                                                   backend_field=current_backend_field,
                                                   join_table_name=join_table_name,
                                                   service_key_dimension_table=service_key_dimension_table,
                                                   service_key_fact_table=service_key_fact_table,
                                                   service_key_alias=service_key)
                else:
                    table_collection_with_select \
                        .add_where_with_join(table_name=fact_table_name,
                                             backend_field=current_backend_field,
                                             join_table_name=join_table_name,
                                             condition=front_field_dict,
                                             service_key_dimension_table=service_key_dimension_table,
                                             service_key_fact_table=service_key_fact_table, )

        for delete_table in tables_to_delete_from_short_collection:
            del table_collection_with_select[delete_table]

        return table_collection_with_select

    @staticmethod
    def add_join_calculation(current_field_name: str, current_calculation: str, table_name: str, join_table: str,
                             service_key: str, service_key_dimension: str, service_key_fact: str,
                             short_tables_collection: ShortTablesCollectionForSelect,
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
        backend_field = table_collection.get_backend_field_name(join_table, current_field_name)

        short_tables_collection.add_join_field_for_aggregation(table_name, current_field_name, current_calculation,
                                                               join_table, service_key, service_key_dimension,
                                                               service_key_fact, backend_field)

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

            select_list, select_for_group_by, joins, where, has_calculation = self \
                .generate_structure_for_each_piece_of_join(short_tables_collection, table)

            not_selected_fields_no = len(short_tables_collection.get_all_selects(table))

            sql = self.generate_select_query(select_list, select_for_group_by, joins, where, has_calculation, table,
                                             not_selected_fields_no)

            temp_structure[table] = {}
            temp_structure[table]["sql"] = sql
            temp_structure[table]["not_selected_fields_no"] = not_selected_fields_no

        return temp_structure

    def generate_structure_for_each_piece_of_join(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                  table: str) -> tuple[list[str], list[str], dict, list[str], bool]:

        """
        :param short_tables_collection:
        :param table:
        :return:
        """

        return self.olap_select_builder.generate_structure_for_each_possible_table(short_tables_collection, table)

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
        return self.olap_select_builder.generate_select_query(select_list, select_for_group_by, joins, where,
                                                              has_calculation, table_name, not_selected_fields_no)

    @staticmethod
    def has_fact_table_fields(frontend_fields: OlapFrontendToBackend, tables_collection: OlapTablesCollection) -> bool:
        """
        Counts how many fact fields are in base_table
        :param frontend_fields:
        :param tables_collection:
        :return:
        """

        has_fact_field: bool = False
        base_table: str | None = None

        for table in tables_collection.get_fact_tables_collection():

            table_fields: dict = tables_collection.get_fact_table_fields(table)

            for field in table_fields:
                if table_fields[field]["calculation_type"] is not None:
                    continue

            base_table = table

        if base_table is None:
            raise OlapException("No base table")

        frontend_fields_base_table: list = tables_collection.get_frontend_fields(base_table)

        for field in frontend_fields.get_select():
            if field["field_name"] in frontend_fields_base_table:
                return True

        for field in frontend_fields.get_calculation():
            if field["field_name"] in frontend_fields_base_table:
                return True

        for field in frontend_fields.get_where():
            if field["field_name"] in frontend_fields_base_table:
                return True

        return has_fact_field

    def generate_pre_select_for_dimension_only(self, frontend_fields: OlapFrontendToBackend,
                                               tables_collection: OlapTablesCollection) \
            -> tuple[str | None, list[str], list[str], list[str], bool]:
        short_tables_collection: ShortTablesCollectionForSelect = ShortTablesCollectionForSelect()

        def get_table_name_by_field(current_table_name_: str, table_name_: str) -> tuple[str, str]:
            if table_name_ is None:
                table_name_ = current_table_name_
            else:
                if table_name_ != current_table_name_:
                    raise OlapException(MANY_DIMENSION_TABLES_ERR)

            short_table_name_ = table_name_.split(".")[-1]

            return table_name_, short_table_name_

        table_name: str | None = None
        short_table_name: str

        select_list: list[str] = []
        # Fields to put after group by. Separate by comma
        select_for_group_by: list[str] = []
        # All field should be inner joined
        # Structure {join_table_name: sk}
        where: list[str] = []
        # Has calculation
        has_calculation: bool = False

        for field in frontend_fields.get_select():
            current_table_name: str = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            table_name, short_table_name = get_table_name_by_field(current_table_name, table_name)
            backend_name: str = "{}.{}" \
                .format(short_table_name, tables_collection.get_backend_field_name(table_name, field["field_name"]))
            current_backend_name = FIELD_NAME_WITH_ALIAS.format(f"{short_tables_collection}.{backend_name}",
                                                                field["field_name"])
            select_list.append(current_backend_name)
            select_for_group_by.append(current_backend_name)

        for field in frontend_fields.get_calculation():
            current_table_name: str = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            table_name, short_table_name = get_table_name_by_field(current_table_name, table_name)
            backend_name: str = "{}({}.{})" \
                .format(field["calculation"], short_table_name,
                        tables_collection.get_backend_field_name(table_name, field["field_name"]))
            select_list.append(FIELD_NAME_WITH_ALIAS.format(f"{short_tables_collection}.{backend_name}",
                                                            field["field_name"]))

            has_calculation = True

        for field in frontend_fields.get_where():
            current_table_name: str = tables_collection.get_dimension_table_with_field(field["field_name"])[0]
            table_name, short_table_name = get_table_name_by_field(current_table_name, table_name)
            backend_name: str = f"{short_table_name}.{field['field_name']}"
            where.append("{} {} {}".format(backend_name, field["where"], field["condition"]))

        return table_name, select_list, select_for_group_by, where, has_calculation
