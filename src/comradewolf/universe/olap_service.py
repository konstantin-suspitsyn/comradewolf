from select import select

from comradewolf.universe.olap_language_select_builders import OlapSelectBuilder
from comradewolf.utils.enums_and_field_dicts import OlapCalculations, OlapFollowingCalculations, FilterTypes
from comradewolf.utils.exceptions import OlapException
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapTablesCollection, \
    ShortTablesCollectionForSelect, TableForFilter, SelectFilter, OlapFilterFrontend, SelectCollection
from comradewolf.utils.utils import create_field_with_calculation

NO_FACT_TABLES = "No fact tables"


class OlapService:
    """
    Olap service
    Receives data from frontend and returns SQL-script
    """

    def __init__(self, olap_select_builder: OlapSelectBuilder):
        self.olap_select_builder = olap_select_builder

    @staticmethod
    def fact_table_in_query(frontend_fields: OlapFrontendToBackend, tables_collection: OlapTablesCollection) -> bool:
        """
        Checks if fact table is in query
        :param tables_collection:
        :param frontend_fields:
        :return:
        """

        all_fact_fields: list[str] = []

        fact_tables = tables_collection.get_fact_tables_collection()

        for fact_table in fact_tables:
            for field in fact_tables[fact_table]["fields"]:
                all_fact_fields.append(field)

        for field in frontend_fields.get_select():
            if field["field_name"] in all_fact_fields:
                return True

        for field in frontend_fields.get_calculation():
            if field["field_name"] in all_fact_fields:
                return True

        for field in frontend_fields.get_where():
            if field["field_name"] in all_fact_fields:
                return True

        return False

    def build_select(self, frontend_fields: OlapFrontendToBackend) -> dict:
        """
        Builds
        :param frontend_fields:
        :return:
        """
        pass

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

        table_collection_with_select = short_tables_collection

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
        :param service_key_fact:
        :param service_key_dimension:
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

    def generate_selects_from_collection(self, short_tables_collection: ShortTablesCollectionForSelect,
                                         add_order_by: bool)  -> SelectCollection:
        """
        Generates select structure from short tables collection
        :param short_tables_collection: should be created from self.generate_pre_select_collection()
        :param add_order_by: add order by to fact query or not
        :return:
        """

        temp_structure: SelectCollection = SelectCollection()

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
            # Order by list
            order_by: list[str]

            select_list, select_for_group_by, joins, where, order_by, has_calculation = self \
                .generate_structure_for_each_piece_of_join(short_tables_collection, table)

            not_selected_fields_no = len(short_tables_collection.get_all_selects(table))

            sql, has_group_by = self.generate_select_query(select_list, select_for_group_by, joins, where,
                                                           has_calculation, table, order_by, not_selected_fields_no,
                                                           add_order_by)

            temp_structure.add_table(table, sql, not_selected_fields_no, has_group_by)

        return temp_structure

    def generate_structure_for_each_piece_of_join(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                  table: str) \
            -> tuple[list[str], list[str], dict, list[str], list[str], bool]:
        #TODO: rename function
        """
        :param short_tables_collection:
        :param table:
        :return:
        """

        return self.olap_select_builder.generate_structure_for_each_fact_table(short_tables_collection, table)

    def generate_select_query(self, select_list: list, select_for_group_by: list, joins: dict, where: list,
                              has_calculation: bool, table_name: str, order_by: list[str], not_selected_fields_no: int,
                              add_order_by: bool) -> tuple[str, bool]:
        """
        Generates select statement ready for database query
        All parameters come from self.generate_structure_for_each_piece_of_join()
        :param order_by: order_by fields
        :param has_calculation: If true, our select needs GROUP BY with select_for_group_by
        :param not_selected_fields_no:
        :param table_name: table name for FROM
        :param select_list: list of select pieces
        :param select_for_group_by: if there is any calculation we need to use this list in group by
        :param joins: tables to be joined
        :param where: list of where conditions
        :param add_order_by: add order by to fact query or not
        :return: select statement and True or false if query has calculation
        """
        return self.olap_select_builder.generate_select_query(select_list, select_for_group_by, joins, where,
                                                              has_calculation, table_name, order_by,
                                                              not_selected_fields_no, add_order_by)

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

    def generate_structure_for_dimension_table(self, frontend_fields: OlapFrontendToBackend,
                                               tables_collection: OlapTablesCollection) \
            -> tuple[str | None, list[str], list[str], list[str], bool]:
        return self.olap_select_builder.generate_structure_for_dimension_table(frontend_fields, tables_collection)

    @staticmethod
    def get_tables_with_field(alias_field_name: str, tables_collection: OlapTablesCollection) -> list[str]:
        """
        Get tables with alias_field_name
        Should be used for frontend filters
        :param alias_field_name: alias of field
        :param tables_collection:
        :return: list of tables containing alias_field_name
        """

        table_names: list[str] = []

        dimension_table_data = tables_collection.get_dimension_table_with_field(alias_field_name)

        if dimension_table_data is not None:
            table_names.append(dimension_table_data[0])

            return table_names

        data_tables = tables_collection.get_data_tables_with_field(alias_field_name)

        if data_tables is None:
            raise OlapException(f"Field {alias_field_name} does not exist")
        else:
            return data_tables

    @staticmethod
    def get_tables_for_filter(alias_field_name: str, tables: list[str], tables_collection: OlapTablesCollection) \
            -> list[TableForFilter]:
        """

        :param tables_collection:
        :param alias_field_name:
        :param tables:
        :return:
        """

        tables_filter: list[TableForFilter] = []

        for table in tables:
            is_distinct: bool = False

            if table in tables_collection.get_dimension_table_names():
                is_distinct = True
                return [TableForFilter(table, alias_field_name, is_distinct, 0)]

            if tables_collection.get_data_table_calculation(table, alias_field_name) == \
                    OlapCalculations.DISTINCT.value:
                is_distinct = True
                return [TableForFilter(table, alias_field_name, is_distinct, 0)]

            if tables_collection.get_data_table_calculation(table, alias_field_name) is None:
                tables_filter.append(TableForFilter(table, alias_field_name, is_distinct,
                                                    tables_collection.get_number_of_fields(table)))

        return tables_filter

    def generate_filter_select(self, tables: list[TableForFilter], field_alias: str, select_type: str,
                               tables_collection: OlapTablesCollection) -> SelectFilter:
        """
        Generates select for frontend filters
        :param tables_collection:
        :param tables:
        :param field_alias:
        :param select_type:
        :return:
        """

        select_filter = SelectFilter()

        for table in tables:

            table_name: str = table.get_table_name()
            number_of_fields: int = table.get_number_of_fields()

            backend_field = tables_collection.get_backend_field_name(table_name, field_alias)

            select_statement: str

            if select_type == FilterTypes.ALL.value:
                select_statement = self.olap_select_builder.get_select_fiter_all(backend_field)
            elif select_type == FilterTypes.MAX_MIN.value:
                select_statement = self.olap_select_builder.get_select_fiter_max_min(backend_field)
            else:
                raise OlapException(f"Wrong type of select_type: {select_type}")

            sql = f"{select_statement} FROM {table_name}"

            select_filter.add_table(table_name, sql, number_of_fields)


        return select_filter

    def generate_select_for_dimension(self, table_name: str, select_list: list[str], select_for_group_by: list[str],
                                      where: list[str], has_calculation: bool, add_order_by: bool) -> SelectCollection:
        select_collection: SelectCollection = SelectCollection()

        sql, has_group_by = self.olap_select_builder.generate_select_query(select_list, select_for_group_by, {}, where,
        has_calculation, table_name, [], 0, add_order_by)

        select_collection.add_table(table_name, sql, 0, has_group_by)

        return select_collection

    def select_data(self, frontend_data: OlapFrontendToBackend, tables_collection: OlapTablesCollection,
                    add_order_by: bool = False) -> SelectCollection:
        """
        Starts all necessary functions to satisfy frontend query
        :param frontend_data: OlapFilterFrontend with data from frontend
        :param tables_collection: OlapTablesCollection from OlapStructureGenerator
        :param add_order_by: add order by to fact query or not
        :return: selects in form of SelectCollection.class
        """
        has_fact_table: bool = self.fact_table_in_query(frontend_data, tables_collection)

        if has_fact_table:
            short_tables_collection_for_select: ShortTablesCollectionForSelect = \
                self.generate_pre_select_collection(frontend_data, tables_collection)
            return self.generate_selects_from_collection(short_tables_collection_for_select, add_order_by)


        if not has_fact_table:
            table_name, select_list, select_for_group_by, where, has_calculation \
                = self.generate_structure_for_dimension_table(frontend_data, tables_collection)

            return self.generate_select_for_dimension(table_name, select_list, select_for_group_by, where,
                                                      has_calculation, add_order_by)



    def select_filter_for_frontend(self, frontend_data: OlapFilterFrontend, tables_collection: OlapTablesCollection) \
            -> SelectFilter:
        """
        Starts all necessary functions to get select statement for frontend filters
        :param frontend_data: OlapFilterFrontend with data from frontend
        :param tables_collection: OlapTablesCollection from OlapStructureGenerator
        :return: selects in form of SelectFilter.class
        """
        all_tables_with_field:  list[str] = self.get_tables_with_field(frontend_data.get_field_alias_name(),
                                                                       tables_collection)
        tables: list[TableForFilter] = self.get_tables_for_filter(frontend_data.get_field_alias_name(),
                                                                  all_tables_with_field, tables_collection)

        select_filter: SelectFilter = self.generate_filter_select(tables, frontend_data.get_field_alias_name(),
                                                                  frontend_data.get_select_type(), tables_collection)

        return select_filter
