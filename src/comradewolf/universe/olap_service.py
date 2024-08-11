from comradewolf.utils.enums_and_field_dicts import OlapCalculations
from comradewolf.utils.olap_data_types import OlapFrontendToBackend, OlapTablesCollection, \
    ShortTablesCollectionForSelect

SELECT = "SELECT"
WHERE = "WHERE"
GROUP_BY = "GROUP BY"
INNER_JOIN = "INNER_JOIN"


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
        short_tables_collection = self.add_select_fields_to_short_tables_collection(short_tables_collection,
                                                                                    frontend_fields.get_select(),
                                                                                    tables_collection)

        short_tables_collection = self.add_select_fields_to_short_tables_collection(short_tables_collection,
                                                                                    frontend_fields.get_where(),
                                                                                    tables_collection,
                                                                                    True)

        short_tables_collection = \
            self.add_calculation_fields_to_short_tables_collection(short_tables_collection,
                                                                   frontend_fields.get_calculation(),
                                                                   tables_collection)

        return short_tables_collection

    def add_calculation_fields_to_short_tables_collection(self, short_tables_collection: ShortTablesCollectionForSelect,
                                                          calculations: list,
                                                          tables_collection: OlapTablesCollection) \
            -> ShortTablesCollectionForSelect:

        short_tables_collection: ShortTablesCollectionForSelect = short_tables_collection.copy()

        list_of_fact_tables = list(short_tables_collection.keys())

        # If service key not in table, remove table from table_collection
        tables_to_delete_from_short_collection: list[str] = []

        can_use_sk: bool = False

        for calculation_field in calculations:
            current_field_name = calculation_field["field_name"]
            current_calculation = calculation_field["calculation"]

            dimension_fields: list | None = tables_collection.get_dimension_table_with_field(current_field_name)

            # Check if field in dimension table
            if dimension_fields is not None:

                # Can we use service key for count
                if (current_calculation in [OlapCalculations.COUNT, OlapCalculations.COUNT_DISTINCT]) & \
                        tables_collection.get_is_sk_for_count(dimension_fields[0], current_field_name):
                    can_use_sk = True

            for table in list_of_fact_tables:

                if table in tables_to_delete_from_short_collection:
                    continue

                add_dimension: bool = False
                add_fact_field: bool
                add_sk_field: bool = False

                if dimension_fields is not None:
                    # Service key not in table
                    if tables_collection.is_field_in_data_table(dimension_fields[1], dimension_fields[0], None) \
                            is False:
                        tables_to_delete_from_short_collection.append(table)
                        continue

                if can_use_sk:
                    short_tables_collection, add_sk_field = self.add_calculation_no_join(dimension_fields[1],
                                                                                         current_calculation,
                                                                                         table,
                                                                                         short_tables_collection,
                                                                                         tables_collection)

                if add_sk_field:
                    continue

                short_tables_collection, add_fact_field = self.add_calculation_no_join(current_field_name,
                                                                                       current_calculation,
                                                                                       table,
                                                                                       short_tables_collection,
                                                                                       tables_collection)

                if add_fact_field:
                    continue

                if can_use_sk:
                    short_tables_collection, add_dimension = self.add_join_calculation(current_field_name,
                                                                                       current_calculation,
                                                                                       table,
                                                                                       dimension_fields[0],
                                                                                       dimension_fields[1],
                                                                                       short_tables_collection)

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
            if len(tables_collection[table_name]["all_selects"]) == 0:
                short_tables_collection.add_select_field(table_name, current_field_name, current_calculation)
                added_dimension = True
                return short_tables_collection, added_dimension

            further_calculation: str | None = tables_collection.get_data_table_further_calculation(table_name,
                                                                                                   current_field_name,
                                                                                                   current_calculation)

            # You can NOT aggregate aggregated field
            if further_calculation is None:
                return short_tables_collection, added_dimension

            # You can aggregate aggregated field
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

        :param is_where:
        :param short_tables_collection:
        :param frontend_fields_select_or_where:
        :param tables_collection:
        :return:
        """

        table_collection_with_select = short_tables_collection.copy()

        list_of_fact_tables = list(table_collection_with_select.keys())

        tables_to_delete_from_short_collection: list[str] = []

        # Iterate through select fields
        for front_field_dict in frontend_fields_select_or_where:

            current_field: str = front_field_dict["field_name"]

            dimension_table_and_service_key: list | None = tables_collection.get_dimension_table_with_field(
                current_field)

            for fact_table_name in list_of_fact_tables:

                # This table already did not satisfy one of fields
                if fact_table_name in tables_to_delete_from_short_collection:
                    continue

                is_field_in_table: bool = tables_collection.is_field_in_data_table(current_field, fact_table_name, None)

                # Field is in the fact table. Add select or where move to next table in loop
                # Field is not in fact table
                if is_field_in_table:
                    if is_where is False:
                        table_collection_with_select.add_select_field(fact_table_name, current_field)
                    else:
                        table_collection_with_select.add_where(fact_table_name, fact_table_name,
                                                               front_field_dict)
                    continue

                # Not dimension table and not in fact table
                # Just add table to delete later
                if dimension_table_and_service_key is None:
                    tables_to_delete_from_short_collection.append(fact_table_name)
                    continue

                # Checking if service key in fact table
                is_service_key_in_table: bool = tables_collection.is_field_in_data_table(
                    dimension_table_and_service_key[1], fact_table_name, None)

                if is_service_key_in_table is False:
                    tables_to_delete_from_short_collection.append(fact_table_name)
                    continue

                # Field is not in fact table, but you can join dimension table
                if is_where is False:
                    table_collection_with_select.add_join_field_for_select(fact_table_name, current_field,
                                                                           dimension_table_and_service_key[0],
                                                                           dimension_table_and_service_key[1])
                else:
                    table_collection_with_select.add_where_with_join(fact_table_name, current_field,
                                                                     dimension_table_and_service_key[0],
                                                                     dimension_table_and_service_key[1],
                                                                     front_field_dict)

        for delete_table in tables_to_delete_from_short_collection:
            del table_collection_with_select[delete_table]

        return table_collection_with_select

    @staticmethod
    def add_join_calculation(current_field_name: str, current_calculation: str, table_name: str, join_table: str,
                             service_key: str, short_tables_collection: ShortTablesCollectionForSelect) \
            -> tuple[ShortTablesCollectionForSelect, bool]:

        short_tables_collection.add_join_field_for_aggregation(table_name, current_field_name, current_calculation,
                                                               join_table, service_key)

        return short_tables_collection, True
