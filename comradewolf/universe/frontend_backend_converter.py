from typing import Tuple

from comradewolf.utils.data_types import FieldsFromFrontend, FieldsForQuery, AllFields, AllTables, WhereFields
from comradewolf.utils.enums_and_field_dicts import FieldType, TableTypes, FrontendTypeFields
from comradewolf.utils.language_specific_builders import BaseCalculationBuilder
from comradewolf.utils.utils import get_table_from_field, get_fields


class FrontendBackendConverter:
    """
    Converts data from frontend to backend for further query creation
    """

    def __init__(self, all_fields: AllFields, all_tables: AllTables, predefined_where: WhereFields,
                 calculation_builder: BaseCalculationBuilder) -> None:
        """
        Initialize class
        :param all_fields: all fields with properties
        :param all_tables: dict with table and properties
        """
        self.all_fields = all_fields
        self.all_tables = all_tables
        self.predefined_where = predefined_where
        self.calculation_builder = calculation_builder

    def convert_from_frontend_to_backend(self, frontend_fields: dict) -> FieldsForQuery:
        all_fields_by_table: FieldsForQuery = FieldsForQuery()

        # Add select fields
        for field in frontend_fields["select"]:
            all_fields_by_table.add_field(field, self.all_fields, self.all_tables)

        # Add Calculation fields
        for field in frontend_fields["calculation"]:
            for calc_table_name in field:
                if field[calc_table_name] == "PREDEFINED":
                    all_fields_by_table.add_field(calc_table_name, self.all_fields, self.all_tables)
                else:
                    all_fields_by_table.add_user_calculations_field(
                        calc_table_name,
                        self.calculation_builder.generate_calculation(calc_table_name, field[calc_table_name]),
                        self.all_tables)

        # Add where fields
        all_fields_by_table.add_overall_where(frontend_fields["where"],
                                              self.all_fields,
                                              self.all_tables,
                                              self.predefined_where)

        return all_fields_by_table

    def del_convert_from_frontend_to_backend(self, fields_for_query_structure: FieldsFromFrontend) -> FieldsForQuery:
        """
        Receives data from frontend as type of FieldsFromFrontend and converts it to FieldsForQuery type
        :param fields_for_query_structure:
        :return:
        """

        # Generate all possible table types
        all_fields_by_table = FieldsForQuery()

        # Working with select
        for frontend_field_type in fields_for_query_structure:
            # Could be one of [select, where, calculations]
            for field in fields_for_query_structure[frontend_field_type]:
                current_table: str
                current_table_type: str

                select: set = set()
                calculations: set = set()
                where: set = set()
                join_tables: set = set()

                if frontend_field_type == FrontendTypeFields.SELECT.value:
                    current_table = get_table_from_field(field)
                    current_table_type = self.all_tables[current_table]
                    field_type = self.all_fields[field]["type"]
                    if field_type == FieldType.CALCULATION.value:
                        calculations.add(self.all_fields[field]["calculation"])
                        if "where" in self.all_fields[field]:
                            where.add(self.all_fields[field]["where"])
                        if "join_tables" in self.all_fields[field]:
                            join_tables.add(self.all_fields[field]["join_tables"])

                    if field_type in [FieldType.SELECT.value, FieldType.VALUE.value]:
                        select.add(field)

                    all_fields_by_table.add_fields_to_table(table_name=current_table,
                                                            table_type=current_table_type,
                                                            select=select,
                                                            calculations=calculations,
                                                            where=where,
                                                            join_tables=join_tables)

                if frontend_field_type in [FrontendTypeFields.CALCULATIONS.value, FrontendTypeFields.WHERE.value]:
                    data_tables, dimension_tables = self.__get_tables(field)

                    all_fields_by_table.add_data_tables_if_not_exist(data_tables)
                    all_fields_by_table.add_dimension_tables_if_not_exist(dimension_tables)

                    if frontend_field_type == FrontendTypeFields.CALCULATIONS.value:
                        calculations.add(field)
                    if frontend_field_type == FrontendTypeFields.WHERE.value:
                        where.add(field)

                    if (len(data_tables) > 1) and (frontend_field_type == FrontendTypeFields.CALCULATIONS.value):
                        all_fields_by_table.add_standalone_calculation(calculations)

                        continue

                    if ((len(data_tables) + len(dimension_tables) > 1) and
                            (frontend_field_type == FrontendTypeFields.WHERE.value)):
                        all_fields_by_table.add_standalone_where(where)

                        continue

                    current_table: str = ""
                    # Do not change order of len ifs
                    if len(data_tables) == 0:
                        current_table = dimension_tables.pop()

                    if len(data_tables) == 1:
                        current_table = data_tables.pop()

                    # TODO: think of something with two data tables in one calculation
                    if len(data_tables) > 1:
                        raise RuntimeError("This was not yet planned")

                    current_table_type = self.all_tables[current_table]

                    all_fields_by_table.add_fields_to_table(table_name=current_table,
                                                            table_type=current_table_type,
                                                            select=select,
                                                            calculations=calculations,
                                                            where=where,
                                                            join_tables=join_tables)

                    for dimension_table in dimension_tables:
                        all_fields_by_table.add_table_if_not_exist(dimension_table, "dimension")

        return all_fields_by_table

    def __get_tables(self, aggregation_or_where: str) -> Tuple[set, set]:
        """
        Returns data_tables and dimension_tables from aggregation or where clause
        :param aggregation_or_where:
        :return:
        """
        data_tables = set()
        dimension_tables = set()

        extracted_fields = get_fields(aggregation_or_where)

        for field in extracted_fields:
            if field not in self.all_fields:
                continue

            table_from_field = get_table_from_field(field)

            if self.all_tables[table_from_field] == TableTypes.DATA.value:
                data_tables.add(table_from_field)

            if self.all_tables[table_from_field] == TableTypes.DIMENSION.value:
                dimension_tables.add(table_from_field)

        if (len(data_tables) == 0) and (len(dimension_tables) == 0):
            raise RuntimeError("Таблицу из поля не получилось получить")

        return data_tables, dimension_tables
