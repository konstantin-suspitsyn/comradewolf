import copy

from comradewolf.universe.possible_joins import AllPossibleJoins
from comradewolf.utils.data_types import FieldsForQuery, AllFields, AllTables, WhereFields
from comradewolf.utils.enums_and_field_dicts import TableTypes, FieldType
from comradewolf.utils.exceptions import QueryBuilderException
from comradewolf.utils.language_specific_builders import BaseCalculationBuilder
from comradewolf.utils.utils import join_on_to_string


class QueryGenerator:
    """
    Generates query
    Suitable for PostgreSQL
    """

    joins: AllPossibleJoins
    tables_dict: AllTables
    all_fields: AllFields

    CTE_JOIN = "cte_join"

    CTE = "cte_{}"
    CALC_FIELD = "{}_calc_{}"
    COMBI = "combi"

    DIMENSION_SELECTS_K = "dimension_selects"
    MAIN_CTE = "main_cte"
    OUT_K = "out"
    QUERY_K = "query"
    SELECT_K = "select"
    CALCULATIONS_K = "calculation"
    ALL_CTE = "all_cte"
    WHERE_K = "where"

    def __init__(self, tables_dict: AllTables, all_fields: AllFields, predefined_where: WhereFields,
                 language_specific_builder: BaseCalculationBuilder):

        # All tables
        self.tables_dict = tables_dict
        # ShortestDistance is singleton
        self.joins: AllPossibleJoins = AllPossibleJoins()

        self.all_fields = all_fields

        self.where_fields = predefined_where

        self.language_specific = language_specific_builder

    def generate_query(self, selected_objects: FieldsForQuery) -> str:
        """
        Select query for fields
        :param selected_objects:
        :return: query string
        """

        query: str

        if len(selected_objects.get_fact_tables()) > 1:
            query = self.generate_select_for_multiple_data_tables(selected_objects)
        else:
            query = self.generate_select_for_one_data_table(selected_objects)

        return query

    def generate_select_for_one_data_table(self, selected_objects: FieldsForQuery,
                                           exclude_tables: list | None = None) -> str:
        """
        Generates query for one fact table and any number of dimension tables
        :param exclude_tables: only one fact table in cte. Other fact tables should go here
        :param selected_objects: selected objects. Must have only one data table
        :return: SQL query
        """

        # This block of variables is used for sql generator
        select: set = set()
        calculation: set = set()
        where: set = set()
        join_tables: set = set()
        from_table: str = ""

        query: str

        # Check if method used correctly
        if len(selected_objects.get_fact_tables()) > 1:
            raise RuntimeError("This method for structure with one fact table")

        if len(selected_objects.get_fact_tables()) == 1:
            from_table = list(selected_objects.get_fact_tables())[0]

            # Check if all joins exist
            for dimension_table in selected_objects.get_dimension_tables():
                table_ = list(selected_objects.get_fact_tables())[0]
                if not self.joins.has_join(table_, dimension_table):
                    raise RuntimeError("No join")
        elif len(selected_objects.get_dimension_tables()) > 0:

            from_table = self.__get_dimension_table_that_connects_with_others(selected_objects.get_dimension_tables())

        # Get all tables needed for query
        join_tables.update(selected_objects.get_fact_tables())
        join_tables.update(selected_objects.get_dimension_tables())

        # Remove table used in from
        if from_table not in join_tables:
            raise QueryBuilderException("Соединения между таблицами не настроены")

        join_tables.remove(from_table)

        for key in TableTypes:
            list_of_tables = list(selected_objects[key.value].keys())
            for table in list_of_tables:
                for current_calculation in selected_objects[key.value][table][self.SELECT_K]:
                    select.update([current_calculation])
                # TODO: SYNC this with BBBB
                i: int = 0
                for c in selected_objects[key.value][table][self.CALCULATIONS_K]:
                    calculation.add("{} as {}".format(c, self.CALC_FIELD.format(from_table.split(".")[-1],
                                                                                i)))
                    i += 1
                where.update(selected_objects[key.value][table][self.WHERE_K])

        where_condition: dict = selected_objects.get_overall_where()

        if len(where) > 0:
            if where_condition != {}:
                where_condition = {"and": [where_condition]}
            else:
                where_condition = {"and": []}
            for where_item in where:
                where_condition["and"].append({"predefined": where_item})

        where_string: str = self.generate_where_string(where_condition, exclude_tables)

        query = self.__generate_sql_text_for_one_data_table(select, calculation, where_string, from_table, join_tables)

        return query

    def generate_select_for_multiple_data_tables(self, selected_objects: FieldsForQuery) -> str:
        """
        Generates CTE queries with multiple data tables
        It will have structure:

        with
             cte_0 as (
                         all fields for fact_table[0]
                        ,all dimension table fields
                     )
            ,cte_1 as (
                         all fields for fact_table[1]
                        ,all dimension table fields
                      )
            ,cte_i as (
                         all fields for fact_table[i]
                        ,all dimension table fields
                      )
            cte_main as (UNION of all non calculation fields)

        select
            fields
        from cte_main
            left join cte_0
            left join cte_1
            left join cte_i
        where if needed
        group by if needed

        :param selected_objects:
        :return: sql query string
        """
        dimension_tables: set = selected_objects.get_dimension_tables()
        fact_tables: set = selected_objects.get_fact_tables()

        # Fast check if all joins exist
        for fact_table in fact_tables:
            if len(selected_objects[TableTypes.DATA.value][fact_table][self.SELECT_K]) > 0:
                raise QueryBuilderException("Выбраны 2 или более фактовые таблицы с невозможностью их соединения")

            for dimension_table in dimension_tables:

                if not self.joins.has_join(fact_table, dimension_table):
                    raise QueryBuilderException("Соединения между выбранными таблицами не настроены")

        # TODO: check if fact tables doesn't have any non-calculation fields

        # For CTE we should define which fields will be used for join
        # If for every fact table, joined dimension table has the same field(s)
        #   Those fields should be used for later join of two fact fields
        # If not then fact fields will be used

        # Start building CTEs
        # TODO: create special format
        cte_properties = {}

        cte_no = 0

        # Will contain data for cte
        cte_properties[self.ALL_CTE] = {}

        for fact_table in fact_tables:

            other_fact_tables = list(fact_tables)
            other_fact_tables.remove(fact_table)

            current_cte = self.CTE.format(cte_no)
            cte_properties[self.ALL_CTE][current_cte] = {}
            current_select = copy.deepcopy(selected_objects)
            current_select.remove_all_fact_tables_except_named(fact_table)
            cte_properties[self.ALL_CTE][current_cte][self.QUERY_K] = self.generate_select_for_one_data_table(
                current_select, other_fact_tables)
            cte_properties[self.ALL_CTE][current_cte][self.OUT_K] = set()

            # TODO: SYNC this with BBBB
            for item in range(len(selected_objects[TableTypes.DATA.value][fact_table][
                                      self.CALCULATIONS_K])):
                cte_properties[self.ALL_CTE][current_cte][self.OUT_K].add(self.CALC_FIELD.format(fact_table.split(
                    ".")[-1], item))

            cte_no += 1

        cte_properties["calculations"] = set()
        cte_properties[self.DIMENSION_SELECTS_K] = set()

        for dimension_table in dimension_tables:
            cte_properties[self.DIMENSION_SELECTS_K].update(
                selected_objects[TableTypes.DIMENSION.value][dimension_table][
                    FieldType.SELECT.value])

        # Build all CTEs
        query = self.__generate_text_query_for_multiple_tables(cte_properties)

        return query

    def __generate_sql_text_for_one_data_table(self, select: set, calculation: set, where: str,
                                               from_table: str, join_tables: set) -> str:
        """
        Generates sql from one data table
        :param select:
        :param calculation:
        :param where:
        :param from_table:
        :param join_tables:
        :return:
        """

        query: str = "SELECT\n\t "
        group_by: str = "\nGROUP BY\n\t "
        calculation_select: str = ""
        select_query: str = ""
        from_query = "\nFROM {}\n".format(from_table)
        where_query = "\nWHERE\n\t"

        if len(select) > 0:
            select_query = "\n\t,".join(select)
            query += select_query
            calculation_select = "\n\t,"

        if len(calculation) > 0:
            calculation_select += "\n\t,".join(calculation)
            query += calculation_select

        query += from_query

        for end_table in join_tables:
            for key in self.joins.get_join(from_table, end_table).keys():
                join_temp = self.joins.get_join(from_table, end_table)[key]

                query += "\n{} join {} \n on {}".format(join_temp["how"], end_table,
                                                        join_on_to_string(join_temp["on"]))

        if len(where) > 0:
            query += where_query + where

        if (len(calculation) > 0) and (len(select) > 0):
            query += group_by
            query += select_query

        return query

    def __get_dimension_table_that_connects_with_others(self, dimension_tables: set):
        """
        Selects dimension table that has joins with all others dimension tables in set
        :param dimension_tables: set of dimension tables
        :return: string with name of tables or raises an error
        """
        if len(dimension_tables) == 1:
            return dimension_tables.pop()

        for from_table in dimension_tables:

            for join_table in dimension_tables:
                if from_table == join_table:
                    continue
                if not self.joins.has_join(from_table, join_table):
                    break

            return from_table

        raise QueryBuilderException("No join")

    def __generate_text_query_for_multiple_tables(self, cte_properties: dict) -> str:
        """
        Generates query text for multiple fact tables
        :param cte_properties: prepared for CTE tables dict
        :return: SQL query
        """
        query = "WITH \n"

        cte_template = "{} as \n(\n{}\n)"
        main_cte_query_full = ",\n" + str(self.MAIN_CTE) + " as \n({})"
        # Contains select statements for main_cte
        main_cte_table_queries: list[str] = []
        union_cte_query = "\nSELECT\n\t {} \nFROM {}"
        left_join_cte: list[str] = []
        left_join_template: str = "LEFT JOIN {}\n\tON\n\t{}"
        field_equality: str = "{}.{} = {}.{}"

        select_fields: list[str] = []
        select_field_template: str = "{}.{}"

        coalesce_template: str = "COALESCE({})"

        cte_query: list[str] = []

        # START generate code for CTEs
        dimension_selects = [f.split(".")[-1] for f in cte_properties[self.DIMENSION_SELECTS_K]]

        for cte in cte_properties[self.ALL_CTE]:
            # Generates selects for main_cte
            main_cte_table_queries.append(union_cte_query.format("\n\t,".join(dimension_selects), cte))
            cte_query.append(cte_template.format(cte, cte_properties[self.ALL_CTE][cte][self.QUERY_K]))
            and_join: list[str] = []

            for field in dimension_selects:
                and_join.append(field_equality.format(cte, field, self.MAIN_CTE, field))

            for field in cte_properties[self.ALL_CTE][cte][self.OUT_K]:
                select_fields.append(select_field_template.format(cte, field))

            left_join_cte.append(left_join_template.format(cte, "\n\tAND ".join(and_join)))

        for dimension_field in cte_properties[self.DIMENSION_SELECTS_K]:
            dimension_select_list: list[str] = []
            for cte in cte_properties[self.ALL_CTE]:
                dimension_select_list.append(select_field_template.format(cte, dimension_field.split(".")[-1]))
            select_fields.append(coalesce_template.format(", ".join(dimension_select_list)))

        query += ",\n".join(cte_query)
        query += main_cte_query_full.format("\nUNION".join(main_cte_table_queries))

        # END generate code for CTEs

        # START generate code for select
        select: str = "\n\t,".join(select_fields)
        query += union_cte_query.format(select, self.MAIN_CTE)
        query += "\n" + "\n".join(left_join_cte)

        return query

    def generate_where_string(self, where: dict, exclude_tables: list[str] | None = None) -> str:
        """
        Generate where string for query
        Taking into account how the date and text should be formatted

        :param exclude_tables:
        :param where:
        :return:
        """

        if exclude_tables is None:
            exclude_tables = []

        where_pieces = []
        join_word: str = ""

        where_string: str

        for key in where:
            # Exclude cte from where
            if key in exclude_tables:
                continue
            if key == "or" or key == "and":
                if len(where[key]) > 1:
                    join_word = ") \n{} (".format(key)

                for item in where[key]:
                    where_pieces.append(self.generate_where_string(item, exclude_tables))

            elif key == "predefined":
                where_pieces.append(where[key])

            elif where[key]["operator"] == "predefined":
                where_pieces.append(self.where_fields.get_where_query(key))

            else:
                where_pieces.append(self.__generate_single_where(key, where[key]))

        where_string = join_word.join(where_pieces)
        if len(join_word) > 0:
            where_string = "({})".format(where_string)

        return where_string

    def __generate_single_where(self, field_name: str, condition: dict) -> str:
        """
        Generates single where taking into account how the date and text should be formatted
        :param condition:
        :return:
        """

        # "{} {}".format(field_name, condition_of_where)
        where_string_placeholder: str = "{} {}"

        frontend_type: str = self.all_fields.get_frontend_type(field_name)
        operator: str = condition["operator"]
        single_field_placeholder: str = self.language_specific.type_formatting(frontend_type, operator)

        condition_string: str

        if operator == "between":
            condition_string = self.language_specific.operator_formatting(operator).format(
                single_field_placeholder.format(condition["condition"][0]),
                single_field_placeholder.format(condition["condition"][1]), )

        elif operator == "in":
            conditions = [single_field_placeholder.format(f) for f in condition["condition"].strip().split(";")]
            condition_string = "({})".format(", ".join(conditions))

        else:
            condition_string: str = self.language_specific.operator_formatting(operator).format(
                single_field_placeholder.format(condition["condition"][0]))

        return where_string_placeholder.format(field_name, condition_string)
