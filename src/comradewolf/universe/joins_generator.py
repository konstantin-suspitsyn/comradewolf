from copy import deepcopy

from comradewolf.universe.possible_joins import AllPossibleJoins
from comradewolf.utils.data_types import AllJoins, AllTables


class GenerateJoins:
    """
    Implementation of Dijkstra algorithm to get all possible shortest joins
    """
    all_tables = set()
    direct_joins = set()

    __join_weights = {"left": 2, "right": 3, "inner": 1}

    def __init__(self, direct_joins: AllJoins, all_tables: AllTables) -> None:
        """
        :param direct_joins: direct joins are coming from StructureGenerator.get_joins()
        :param all_tables: all tables coming from StructureGenerator.get_tables()
        """
        # Singleton with the shortest joins
        self.joins: AllPossibleJoins = AllPossibleJoins()
        self.direct_joins = direct_joins

        for table in all_tables:
            self.all_tables.add(table)

        for join_start_table in self.direct_joins:
            self.best_joins_for_start(join_start_table)

    def return_join(self, start_table: str, end_table: str) -> bool | dict:
        """
        Returns join if exists
        Returns false if does not exist
        :param start_table: from part of join
        :param end_table: join part of join
        :return:
        """
        if not self.joins.has_join(start_table, end_table):
            self.best_joins_for_start(start_table)

        return self.joins.get_join(start_table, end_table)

    def best_joins_for_start(self, start_table: str) -> None:
        """
        Generates all possible joins for start table
        :param start_table:
        :return:
        """
        # Something like +∞ in Dijkstra algorithm
        max_no = 1e7
        min_node_count = 1e7
        min_node: str | None = None

        join_dict = {}
        joins_by_table = deepcopy(self.direct_joins)

        all_tables = self.all_tables.copy()
        all_tables.remove(start_table)

        # Step 0. Create all connections
        for table in all_tables:

            join_dict[table] = {}
            if table in joins_by_table[start_table]:
                steps: int = self.__join_weights[joins_by_table[start_table][table]["how"]]
                join_dict[table]["steps"] = steps
                join_dict[table][0] = {}
                join_dict[table][0]["table"] = table
                for key in joins_by_table[start_table][table]:
                    join_dict[table][0][key] = joins_by_table[start_table][table][key]
                if (steps < min_node_count) & (table in joins_by_table):
                    min_node = table
                    min_node_count = steps
            else:
                join_dict[table]["steps"] = max_no

        # Remove visited connections
        del joins_by_table[start_table]

        join_of_start_table = self.__recursive_joins(join_dict, joins_by_table, set(joins_by_table.keys()), min_node)

        key_list = list(join_of_start_table.keys())

        # Remove not possible joins
        for key in key_list:
            if join_of_start_table[key]["steps"] == max_no:
                del join_of_start_table[key]

        # Remove steps key. We don't need it anymore
        for key in join_of_start_table.keys():
            del join_of_start_table[key]["steps"]

        self.joins.all_joins_by_starting_table({start_table: join_of_start_table})

    def __recursive_joins(self,
                          created_joins: dict,
                          all_joins: AllJoins,
                          all_tables: set,
                          next_node: str | None) -> dict:
        """
        Recursive way to generate connections using Dijkstra algorithm
        :param created_joins: created joins on previous step
        :param all_joins: copy of self.direct_joins with removed visited branches
        :param all_tables: copy of self.all_tables of with removed visited branches
        :param next_node: next node if exists
        :return:
        """
        if next_node is None:
            return created_joins

        all_tables.remove(next_node)

        for table in all_joins[next_node]:
            # Ignore existing nodes
            if table not in all_tables:
                continue
            steps: int = self.__join_weights[all_joins[next_node][table]["how"]]

            if created_joins[next_node]["steps"] + steps < created_joins[table]["steps"]:

                inner_dict_with_joins = created_joins[next_node].copy()

                all_keys = list(inner_dict_with_joins.keys())
                all_keys.remove("steps")

                next_key = max(all_keys) + 1
                inner_dict_with_joins[next_key] = {}
                inner_dict_with_joins[next_key]["table"] = table
                inner_dict_with_joins["steps"] = created_joins[next_node]["steps"] + steps
                for key in all_joins[next_node][table]:
                    inner_dict_with_joins[next_key][key] = all_joins[next_node][table][key]

                created_joins[table] = inner_dict_with_joins

        max_no = 1e7
        min_node: str | None = None

        for table in all_tables:
            if created_joins[table]["steps"] < max_no:
                max_no = created_joins[table]["steps"]
                min_node = table

        return self.__recursive_joins(created_joins, all_joins, all_tables, min_node)
