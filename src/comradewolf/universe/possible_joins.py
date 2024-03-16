from collections import UserDict
from typing_extensions import Self

from comradewolf.utils.exceptions import QueryBuilderException
from comradewolf.utils.utils import singleton


@singleton
class AllPossibleJoins(UserDict):
    """
            All possible joins
            Even, if you need join two or more tables to get to next table

            Structure:
            {
                "left_table_name":
                    {
                        "right_table_name":
                            {
                                join_number: # number of join to get to last table
                                    {
                                        "table": table_name, # table of this join number
                                        "how": left/right/inner/outer,
                                        "on":
                                            {
                                                "between_tables": list of signs, # [=, <, >]
                                                "first_table_on": list of field names from left table,
                                                "second_table_on": ist of field names from right table
                                            }
                                    }
                            }
                    }
            }
    """

    def has_join(self, start_table: str, end_table: str) -> bool:
        """
        Checks if self.data has join between tables start_table and end_table
        :param start_table:
        :param end_table:
        :return:
        """
        if start_table not in self.data:
            return False

        if end_table not in self.data[start_table]:
            return False

        return True

    def has_table_with_joins(self, start_table: str) -> bool:
        """
        Checks if self.data has join starting with start_table
        :param start_table:
        :return:
        """
        if start_table not in self.data:
            return False

        return True

    def all_joins_by_starting_table(self, join_dict: dict):
        self.data.update(join_dict)

    def add_join(self, start_table: str, end_table: str, complete_path_of_joins: dict) -> None:
        """
        Adds join to self.data
        :param start_table:
        :param end_table:
        :param complete_path_of_joins:
        :return:
        """
        if not self.has_join(start_table, end_table):
            self.data[start_table] = {}
            self.data[start_table][end_table] = complete_path_of_joins

    def get_join(self, start_table: str, end_table: str) -> dict | bool:
        """
        Returns a join
        :param start_table:
        :param end_table:
        :return:
        """
        if not self.has_join(start_table, end_table):
            message: str = "Не найден join между таблицами"
            raise QueryBuilderException(message)
        return self.data[start_table][end_table]

    def get_all_joins(self) -> Self:
        """
        Return all joins that were created
        :return:
        """
        return self
