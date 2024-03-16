from abc import ABC, abstractmethod


class BaseCalculationBuilder(ABC):
    """
    Base class for all basic calculation builders
    """

    def generate_calculation(self, field_name: str, calculation_type: str) -> str:
        if calculation_type == "sum":
            return self.generate_sum(field_name)

        if calculation_type == "avg":
            return self.generate_avg(field_name)

        if calculation_type == "min":
            return self.generate_min(field_name)

        if calculation_type == "max":
            return self.generate_max(field_name)

        if calculation_type == "count":
            return self.generate_count(field_name)

        if calculation_type == "count distinct":
            return self.generate_count_distinct(field_name)

        raise RuntimeError

    @abstractmethod
    def generate_sum(self, field_name: str) -> str:
        pass

    @abstractmethod
    def generate_avg(self, field_name: str) -> str:
        pass

    @abstractmethod
    def generate_count_distinct(self, field_name: str) -> str:
        pass

    @abstractmethod
    def generate_max(self, field_name: str) -> str:
        pass

    @abstractmethod
    def generate_min(self, field_name: str) -> str:
        pass

    @abstractmethod
    def generate_count(self, field_name: str) -> str:
        pass

    @abstractmethod
    def type_formatting(self, front_type_format: str, operator_format: str) -> str:
        """
        Returns template for formatting of where
        :param operator_format:
        :param front_type_format: front_type_format should be the same as frontend
        :return:
        """

        basic_format: str = "{}"

        formats_and_placeholders: dict = {
            "text": "'{}'",
            "date": "'{}'",
        }

        operator_format_and_placeholders: dict = {
            "like": "'%{}%'",
            "not_like": "'%{}%'",
            "startswith": "'%{}'",
            "endswith": "'{}%'"
        }

        if operator_format in operator_format_and_placeholders:
            basic_format = operator_format_and_placeholders[operator_format]
        elif front_type_format in formats_and_placeholders:
            basic_format = formats_and_placeholders[front_type_format]

        return basic_format

    @abstractmethod
    def operator_formatting(self, operator_format: str) -> str:
        """
        Used to define anything that needs additional characters like % in like / not like statements
        :param operator_format:
        :return:
        """

        operators_and_placeholders: dict = {
            "=": " = {} ",
            ">": " > {} ",
            "<": " < {} ",
            ">=": " >= {} ",
            "<=": " <= {} ",
            "between": " BETWEEN {} AND {} ",
            "like": " LIKE {} ",
            "not like": " NOT LIKE '{}' ",
            "in": " IN {} ",
            "not in": " NOT IN {} ",
            "startswith": " LIKE '{}' ",
            "endswith": " LIKE '{}' ",
        }

        return operators_and_placeholders[operator_format]


class PostgresCalculationBuilder(BaseCalculationBuilder):
    """
    Postgres-like
    """

    def type_formatting(self, front_type_format: str, operator_format: str) -> str:

        basic_format: str = "{}"

        formats_and_placeholders: dict = {
            "text": "'{}'",
            "date": "'{}'",
        }

        operator_format_and_placeholders: dict = {
            "like": "'%{}%'",
            "not_like": "'%{}%'",
            "startswith": "'%{}'",
            "endswith": "'{}%'"
        }

        if operator_format in operator_format_and_placeholders:
            basic_format = operator_format_and_placeholders[operator_format]
        elif front_type_format in formats_and_placeholders:
            basic_format = formats_and_placeholders[front_type_format]

        return basic_format

    def operator_formatting(self, operator_format: str) -> str:

        operators_and_placeholders: dict = {
            "=": " = {} ",
            ">": " > {} ",
            "<": " < {} ",
            ">=": " >= {} ",
            "<=": " <= {} ",
            "between": " BETWEEN {} AND {} ",
            "like": " LIKE {} ",
            "not like": " NOT LIKE '{}' ",
            "in": " IN {} ",
            "not in": " NOT IN {} ",
            "startswith": " LIKE '{}' ",
            "endswith": " LIKE '{}' ",
        }

        return operators_and_placeholders[operator_format]

    def generate_sum(self, field_name: str) -> str:
        return "SUM({})".format(field_name)

    def generate_avg(self, field_name: str) -> str:
        return "AVG({})".format(field_name)

    def generate_count_distinct(self, field_name: str) -> str:
        return "COUNT(DISTINCT {})".format(field_name)

    def generate_max(self, field_name: str) -> str:
        return "MAX({})".format(field_name)

    def generate_min(self, field_name: str) -> str:
        return "MIN({})".format(field_name)

    def generate_count(self, field_name: str) -> str:
        return "COUNT({})".format(field_name)
