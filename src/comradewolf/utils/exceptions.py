class RepeatingTableException(Exception):
    """
    Error is thrown where we got two or more objects with repeating names
    """

    def __init__(self, file_name: str, name: str, duplicate_key_name: str) -> None:
        message = f"В файле {file_name} обнаружен дубликат {duplicate_key_name} {name}"
        super().__init__(message)


class NoMandatoryKeyException(Exception):
    """
    Exception if imported data doesn't have mandatory key
    """

    def __init__(self, file_name: str, key: str) -> None:
        message = f"В файле {file_name} не найден ключ {key}"
        super().__init__(message)


class UnknownTypeOfImport(Exception):
    """
    Exception if type of import not one of ImportTypes.values
    """

    def __init__(self, type_of_import: str) -> None:
        message = f"Неизвестный тип импорта {type_of_import}"
        super().__init__(message)


class NoHumanNameForShownField(Exception):
    """
    Error when setting field to show and not giving it a name
    """

    def __init__(self, field_name: str) -> None:
        message = f"Показатель show поля {field_name} == True, при этом поле \"name\" не представлено"
        super().__init__(message)


class UnknownFieldTypeForField(Exception):
    """
    Exception if type of field not in FieldType
    """

    def __init__(self, field_name: str, field_type: str) -> None:
        message = f"Неизвестный тип поля {field_type} для {field_name}"
        super().__init__(message)


class UnknownFieldType(Exception):
    """
    Exception if type of field not in FieldType
    """

    def __init__(self, field_type: str) -> None:
        message = f"Неизвестный тип поля {field_type}"
        super().__init__(message)


class FieldsFromFrontendWrongValue(Exception):
    """
    Exception if any value under key of FieldsFromFrontend class is not list
    """

    def __init__(self, key: str, should_be_type: str, is_type_of: str) -> None:
        """
        :param key: Key of dictionary
        :param should_be_type: key should be instance of this field
        :param is_type_of: key is instance of this field
        """
        message = f"Проблема с данными из frontend. {key} должен быть типом {should_be_type}, а является {is_type_of}"
        super().__init__(message)


class UnknownTableType(Exception):
    def __init__(self, table_name: str, table_type: str) -> None:
        message = f"Неизвестный тип таблицы {table_name}: {table_type}"
        super().__init__(message)


class UnknownFrontFieldType(Exception):
    """
    Exception if front field type is not in FrontFieldTypes(enum.Enum)
    """

    def __init__(self, field_name: str, front_field_type: str) -> None:
        message = f"Неизвестный тип поля для front-end. Поле: {field_name}, тип поля {front_field_type}"
        super().__init__(message)


class NotAllMandatoryFields(Exception):
    """
    Thrown if any of the mandatory fields does not exist
    """

    def __init__(self, message: str):
        super().__init__(message)


class ObjectExists(Exception):
    """
    Thrown if an object exists already
    """

    def __init__(self, message: str):
        super().__init__(message)


class QueryBuilderException(Exception):
    """
    Error occurring during building error
    """

    def __init__(self, message: str):
        super().__init__(message)


class OlapCreationException(Exception):
    """
    Error occurring during incorrect OLAP objects creation
    """

    def __init__(self, message: str):
        super().__init__(message)


class OlapTableExists(Exception):
    """
    Error occurring during trying to create existing OLAP objects table
    """

    def __init__(self, table_name: str, table_type: str) -> None:
        message: str = f"Таблица {table_name} уже в списке таблиц {table_type}"
        super().__init__(message)


class ConditionFieldsError(Exception):
    """
    Error is thrown when all conditions for function do not satisfy function logic
    """

    def __init__(self, message: str):
        super().__init__(message)


class OlapException(Exception):
    """
    Any exception
    """
    def __init__(self, message: str):
        super().__init__(message)
