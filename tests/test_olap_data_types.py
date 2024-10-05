import pytest

from comradewolf.utils.exceptions import OlapCreationException
from comradewolf.utils.olap_data_types import OlapDataTable, OlapDimensionTable, SERVICE_KEY_EXISTS_ERROR_MESSAGE, \
    NO_FRONT_NAME_ERROR, ERROR_FOLLOWING_CALC_SPECIFIED_WITHOUT_CALC


def test_data_dimension_table() -> None:
    """
    Tests data dimension
    :return:
    """

    fields_for_testing = {
        "test_field1": {
            "alias_name": "test_field1",
            "field_type": "service_key",
            "front_name": None,
            "data_type": "text",
        },
        "test_field2": {
            "alias_name": "test_field2",
            "field_type": "dimension",
            "front_name": "test_field2",
            "data_type": "text",
        },
        "test_field_second_service_key": {
            "alias_name": "test_field1",
            "field_type": "service_key",
            "front_name": None,
            "data_type": "text",
        },
        "test_field_no_front_name": {
            "alias_name": "test_field1",
            "field_type": "dimension",
            "front_name": None,
            "data_type": "text",
        },
        "test_field_unknown_type": {
            "alias_name": "test_field1",
            "field_type": "unknown_type",
            "front_name": None,
            "data_type": "text",
        },
    }

    data_dimension_table: OlapDimensionTable = OlapDimensionTable("test_dimension_table")

    data_dimension_table.add_field("test_field1",
                                   fields_for_testing["test_field1"]["field_type"],
                                   fields_for_testing["test_field1"]["alias_name"],
                                   fields_for_testing["test_field1"]["data_type"],
                                   fields_for_testing["test_field1"]["front_name"],
                                   )

    data_dimension_table.add_field("test_field2",
                                   fields_for_testing["test_field2"]["field_type"],
                                   fields_for_testing["test_field2"]["alias_name"],
                                   fields_for_testing["test_field1"]["data_type"],
                                   fields_for_testing["test_field2"]["front_name"], )

    with pytest.raises(OlapCreationException, match=SERVICE_KEY_EXISTS_ERROR_MESSAGE):
        data_dimension_table.add_field("test_field_second_service_key",
                                       fields_for_testing["test_field_second_service_key"]["field_type"],
                                       fields_for_testing["test_field_second_service_key"]["alias_name"],
                                       fields_for_testing["test_field1"]["data_type"],
                                       fields_for_testing["test_field_second_service_key"]["front_name"], )

    with pytest.raises(OlapCreationException, match=NO_FRONT_NAME_ERROR):
        data_dimension_table.add_field("test_field_no_front_name",
                                       fields_for_testing["test_field_no_front_name"]["field_type"],
                                       fields_for_testing["test_field_no_front_name"]["alias_name"],
                                       fields_for_testing["test_field1"]["data_type"],
                                       fields_for_testing["test_field_no_front_name"]["front_name"], )

    with pytest.raises(OlapCreationException, match="should be one of"):
        data_dimension_table.add_field("test_field_unknown_type",
                                       fields_for_testing["test_field_unknown_type"]["field_type"],
                                       fields_for_testing["test_field_unknown_type"]["alias_name"],
                                       fields_for_testing["test_field1"]["data_type"],
                                       fields_for_testing["test_field_unknown_type"]["front_name"], )

    assert len(data_dimension_table.get_field_names()) == 2


def test_data_table() -> None:
    """
    Tests OlapDataTable.class
    :return:
    """

    fields = {
        "test_field_1": {
            "alias_name": "alias_name",
            "field_type": "service_key",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_2": {
            "alias_name": "alias_name_2",
            "field_type": "service_key",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_unknown_type": {
            "alias_name": "alias_name_2",
            "field_type": "unknown_type",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_same_alias_name": {
            "alias_name": "alias_name_2",
            "field_type": "service_key",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_unknown_calc": {
            "alias_name": "alias_name_3",
            "field_type": "service_key",
            "calculation_type": "test_field_unknown_calc",
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_no_front_name": {
            "alias_name": "alias_name_nfn",
            "field_type": "dimension",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": None,
            "data_type": "number"
        },
        "test_field_incorrect_following_calculation": {
            "alias_name": "alias_name",
            "field_type": "service_key",
            "calculation_type": "sum",
            "following_calculation": "abc",
            "front_name": None,
            "data_type": "number"
        },
        "test_field_incorrect_following_calculation_wrong_calculation": {
            "alias_name": "alias_name",
            "field_type": "service_key",
            "calculation_type": None,
            "following_calculation": "sum",
            "front_name": None,
            "data_type": "number"
        },
        "test_field_3": {
            "alias_name": "alias_name_3",
            "field_type": "value",
            "calculation_type": None,
            "following_calculation": None,
            "front_name": "my_name",
            "data_type": "number"
        },
    }

    data_olap_table = OlapDataTable("test_data_table")

    data_olap_table.add_field("test_field_1",
                              fields["test_field_1"]["alias_name"],
                              fields["test_field_1"]["field_type"],
                              fields["test_field_1"]["calculation_type"],
                              fields["test_field_1"]["following_calculation"],
                              fields["test_field_1"]["data_type"],
                              fields["test_field_1"]["front_name"])

    data_olap_table.add_field("test_field_2",
                              fields["test_field_2"]["alias_name"],
                              fields["test_field_2"]["field_type"],
                              fields["test_field_2"]["calculation_type"],
                              fields["test_field_2"]["following_calculation"],
                              fields["test_field_2"]["data_type"],
                              fields["test_field_2"]["front_name"])

    with pytest.raises(OlapCreationException, match="is not one of"):
        data_olap_table.add_field("test_field_unknown_type",
                                  fields["test_field_unknown_type"]["alias_name"],
                                  fields["test_field_unknown_type"]["field_type"],
                                  fields["test_field_unknown_type"]["calculation_type"],
                                  fields["test_field_unknown_type"]["following_calculation"],
                                  fields["test_field_unknown_type"]["data_type"],
                                  fields["test_field_unknown_type"]["front_name"])

    with pytest.raises(OlapCreationException, match="is not one of"):
        data_olap_table.add_field("test_field_unknown_calc",
                                  fields["test_field_unknown_calc"]["alias_name"],
                                  fields["test_field_unknown_calc"]["field_type"],
                                  fields["test_field_unknown_calc"]["calculation_type"],
                                  fields["test_field_unknown_calc"]["following_calculation"],
                                  fields["test_field_unknown_calc"]["data_type"],
                                  fields["test_field_unknown_calc"]["front_name"])

    with pytest.raises(OlapCreationException, match="specified on field_type"):
        data_olap_table.add_field("test_field_no_front_name",
                                  fields["test_field_no_front_name"]["alias_name"],
                                  fields["test_field_no_front_name"]["field_type"],
                                  fields["test_field_no_front_name"]["calculation_type"],
                                  fields["test_field_no_front_name"]["following_calculation"],
                                  fields["test_field_no_front_name"]["data_type"],
                                  fields["test_field_no_front_name"]["front_name"])

    with pytest.raises(OlapCreationException, match="not one of"):
        data_olap_table.add_field("test_field_incorrect_following_calculation",
                                  fields["test_field_incorrect_following_calculation"]["alias_name"],
                                  fields["test_field_incorrect_following_calculation"]["field_type"],
                                  fields["test_field_incorrect_following_calculation"]["calculation_type"],
                                  fields["test_field_incorrect_following_calculation"]["following_calculation"],
                                  fields["test_field_incorrect_following_calculation"]["data_type"],
                                  fields["test_field_incorrect_following_calculation"]["front_name"])

    with pytest.raises(OlapCreationException, match=ERROR_FOLLOWING_CALC_SPECIFIED_WITHOUT_CALC):
        data_olap_table.add_field("test_field_incorrect_following_calculation_wrong_calculation",
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"]["alias_name"],
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"]["field_type"],
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"][
                                      "calculation_type"],
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"][
                                      "following_calculation"],
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"]["data_type"],
                                  fields["test_field_incorrect_following_calculation_wrong_calculation"]["front_name"])

    data_olap_table.add_field("test_field_3",
                              fields["test_field_3"]["alias_name"],
                              fields["test_field_3"]["field_type"],
                              fields["test_field_3"]["calculation_type"],
                              fields["test_field_3"]["following_calculation"],
                              fields["test_field_3"]["data_type"],
                              fields["test_field_3"]["front_name"])

    assert len(data_olap_table["fields"]) == 3
