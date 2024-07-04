from tc_analyzer_lib.algorithms.utils.member_activity_utils import convert_to_dict


def test_empty():
    results = convert_to_dict(data=(), dict_keys=[])

    assert results == {}


def test_single_data():
    results = convert_to_dict(data=["value1"], dict_keys=["var1"])

    assert results == {"var1": "value1"}


def test_multiple_data():
    results = convert_to_dict(
        data=["value1", "value2", "value3"], dict_keys=["var1", "var2", "var3"]
    )

    assert results == {"var1": "value1", "var2": "value2", "var3": "value3"}
