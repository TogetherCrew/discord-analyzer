from discord_analyzer.algorithms.activity_hourly import parse_reaction


def test_parse_raction_no_input():
    sample_input = []
    output = parse_reaction(sample_input)

    assert output == []


def test_parse_reaction_partial_single_input():
    sample_input = ["user1,"]

    output = parse_reaction(sample_input)

    assert output == [["user1", ""]]


def test_parese_reaction_multiple_input_with_empty_reactions():
    sample_input = ["item1,item2|item3,,item4|item5,item6,item7|,"]

    output = parse_reaction(sample_input)

    assert output == [
        ["item1", "item2|item3", "", "item4|item5", "item6", "item7|", ""]
    ]


def test_parese_reaction_multiple_input_with_space_reactions():
    sample_input = ["item1,item2|item3, ,item4|item5,item6,item7|, "]

    output = parse_reaction(sample_input)

    assert output == [
        ["item1", "item2|item3", " ", "item4|item5", "item6", "item7|", " "]
    ]


def test_parse_raction_single_input():
    sample_input = ["emoji1"]

    output = parse_reaction(sample_input)

    assert len(output) == 1
    assert len(output[0]) == 1
    assert output == [["emoji1"]]


def test_parse_raction_multiple_input_with_singleComma():
    sample_input = ["mehrdad_mms#8600,😁", "mehrdad_mms#8600,🙌", "mehrdad_mms#8600,🤌"]
    output = parse_reaction(sample_input)

    assert len(output) == 3
    assert output[0] == ["mehrdad_mms#8600", "😁"]
    assert output[1] == ["mehrdad_mms#8600", "🙌"]
    assert output[2] == ["mehrdad_mms#8600", "🤌"]


def test_parse_raction_multiple_input_with_multipleComma():
    sample_input = ["sepehr#3795,thegadget.eth#3374,👍", "sepehr#3795,❤️"]

    output = parse_reaction(sample_input)

    assert len(output) == 2
    assert output[0] == ["sepehr#3795", "thegadget.eth#3374", "👍"]
    assert output[1] == ["sepehr#3795", "❤️"]
