import numpy
import pandas

from kf_lib_data_ingest.common.misc import (
    clean_up_df,
    recover_containers_from_df_strings,
)


def test_convert_to_str():
    df1 = pandas.DataFrame(
        {
            # Do not change existing test cases.
            # Add more columns if you need to.
            # With all numbers and None, the column converts to float and
            # the None here changes to NaN (which would break comparisons).
            # After convert_to_str the NaN is back to a friendly None.
            # Lists and dicts should recursively clean their internals but stay
            # as native objects.
            "a": [1.0, 2, 0.2, 0.0, None],
            "b": ["1_1", " ab ", 2, 2.56, None],
            "c": [1, 2, 3, [1.0, " 2", "a"], {"a": 0.0, "b": 2}],
        },
        dtype=object,
    )
    df2 = pandas.read_json(df1.to_json(), dtype=object)

    assert df1["a"][4] is None
    assert df2["a"][4] is not None
    assert numpy.isnan(df2["a"][4])
    assert not df1.astype(str).equals(df2.astype(str))

    df3 = clean_up_df(df1)
    df4 = clean_up_df(df2)

    assert df3["a"][4] is None
    assert df4["a"][4] is None
    assert df3.astype(str).equals(df4.astype(str))

    assert df3["b"][0] == "1_1"
    assert df3["b"][1] == "ab"
    assert df3["b"][3] == "2.56"
    assert df3["a"][0] == "1"
    assert df3["a"][3] == "0"
    assert df3["a"][2] == "0.2"
    assert df3["c"][3] == "['1', '2', 'a']"
    assert df3["c"][4] == "{'a': '0', 'b': '2'}"

    # test that list and dict are subsequently recoverable
    df5 = recover_containers_from_df_strings(df3)
    assert df5["c"][3] == ["1", "2", "a"]
    assert df5["c"][4] == {"a": "0", "b": "2"}
    # and that everything else is the same
    for col in df5.columns:
        for i in range(len(df5)):
            if not (col == "c" and ((i == 3) or (i == 4))):
                assert df5[col][i] == df3[col][i]
