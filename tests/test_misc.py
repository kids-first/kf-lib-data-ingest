import pandas

from kf_lib_data_ingest.common.misc import *


def test_numeric_to_str():
    df1 = pandas.DataFrame(
        {'a': [1, 2, 3, None], 'b': ['1_1', ' ab ', 2, None]}, dtype=object
    )
    df2 = pandas.read_json(df1.to_json(), dtype=object)
    assert not df1.astype(str).equals(df2.astype(str))

    df1 = df1.applymap(
        lambda x: numeric_to_str(x, replace_na=True)
    )
    df2 = df2.applymap(
        lambda x: numeric_to_str(x, replace_na=True)
    )
    assert df1.astype(str).equals(df2.astype(str))

    assert df1['b'][0] == '1_1'
    assert df1['b'][1] == 'ab'
