import pandas as pd

from bydelsfakta.jobs.levekar_totalt import keep_newest


def test_keep_newest():
    df = pd.DataFrame(data={"date": [2019, 2020, 2020], "value": [1, 2, 3]})
    assert keep_newest(df).values.tolist() == [[2], [3]]
