import numpy as np
import pandas as pd
import pytest

from bydelsfakta.jobs.folkemengde import _sum_nans


def test_sum_nans_values():
    series = pd.Series(data=[4, 5, 6])
    assert _sum_nans(series) == 15


def test_sum_nans_all_nan():
    series = pd.Series(data=[np.nan, np.nan, np.nan])
    assert np.isnan(_sum_nans(series))


def test_sum_nans_mix_raises():
    series = pd.Series(data=[4, 5, np.nan, 7])
    with pytest.raises(ValueError):
        _sum_nans(series)
