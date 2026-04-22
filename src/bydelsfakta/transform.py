from functools import reduce

import numpy as np
import pandas as pd

from bydelsfakta import geography


def historic(*dfs, date="date"):
    uniques = [df[date].unique() for df in dfs]
    reduced = reduce(lambda a, b: np.intersect1d(a, b), uniques)
    return [df[df[date].isin(reduced)] for df in dfs]


def status(*dfs, date="date"):
    years = None

    for df in dfs:
        year_set = set(df[date])
        years = years.intersection(year_set) if years else year_set

    if len(years) == 0:
        raise ValueError("No overlapping years in dataframes")

    return [df[df[date] == max(years)] for df in dfs]


def add_district_id(org, district_column=None):
    df = org.copy()
    df["district"] = df["delbydelid"].str.slice(4, 6)

    if district_column:
        missing = df["district"].isnull()
        df.loc[missing, "district"] = df[missing][district_column].apply(
            geography.get_district_id
        )
        return df
    else:
        return df[df["district"].str.len() > 0]


def pivot_table(df, pivot_column, value_columns):
    key_columns = list(
        filter(lambda x: x not in [pivot_column, *value_columns], list(df))
    )
    df_pivot = pd.concat(
        (
            df[key_columns],
            df.pivot(columns=pivot_column, values=value_columns),
        ),
        axis=1,
    )
    return df_pivot.groupby(key_columns).sum().reset_index()
