from pathlib import Path

import pandas as pd
import pytest

from bydelsfakta.aggregate import ColumnNames

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def husholdninger_df():
    column_names = ColumnNames()
    dtype = {
        column_names.date: int,
        column_names.district_id: object,
        column_names.district_name: object,
        column_names.sub_district_id: object,
        column_names.sub_district_name: object,
    }
    return pd.read_csv(
        DATA_DIR / "husholdning_test_input.csv", sep=";", dtype=dtype
    ).rename(columns={"aar": "date"})


@pytest.fixture
def template_df():
    column_names = ColumnNames()
    return pd.read_csv(
        DATA_DIR / "transform_output_test_input.csv",
        sep=";",
        dtype={
            column_names.sub_district_id: object,
            column_names.district_id: object,
        },
    )


@pytest.fixture
def template_df_latest(template_df):
    return template_df[template_df["date"] == 2018]


@pytest.fixture
def template_k_df():
    column_names = ColumnNames()
    return pd.read_csv(
        DATA_DIR / "template_k_test_input.csv",
        sep=";",
        dtype={
            column_names.sub_district_id: object,
            column_names.district_id: object,
        },
    )
