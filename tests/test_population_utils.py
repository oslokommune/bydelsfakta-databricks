from pathlib import Path

import pandas as pd
import pytest

from bydelsfakta.population_utils import generate_population_df

DATA_DIR = Path(__file__).parent / "data" / "population_utils"


@pytest.fixture
def population_raw():
    return pd.read_csv(
        DATA_DIR / "population_utils_input.csv", sep=";"
    ).rename(columns={"aar": "date"})


def test_population_total(population_raw):
    population_total = generate_population_df(population_raw)
    expected = pd.read_csv(
        DATA_DIR / "population_total_expected.csv", sep=";"
    )
    assert sorted(
        population_total.to_dict("records"),
        key=lambda r: (r["date"], r["delbydel_id"]),
    ) == sorted(
        expected.to_dict("records"),
        key=lambda r: (r["date"], r["delbydel_id"]),
    )


def test_population_in_range(population_raw):
    population_30_to_59 = generate_population_df(
        population_raw, min_age=30, max_age=59
    )
    expected = pd.read_csv(
        DATA_DIR / "population_30_to_59_expected.csv", sep=";"
    )
    assert sorted(
        population_30_to_59.to_dict("records"),
        key=lambda r: (r["date"], r["delbydel_id"]),
    ) == sorted(
        expected.to_dict("records"),
        key=lambda r: (r["date"], r["delbydel_id"]),
    )
