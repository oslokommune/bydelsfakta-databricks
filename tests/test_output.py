from datetime import date

import pandas as pd

from bydelsfakta.output import get_min_max_values_and_ratios


def test_get_min_max_values_and_ratios():
    df = pd.DataFrame.from_dict(
        [
            {"date": date(2025, 1, 1), "value": 1, "value_ratio": 0.7, "bydel_id": "0"},
            {"date": date(2025, 1, 1), "value": 2, "value_ratio": 0.2, "bydel_id": "0"},
            {"date": date(2025, 1, 1), "value": 3, "value_ratio": 0.5, "bydel_id": "0"},
            {
                "date": date(2025, 1, 1),
                "value": 20,
                "value_ratio": 0.05,
                "bydel_id": "99",
            },
            {
                "date": date(2024, 1, 1),
                "value": 10,
                "value_ratio": 0.9,
                "bydel_id": "0",
            },
        ]
    )

    res = get_min_max_values_and_ratios(df, "value")

    assert res == {"value": [1.0, 3.0], "ratio": [0.2, 0.7]}
