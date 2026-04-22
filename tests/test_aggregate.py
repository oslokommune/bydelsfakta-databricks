import pandas as pd

from bydelsfakta.aggregate import Aggregate, ColumnNames

column_names = ColumnNames()


def test_aggregation_sum(husholdninger_df):
    aggregation = {"personer_1": "sum"}
    agg = Aggregate(aggregation)
    df_act = agg.aggregate(husholdninger_df)

    expected_oslo_i_alt = (
        husholdninger_df.groupby([column_names.date])["personer_1"]
        .sum()
        .reset_index()
    )
    expected_oslo_i_alt["bydel_id"] = "00"

    expected_districts = (
        husholdninger_df.groupby(
            [column_names.date, column_names.district_id]
        )["personer_1"]
        .sum()
        .reset_index()
    )

    expected_sub_districts = (
        husholdninger_df.groupby(
            [
                column_names.date,
                column_names.district_id,
                column_names.sub_district_id,
            ]
        )["personer_1"]
        .sum()
        .reset_index()
    )

    expected = pd.concat(
        [expected_sub_districts, expected_districts, expected_oslo_i_alt]
    )
    assert list(df_act["personer_1"].sort_values()) == list(
        expected["personer_1"].sort_values()
    )


def test_add_ratios(husholdninger_df):
    husholdninger_df["ratio_1_2"] = (
        husholdninger_df["personer_1"] / husholdninger_df["personer_2"]
    )

    df = Aggregate({}).add_ratios(
        husholdninger_df,
        data_points=["personer_1"],
        ratio_of=["personer_2"],
    )

    assert list(df["ratio_1_2"].sort_values()) == list(
        df["personer_1_ratio"].sort_values()
    )


def test_duplication_is_handled(husholdninger_df):
    df = pd.concat(
        (husholdninger_df, husholdninger_df.iloc[0:1, :]), axis=0
    ).reset_index(drop=True)

    aggregation = {"personer_1": "sum"}
    agg = Aggregate(aggregation)
    df_act = agg.aggregate(df)

    expected_oslo_i_alt = (
        df.groupby([column_names.date])["personer_1"].sum().reset_index()
    )
    expected_oslo_i_alt["bydel_id"] = "00"

    expected_districts = (
        df.groupby([column_names.date, column_names.district_id])[
            "personer_1"
        ]
        .sum()
        .reset_index()
    )

    expected_sub_districts = (
        df.groupby(
            [
                column_names.date,
                column_names.district_id,
                column_names.sub_district_id,
            ]
        )["personer_1"]
        .sum()
        .reset_index()
    )

    expected = pd.concat(
        [expected_sub_districts, expected_districts, expected_oslo_i_alt]
    )

    expected = list(expected["personer_1"].sort_values())
    result = list(df_act["personer_1"].sort_values())

    for i in range(len(result)):
        assert result[i] == expected[i]
