"""Databricks job entry point for the folkemengde dataset.

Reads befolkning-etter-kjonn-og-alder Excel from S3, resolves geography,
computes population and year-over-year changes, and writes per-district JSON
files back to S3.

Supports three dataset types: historisk, historisk_change, and nokkeltall.
"""

import argparse
from enum import Enum

import numpy as np
import pandas as pd

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate
from bydelsfakta.geography import bydel_by_id, delbydel_by_id
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output
from bydelsfakta.population_utils import generate_population_df
from bydelsfakta.templates import TemplateB, TemplateG


class DatasetType(Enum):
    HISTORIC = "historisk"
    HISTORIC_CHANGE = "historisk-prosent"
    KEYNUMBERS = "nokkeltall"


METADATA = {
    DatasetType.HISTORIC: Metadata(
        heading="Folkemengde utvikling historisk", series=[]
    ),
    DatasetType.HISTORIC_CHANGE: Metadata(
        heading="Folkemengde utvikling historisk prosent",
        series=[],
    ),
    DatasetType.KEYNUMBERS: Metadata(
        heading="Nøkkeltall om befolkningen",
        series=[
            {
                "heading": "Folkemengde",
                "subheading": "(totalt)",
            },
            {
                "heading": "Utvikling siste år",
                "subheading": False,
            },
            {
                "heading": "Utvikling siste 10 år",
                "subheading": False,
            },
        ],
    ),
}


def _sum_nans(series):
    """Sum with NaN handling: all-NaN -> NaN, else sum."""
    if series.isna().all():
        return np.nan
    elif series.notna().all():
        return np.sum(series)
    else:
        raise ValueError("Mix of NaN and values")


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel.

    Pivots age into columns ("0" through "120") for use with
    generate_population_df.
    """
    df = read_excel(path, date_column="År")

    geo = df.apply(
        lambda row: _resolve_geo(row["Bydel"], row["Delbydel"]),
        axis=1,
    )
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"])
    df["delbydel_navn"] = geo.map(lambda g: g["delbydel_navn"])
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"])
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"])

    df = df.rename(
        columns={
            "Kjønn": "kjonn",
            "Alder": "alder",
            "Antall personer": "antall_personer",
        }
    )

    df["alder"] = df["alder"].astype(str)

    df = df.pivot_table(
        values="antall_personer",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "kjonn",
        ],
        columns="alder",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    for age in [str(i) for i in range(0, 121)]:
        if age not in df.columns:
            df[age] = 0

    df = df.dropna(subset=["bydel_id"])
    return df


def _resolve_geo(bydel_val, delbydel_val):
    """Resolve geography from Bydel (301XX) and Delbydel IDs."""
    bydel_str = str(int(bydel_val))

    if bydel_str.startswith("301"):
        local = bydel_str[3:]
        bydel_id = local.zfill(2)
    else:
        bydel_id = bydel_str.zfill(2)

    bydel = bydel_by_id(bydel_id)
    bydel_navn = bydel["name"] if bydel else None

    delbydel = delbydel_by_id(int(delbydel_val))
    if delbydel:
        return {
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": bydel_navn,
        }

    return {
        "delbydel_id": None,
        "delbydel_navn": None,
        "bydel_id": bydel_id,
        "bydel_navn": bydel_navn,
    }


def filter_10year_set(df):
    """Filter to only max year and max year - 10."""
    max_year = df["date"].max()
    min_year = max_year - 10

    if not (df.date == min_year).any():
        raise ValueError(
            f"Dataset does not contain 10 year before {max_year}: {min_year}"
        )

    year_filter = df["date"].isin([max_year, min_year])
    return df[year_filter]


def calculate_change(df, *, column_name):
    """Calculate year-over-year change in population."""
    df = df[["bydel_id", "delbydel_id", "date", "population"]]
    indexed = df.set_index(["bydel_id", "delbydel_id", "date"])
    grouped = indexed.groupby(level="delbydel_id")
    return (
        grouped.diff().rename(columns={"population": column_name}).reset_index()
    )


def calculate_change_ratio(df):
    """Calculate change ratios (1-year and 10-year)."""
    df = df.sort_values(["bydel_id", "delbydel_id", "date"])
    df["change_ratio"] = df["change"] / df["population"].shift(1)
    df["change_10y_ratio"] = df["change_10y"] / df["population"].shift(10)
    return df


def generate(df):
    """Generate population data with change columns."""
    df = generate_population_df(df)
    change = calculate_change(df, column_name="change")
    change_10y = calculate_change(
        filter_10year_set(df), column_name="change_10y"
    )

    join_on = ["date", "bydel_id", "delbydel_id"]
    df = pd.merge(df, change, how="left", on=join_on)
    df = pd.merge(df, change_10y, how="left", on=join_on)

    df = Aggregate(
        {
            "population": "sum",
            "change": _sum_nans,
            "change_10y": _sum_nans,
        }
    ).aggregate(df)

    df = calculate_change_ratio(df)
    return df


def generate_keynumbers(df):
    """Generate keynumbers pivot with growth history."""
    [status_df] = transform.status(df)
    growth_df = df[df.date > df.date.max() - 10][
        ["bydel_id", "delbydel_id", "date", "population"]
    ]

    # Avoid dropping NaN indexes during pivot
    growth_df["delbydel_id"] = growth_df["delbydel_id"].fillna("#")

    growth_df = growth_df.pivot_table(
        index=["bydel_id", "delbydel_id"],
        columns="date",
        values="population",
    ).reset_index()

    # Add back NaN delbydel_id
    growth_df["delbydel_id"] = growth_df["delbydel_id"].replace("#", np.nan)

    return pd.merge(
        status_df,
        growth_df,
        how="outer",
        on=["bydel_id", "delbydel_id"],
    )


def transform_folkemengde(df, type_of_ds):
    """Transform folkemengde data for the given dataset type."""
    dataset_type = DatasetType(type_of_ds)
    [df] = transform.historic(df)
    df = generate(df)

    if dataset_type is DatasetType.HISTORIC:
        output = Output(
            values=["population"],
            df=df,
            template=TemplateB(),
            metadata=METADATA[dataset_type],
        )
    elif dataset_type is DatasetType.HISTORIC_CHANGE:
        df = df.dropna(axis=0, how="any", subset=["change", "change_ratio"])

        output = Output(
            values=["change"],
            df=df,
            template=TemplateB(),
            metadata=METADATA[dataset_type],
        )
    elif dataset_type is DatasetType.KEYNUMBERS:
        df = generate_keynumbers(df)
        max_year = df["date"].max()
        year_range = list(range(max_year - 9, max_year + 1))

        output = Output(
            values=["population", "change", "change_10y"],
            df=df,
            template=TemplateG(history_columns=year_range),
            metadata=METADATA[dataset_type],
        )
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return output.generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["historisk", "historisk-prosent", "nokkeltall"],
    )
    args = parser.parse_args()

    input_path = resolve_input(args.input_dir)
    df = read_befolkning_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/befolkning-etter-kjonn-og-alder.csv",
        )

    output = transform_folkemengde(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
