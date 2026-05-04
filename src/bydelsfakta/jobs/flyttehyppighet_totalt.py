"""Databricks job entry point for flyttehyppighet_totalt.

Reads two Excel files (flytting-fra-etter-alder + flytting-til-etter- alder)
from S3, resolves geography, merges immigration/emigration data, and writes
per-district JSON files back to S3.

This dataset uses CUSTOM Template classes (HistoricTemplate, StatusTemplate)
that produce immigration/emigration objects grouped by age and date.
"""

import argparse

import numpy as np
import pandas as pd

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate, ColumnNames
from bydelsfakta.geography import bydel_by_name
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output
from bydelsfakta.templates import Template

graph_metadata = Metadata(
    heading="Flytting etter alder",
    series=[{"heading": "Flytting", "subheading": ""}],
)

aldersgruppe_col = "aldersgruppe_5_aar"


def read_flytting_fra_excel(path):
    """Read flytting-fra-etter-alder Excel, resolve geography.

    Input columns (Excel):
        aargang, til_fra_bydel, fra_bydel, aldgr5, antall

    Pivots til_fra_bydel into movement columns:
        utflytting_fra_oslo, utflytting_innenfor_bydelen,
        utflytting_mellom_bydeler

    Output columns (normalized):
        date, bydel_id, bydel_navn, aldersgruppe_5_aar,
        utflytting_fra_oslo, utflytting_innenfor_bydelen,
        utflytting_mellom_bydeler
    """
    df = read_excel(path, date_column="aargang")

    # Resolve bydel from name
    bydel = df["fra_bydel"].map(
        lambda n: bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}")
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(columns={"aldgr5": aldersgruppe_col})

    # Pivot til_fra_bydel to get movement type columns
    df = df.pivot_table(
        values="antall",
        index=[
            "date",
            "bydel_id",
            "bydel_navn",
            aldersgruppe_col,
        ],
        columns="til_fra_bydel",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Flytting inn/ut av Oslo": "utflytting_fra_oslo",
            "Flytting innenfor bydelen": ("utflytting_innenfor_bydelen"),
            "Flytting mellom bydeler": "utflytting_mellom_bydeler",
        }
    )

    return df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            aldersgruppe_col,
            "utflytting_fra_oslo",
            "utflytting_innenfor_bydelen",
            "utflytting_mellom_bydeler",
        ]
    ]


def read_flytting_til_excel(path):
    """Read flytting-til-etter-alder Excel, resolve geography.

    Input columns (Excel):
        aargang, til_fra_bydel, til_bydel, aldgr5, antall

    Pivots til_fra_bydel into movement columns:
        innflytting_til_oslo, innflytting_innenfor_bydelen,
        innflytting_mellom_bydeler

    Output columns (normalized):
        date, bydel_id, bydel_navn, aldersgruppe_5_aar,
        innflytting_til_oslo, innflytting_innenfor_bydelen,
        innflytting_mellom_bydeler
    """
    df = read_excel(path, date_column="aargang")

    # Resolve bydel from name
    bydel = df["til_bydel"].map(
        lambda n: bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}")
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(columns={"aldgr5": aldersgruppe_col})

    # Pivot til_fra_bydel to get movement type columns
    df = df.pivot_table(
        values="antall",
        index=[
            "date",
            "bydel_id",
            "bydel_navn",
            aldersgruppe_col,
        ],
        columns="til_fra_bydel",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Flytting inn/ut av Oslo": "innflytting_til_oslo",
            "Flytting innenfor bydelen": ("innflytting_innenfor_bydelen"),
            "Flytting mellom bydeler": "innflytting_mellom_bydeler",
        }
    )

    return df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            aldersgruppe_col,
            "innflytting_til_oslo",
            "innflytting_innenfor_bydelen",
            "innflytting_mellom_bydeler",
        ]
    ]


def _immigration_object(row_data):
    return {
        "alder": row_data[aldersgruppe_col],
        "mellomBydeler": row_data["innflytting_mellom_bydeler"],
        "innenforBydelen": row_data["innflytting_innenfor_bydelen"],
        "tilFraOslo": row_data["innflytting_til_oslo"],
    }


def _emigration_object(row_data):
    return {
        "alder": row_data[aldersgruppe_col],
        "mellomBydeler": row_data["utflytting_mellom_bydeler"],
        "innenforBydelen": row_data["utflytting_innenfor_bydelen"],
        "tilFraOslo": row_data["utflytting_fra_oslo"],
    }


def _value_list(value_collection):
    if value_collection.empty:
        return []
    return value_collection.tolist()


def _values(df):
    value_list = []
    for date, group_df in df.groupby(by="date"):
        immigration_list = group_df.apply(
            lambda row: _immigration_object(row), axis=1
        )
        emigration_list = group_df.apply(
            lambda row: _emigration_object(row), axis=1
        )
        entry = {
            "year": date,
            "immigration": _value_list(immigration_list),
            "emigration": _value_list(emigration_list),
        }
        value_list.append(entry)

    return value_list


class HistoricTemplate(Template):
    def values(self, df, series, column_names=ColumnNames()):
        return _values(df)


class StatusTemplate(Template):
    def values(self, df, series, column_names=ColumnNames()):
        return _values(df).pop()


def transform_flyttehyppighet(fra_df, til_df, type_of_ds):
    """Merge fra and til, aggregate, generate output."""
    flytting_df = pd.merge(
        fra_df,
        til_df,
        on=["date", "bydel_navn", "bydel_id", aldersgruppe_col],
    )

    flytting_df["delbydel_id"] = np.nan
    flytting_df["delbydel_navn"] = np.nan

    flytting_df = flytting_df.astype(
        {"delbydel_id": object, "delbydel_navn": object}
    )

    input_df = (
        Aggregate("sum")
        .aggregate(
            flytting_df,
            extra_groupby_columns=[aldersgruppe_col],
        )
        .astype({"date": int})
    )

    if type_of_ds == "historisk":
        [input_df] = transform.historic(input_df)
        output = Output(
            values=None,
            df=input_df,
            metadata=graph_metadata,
            template=HistoricTemplate(),
        ).generate_output()
    elif type_of_ds == "status":
        [input_df] = transform.status(input_df)
        output = Output(
            values=None,
            df=input_df,
            metadata=graph_metadata,
            template=StatusTemplate(),
        ).generate_output()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fra-dir", required=True)
    parser.add_argument("--til-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status", "historisk"],
    )
    args = parser.parse_args()

    fra_df = read_flytting_fra_excel(resolve_input(args.fra_dir))
    til_df = read_flytting_til_excel(resolve_input(args.til_dir))

    if args.silver_dir:
        write_csv_output(
            fra_df,
            f"{args.silver_dir}/flytting-fra-etter-alder.csv",
        )
        write_csv_output(
            til_df,
            f"{args.silver_dir}/flytting-til-etter-alder.csv",
        )

    output = transform_flyttehyppighet(fra_df, til_df, args.type_of_ds)
    write_json_output(output, args.output_dir)
