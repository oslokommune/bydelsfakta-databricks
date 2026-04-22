"""Databricks job entry point for the median_totalpris dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.geography import (
    bydel_by_id,
    bydel_by_name,
    delbydel_by_id,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import TemplateA


def read_median_totalpris_excel(path):
    """Read median-totalpris Excel from S3, resolve geography.

    Input columns (Excel):
        År, Delbydel ID, Delbydel, Bydel ID, Bydel,
        Enhetstype, Antall salg, Median total pris

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        enhetstype, totalpris
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from numeric ID
    delbydel = df["Delbydel ID"].map(delbydel_by_id, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to name
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]), na_action="ignore"
    )
    bydel_via_name = df["Bydel"].map(
        lambda n: bydel_by_name(str(n)), na_action="ignore"
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    # Oslo i alt rows have null bydel_id
    df.loc[df["bydel_id"].isnull(), "bydel_id"] = "00"
    df.loc[df["bydel_navn"].isnull(), "bydel_navn"] = "Oslo i alt"

    df = df.rename(
        columns={
            "Median total pris": "totalpris",
            "Enhetstype": "enhetstype",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "enhetstype",
            "totalpris",
        ]
    ]


def transform_median_totalpris(df, type_of_ds):
    """Pure transformation, ported from functions/median_totalpris.py."""
    df = df.rename(columns={"totalpris": "value"})

    df = df[df["enhetstype"] == "both"].copy()
    df = df.drop(columns=["enhetstype"])

    heading = "Median totalpris"
    series = [
        {
            "heading": "Median totalpris for leiligheter",
            "subheading": "",
        },
    ]
    scale = get_min_max_values_and_ratios(df, "value")

    if type_of_ds == "status":
        [df] = transform.status(df)
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(heading=heading, series=series, scale=scale)

    return Output(
        df=df[~df["value"].isna()],
        values=["value"],
        template=TemplateA(),
        metadata=metadata,
    ).generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status", "historisk"],
    )
    args = parser.parse_args()

    input_path = resolve_input(args.input_dir)
    df = read_median_totalpris_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/median-totalpris.csv")

    output = transform_median_totalpris(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
