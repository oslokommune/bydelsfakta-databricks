"""Databricks job entry point for the levekaar_dodsrate dataset.

Reads a dodsrater Excel file from S3, resolves geography, computes the death
rate ratio, and writes per-district JSON files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.geography import bydel_by_name, delbydel_by_name
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import TemplateA, TemplateB


def read_dodsrater_excel(path):
    """Read dodsrater Excel from S3, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Dødelighet

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        dodsrate
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel from name
    bydel = df["Bydel"].map(
        lambda n: (bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}"))
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(columns={"Dødelighet": "dodsrate"})
    df = df.dropna(subset=["bydel_id"])

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "dodsrate",
        ]
    ]


def transform_dodsrate(df, type_of_ds):
    """Pure transform logic, ported from levekaar_dodsrate."""
    df = df.copy()
    df["dodsrate_ratio"] = df["dodsrate"] / 100

    if type_of_ds == "status":
        [df] = transform.status(df)
        scale = get_min_max_values_and_ratios(df, "dodsrate")
        template = TemplateA()
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
        scale = []
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading=("Dødelighet (gj.snitt siste 7 år) for personer 55–79 år"),
        series=[],
        scale=scale,
    )

    return Output(
        values=["dodsrate"],
        df=df,
        template=template,
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
    df = read_dodsrater_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/dodsrater.csv")

    output = transform_dodsrate(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
