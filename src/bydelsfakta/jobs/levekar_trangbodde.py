"""Databricks job entry point for the levekar_trangbodde dataset.

Reads an Excel file from S3, resolves geography from numeric IDs, transforms
the data through the standard bydelsfakta aggregation pipeline, and writes per-
district JSON files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.geography import (
    bydel_by_id,
    bydel_by_name,
    delbydel_by_name,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import TemplateA, TemplateB


def read_trangbodde_excel(path):
    """Read trangbodde Excel from S3, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Geografi,
        Andel som bor trangt, Antall som bor trangt,
        Antall husholdninger

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        andel_som_bor_trangt, antall_som_bor_trangt,
        antall_husholdninger
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to name
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )
    bydel_via_name = df["Bydel"].map(
        lambda n: (bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}"))
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(
        columns={
            "Andel som bor trangt": "andel_som_bor_trangt",
            "Antall som bor trangt": "antall_som_bor_trangt",
            "Antall husholdninger": "antall_husholdninger",
        }
    )

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "andel_som_bor_trangt",
            "antall_som_bor_trangt",
            "antall_husholdninger",
        ]
    ]


def transform_trangbodde(df, type_of_ds):
    """Pure transform logic, ported from levekar_trangbodde."""
    datapoint = "antall_som_bor_trangt"

    # Compute ratio from the percentage column
    df = df.copy()
    df["antall_som_bor_trangt_ratio"] = df["andel_som_bor_trangt"] / 100

    series = [{"heading": "Trangbodde", "subheading": ""}]

    if type_of_ds == "status":
        [df] = transform.status(df)
        scale = get_min_max_values_and_ratios(df, datapoint)
        template = TemplateA()
    elif type_of_ds == "historisk":
        scale = []
        [df] = transform.historic(df)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading="Levekår Trangbodde",
        series=series,
        scale=scale,
    )

    return Output(
        df=df,
        values=[datapoint],
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
    df = read_trangbodde_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/trangbodde.csv")

    output = transform_trangbodde(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
