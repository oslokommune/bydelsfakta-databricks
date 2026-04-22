"""Databricks job entry point for the boligpriser dataset.

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


def read_boligpriser_excel(path):
    """Read boligpriser Excel from S3, resolve geography.

    Input columns (Excel):
        År, Delbydelnummer, Delbydelsnavn, Oslo-Bydelsnavn,
        antall omsatte blokkleieligheter, kvmpris

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        antall_omsatte_blokkleiligheter, kvmpris
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from numeric ID (sub-district rows)
    delbydel = df["Delbydelnummer"].map(delbydel_by_id, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to Oslo-Bydelsnavn
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]), na_action="ignore"
    )
    bydel_via_name = df["Oslo-Bydelsnavn"].map(
        lambda n: bydel_by_name(str(n)), na_action="ignore"
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(
        columns={
            "antall omsatte blokkleieligheter": (
                "antall_omsatte_blokkleiligheter"
            ),
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "antall_omsatte_blokkleiligheter",
            "kvmpris",
        ]
    ]


def transform_boligpriser(df, type_of_ds):
    """Pure transformation logic, ported from functions/boligpriser.py."""
    df = df.rename(columns={"kvmpris": "value"})

    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [{"heading": heading, "subheading": ""}]
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
    df = read_boligpriser_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df, f"{args.silver_dir}/boligpriser-blokkleiligheter.csv"
        )

    output = transform_boligpriser(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
