"""Databricks job entry point for the levekar_redusert_funksjonsevne dataset.

Reads an Excel file from S3, resolves geography, computes the disability ratio,
and writes per-district JSON files back to S3.
"""

import argparse

import pandas as pd

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

DATA_POINT = "antall_personer_med_redusert_funksjonsevne"


def read_redusert_funksjonsevne_excel(path):
    """Read redusert-funksjonsevne Excel from S3, resolve geography.

    Input columns (Excel):
        År, Geografi,
        Antall personer 16-66 år,
        Antall personer med redusert funksjonsevne,
        Andel personer med redusert funksjonsevne

    The Geografi column contains prefixed names like "Bydel Gamle Oslo",
    "Delbydel Grønland", or "Oslo i alt".

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        antall_personer_med_redusert_funksjonsevne,
        andel_personer_med_redusert_funksjonsevne
    """
    df = read_excel(path, date_column="År")

    # Parse Geografi column into bydel/delbydel
    def resolve_geography(name):
        name = str(name)
        if name.startswith("Delbydel "):
            delbydel_name = name.removeprefix("Delbydel ")
            d = delbydel_by_name(delbydel_name)
            if d:
                return {
                    "delbydel_id": d["id"],
                    "delbydel_navn": d["name"],
                    "bydel_id": None,
                    "bydel_navn": None,
                }
        b = bydel_by_name(name)
        if b:
            return {
                "delbydel_id": None,
                "delbydel_navn": None,
                "bydel_id": b["id"],
                "bydel_navn": b["name"],
            }
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": None,
            "bydel_navn": None,
        }

    geo = df["Geografi"].apply(resolve_geography).apply(pd.Series)
    df = pd.concat([df, geo], axis=1)

    df = df.rename(
        columns={
            "Antall personer med redusert funksjonsevne": (
                "antall_personer_med_redusert_funksjonsevne"
            ),
            "Andel personer med redusert funksjonsevne": (
                "andel_personer_med_redusert_funksjonsevne"
            ),
        }
    )
    df = df.dropna(subset=["bydel_id", "delbydel_id"], how="all")

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "antall_personer_med_redusert_funksjonsevne",
            "andel_personer_med_redusert_funksjonsevne",
        ]
    ]


def transform_redusert_funksjonsevne(df, type_of_ds):
    """Pure transform logic, ported from levekar_redusert_funksjonsevne."""
    df = df.copy()
    df[f"{DATA_POINT}_ratio"] = (
        df["andel_personer_med_redusert_funksjonsevne"] / 100
    )

    series = [{"heading": "Redusert funksjonsevne", "subheading": ""}]

    if type_of_ds == "status":
        [df] = transform.status(df)
        scale = get_min_max_values_and_ratios(df, DATA_POINT)
        template = TemplateA()
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
        scale = []
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading=("Personer fra 16 til 66 år med redusert funksjonsevne"),
        series=series,
        scale=scale,
    )

    return Output(
        df=df,
        values=[DATA_POINT],
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
    df = read_redusert_funksjonsevne_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/redusert-funksjonsevne.csv",
        )

    output = transform_redusert_funksjonsevne(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
