"""Databricks job entry point for the fattige_barnehusholdninger dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse
import re

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

VALUE_COL = "husholdninger_med_barn_under_18_aar_eu_skala"


def _resolve_geography(geografi):
    """Resolve geography from the Geografi string.

    Returns (delbydel_id, delbydel_navn, bydel_id, bydel_navn)
    for a single Geografi value.
    """
    if geografi == "Oslo i alt":
        return None, None, "00", "Oslo i alt"

    # Handle "Uoppgitt" -> "Uten registrert adresse"
    if geografi == "Uoppgitt":
        return (
            None,
            None,
            "99",
            "Uten registrert adresse",
        )

    # Try delbydel: "Delbydel <name>"
    m = re.match(r"^Delbydel (.+)$", geografi)
    if m:
        name = m.group(1)
        delbydel = delbydel_by_name(name)
        if delbydel:
            bydel = bydel_by_id(delbydel["bydel_id"])
            return (
                delbydel["id"],
                delbydel["name"],
                bydel["id"],
                bydel["name"],
            )

    # Try bydel name (e.g. "Bydel Gamle Oslo", "Sentrum",
    # "Marka")
    bydel = bydel_by_name(geografi)
    if bydel:
        return None, None, bydel["id"], bydel["name"]

    return None, None, None, None


def read_fattige_barnehusholdninger_excel(path):
    """Read fattige-husholdninger Excel, resolve geography.

    Input columns (Excel):
        År, Geografi, Husholdninger i alt,
        Husholdninger med barn <18 år i alt,
        Husholdninger EU-skala - andel,
        Husholdninger med barn <18 år EU-skala -andel,
        Husholdninger etter EU-skala - antall,
        Husholdninger med barn <18 år etter EU-skala - antall

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        husholdninger_i_alt,
        husholdninger_med_barn_under_18_aar_i_alt,
        husholdninger_i_alt_eu_skala_andel,
        husholdninger_med_barn_under_18_aar_eu_skala_andel,
        husholdninger_i_alt_eu_skala_antall,
        husholdninger_med_barn_under_18_aar_eu_skala_antall
    """
    df = read_excel(path, date_column="År")

    geo = df["Geografi"].apply(_resolve_geography)
    df["delbydel_id"] = geo.map(lambda g: g[0])
    df["delbydel_navn"] = geo.map(lambda g: g[1])
    df["bydel_id"] = geo.map(lambda g: g[2])
    df["bydel_navn"] = geo.map(lambda g: g[3])

    df = df.rename(
        columns={
            "Husholdninger med barn <18 år etter EU-skala - antall": (
                f"{VALUE_COL}_antall"
            ),
            "Husholdninger med barn <18 år EU-skala -andel": (
                f"{VALUE_COL}_andel"
            ),
            "Husholdninger i alt": "husholdninger_i_alt",
            "Husholdninger med barn <18 år i alt": (
                "husholdninger_med_barn_under_18_aar_i_alt"
            ),
            "Husholdninger EU-skala - andel": (
                "husholdninger_i_alt_eu_skala_andel"
            ),
            "Husholdninger etter EU-skala - antall": (
                "husholdninger_i_alt_eu_skala_antall"
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
            "husholdninger_i_alt",
            "husholdninger_med_barn_under_18_aar_i_alt",
            "husholdninger_i_alt_eu_skala_andel",
            f"{VALUE_COL}_andel",
            "husholdninger_i_alt_eu_skala_antall",
            f"{VALUE_COL}_antall",
        ]
    ]


def transform_fattige_barnehusholdninger(df, type_of_ds):
    """Ported from functions/fattige_barnehusholdninger.py."""

    # Filter rows where both antall and andel are present
    df = df[df[f"{VALUE_COL}_antall"].notnull()].copy()
    df = df[df[f"{VALUE_COL}_andel"].notnull()].copy()

    df[VALUE_COL] = df[f"{VALUE_COL}_antall"]
    df[f"{VALUE_COL}_ratio"] = df[f"{VALUE_COL}_andel"] / 100

    metadata = Metadata(
        heading="Lavinntekts husholdninger med barn",
        series=[],
    )

    if type_of_ds == "status":
        [df] = transform.status(df)
        metadata.add_scale(get_min_max_values_and_ratios(df, VALUE_COL))
        template = TemplateA()
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=df,
        values=[VALUE_COL],
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
    df = read_fattige_barnehusholdninger_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/fattige-husholdninger.csv")

    output = transform_fattige_barnehusholdninger(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
