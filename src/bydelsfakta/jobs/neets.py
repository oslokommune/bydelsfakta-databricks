"""Databricks job entry point for the neets dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.geography import bydel_by_id, delbydel_by_id
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import TemplateA, TemplateB


def _resolve_neets_geo(geo_num):
    """Resolve geography from Geografi_num.

    The neets Excel has delbydel IDs (102-1506), bydel codes
    (30101-30117), special codes (30199), and Oslo (100000).
    """
    num = int(geo_num)

    # Oslo i alt
    if num == 100000:
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": "00",
            "bydel_navn": "Oslo i alt",
        }

    # 301XX bydel-level codes
    s = str(num)
    if s.startswith("301") and len(s) == 5:
        bydel_id = s[3:].zfill(2)
        bydel = bydel_by_id(bydel_id)
        if bydel_id == "99":
            bydel_navn = "Uten registrert adresse"
        elif bydel:
            bydel_navn = bydel["name"]
        else:
            bydel_navn = None
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": bydel_id,
            "bydel_navn": bydel_navn,
        }

    # Delbydel numeric ID
    delbydel = delbydel_by_id(num)
    if delbydel:
        bydel = bydel_by_id(delbydel["bydel_id"])
        return {
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": bydel["name"] if bydel else None,
        }

    return None


def read_neets_excel(path):
    """Read neets Excel from S3, resolve geography.

    Input columns (Excel):
        Geografi_num, Geografi, aar,
        Andel NEETs (15-29 år), Antall NEETs

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        andel, antall
    """
    df = read_excel(path, date_column="aar")

    geo = df["Geografi_num"].map(_resolve_neets_geo)
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"], na_action="ignore")
    df["delbydel_navn"] = geo.map(
        lambda g: g["delbydel_navn"], na_action="ignore"
    )
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"], na_action="ignore")
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"], na_action="ignore")

    # Drop rows that couldn't be resolved
    df = df.dropna(subset=["bydel_id"])

    df = df.rename(
        columns={
            "Andel NEETs (15-29 år)": "andel",
            "Antall NEETs": "antall",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "andel",
            "antall",
        ]
    ]


def transform_neets(df, type_of_ds):
    """Pure transformation, ported from functions/neets.py."""
    df = df.rename(columns={"andel": "antall_ratio"})

    metadata = Metadata(
        heading="NEETs",
        series=[{"heading": "NEETs", "subheading": ""}],
    )

    if type_of_ds == "status":
        [df] = transform.status(df)
        metadata.add_scale(get_min_max_values_and_ratios(df, "antall"))
        template = TemplateA()
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=df,
        values=["antall"],
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
    df = read_neets_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/neets.csv")

    output = transform_neets(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
