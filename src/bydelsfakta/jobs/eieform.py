"""Databricks job entry point for the eieform dataset.

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
    delbydel_by_name,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import TemplateA, TemplateC

DATA_POINTS = ["leier", "andel", "selveier"]


def _resolve_delbydel(val):
    """Resolve delbydel from a name or numeric value."""
    result = delbydel_by_name(str(val))
    if result is not None:
        return result
    try:
        numeric = int(float(val))
        return delbydel_by_id(numeric)
    except (ValueError, TypeError):
        return None


def read_eieform_excel(path):
    """Read eieform Excel from S3, resolve geography, rename cols.

    Input columns (Excel):
        År, Delbydel, Selveier alle, Borettslag-andel alle,
        Leier alle, Selveier uten studenter,
        Borettslag-andel uten studenter, Leier uten studenter

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        selveier_alle, borettslag_andel_alle, leier_alle,
        selveier_uten_studenter, borettslag_andel_uten_studenter,
        leier_uten_studenter
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name or numeric ID
    delbydel = df["Delbydel"].map(_resolve_delbydel)
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to name or
    # 301XX numeric code (bydel-level rows use 30101=bydel 01, etc.)
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )

    def _bydel_fallback(val):
        # Try name first
        result = bydel_by_name(str(val))
        if result is not None:
            return result
        try:
            numeric = int(float(val))
            s = str(numeric)
            # 10000 = Oslo i alt
            if s == "10000":
                return {"id": "00", "name": "Oslo i alt"}
            # 301XX numeric code: 30101 → bydel "01"
            if s.startswith("301") and len(s) == 5:
                return bydel_by_id(s[3:].zfill(2))
        except (ValueError, TypeError):
            pass
        return None

    bydel_via_fallback = df["Delbydel"].map(_bydel_fallback)
    bydel = bydel_via_delbydel.fillna(bydel_via_fallback)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(
        columns={
            "Selveier alle": "selveier_alle",
            "Borettslag-andel alle": "borettslag_andel_alle",
            "Leier alle": "leier_alle",
            "Selveier uten studenter": "selveier_uten_studenter",
            "Borettslag-andel uten studenter": (
                "borettslag_andel_uten_studenter"
            ),
            "Leier uten studenter": "leier_uten_studenter",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "selveier_alle",
            "borettslag_andel_alle",
            "leier_alle",
            "selveier_uten_studenter",
            "borettslag_andel_uten_studenter",
            "leier_uten_studenter",
        ]
    ]


def transform_eieform(df, type_of_ds):
    """Pure transformation logic, ported from functions/eieform.py."""
    df = df.rename(
        columns={
            "selveier_alle": "selveier",
            "borettslag_andel_alle": "andel",
            "leier_alle": "leier",
        }
    )

    # Drop Sentrum (16) and Marka (17)
    df = df[~df["bydel_id"].isin(["16", "17"])].copy()
    df = df.drop_duplicates()

    # Compute ratios
    df["leier_ratio"] = df["leier"] / 100
    df["andel_ratio"] = df["andel"] / 100
    df["selveier_ratio"] = df["selveier"] / 100

    series = [
        {"heading": "Leier", "subheading": ""},
        {"heading": "Andels-/aksjeeier", "subheading": ""},
        {"heading": "Selveier", "subheading": ""},
    ]

    if type_of_ds == "status":
        scale = get_min_max_values_and_ratios(df, "leier")
        [df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "historisk":
        scale = []
        [df] = transform.historic(df)
        template = TemplateC()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading="Husholdninger fordelt etter eie-/leieforhold",
        series=series,
        scale=scale,
    )

    return Output(
        df=df,
        values=DATA_POINTS,
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
    df = read_eieform_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/eieform.csv")

    output = transform_eieform(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
