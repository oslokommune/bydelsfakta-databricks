"""Databricks job entry point for the alder_distribusjon_status dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate
from bydelsfakta.geography import (
    bydel_by_id,
    delbydel_by_id,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output
from bydelsfakta.templates import TemplateE

AGES = [str(i) for i in range(0, 121)]


def _resolve_geo(bydel_val, delbydel_val):
    """Resolve geography from Bydel (30101 format) and Delbydel.

    Bydel column contains 301XX codes:
    - 30101 -> bydel "01", 30199 -> bydel "99", etc.

    Delbydel column contains numeric IDs for delbydel_by_id.
    """
    bydel_str = str(int(bydel_val))

    # Extract bydel ID from 301XX format
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


def read_alder_distribusjon_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Kjønn, Alder, Antall personer

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        kjonn, "0", "1", ..., "120"
    """
    df = read_excel(path, date_column="År")

    # Resolve geography
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

    # Convert Alder to string for column names after pivot
    df["alder"] = df["alder"].astype(str)

    # Pivot age into columns
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

    # Flatten MultiIndex columns from pivot
    df.columns.name = None

    # Ensure all age columns 0-120 exist (fill missing with 0)
    for age in AGES:
        if age not in df.columns:
            df[age] = 0

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df


def transform_alder_distribusjon(df):
    """Pure transformation logic, ported from alder_distribusjon_status.

    Always status only (no historisk variant).
    """
    agg = Aggregate("sum")
    [df] = transform.status(df)
    df["total"] = df.loc[:, "0":"120"].sum(axis=1)
    aggregated = agg.aggregate(df, extra_groupby_columns=["kjonn"])

    heading = "Aldersdistribusjon fordelt på kjønn"
    series = [
        {
            "heading": "Aldersdistribusjon fordelt på kjønn",
            "subheading": "",
        }
    ]

    return Output(
        df=aggregated,
        template=TemplateE(),
        metadata=Metadata(heading=heading, series=series),
        values=AGES,
    ).generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status"],
    )
    args = parser.parse_args()

    input_path = resolve_input(args.input_dir)
    df = read_alder_distribusjon_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/befolkning-etter-kjonn-og-alder.csv",
        )

    output = transform_alder_distribusjon(df)
    write_json_output(output, args.output_dir)
