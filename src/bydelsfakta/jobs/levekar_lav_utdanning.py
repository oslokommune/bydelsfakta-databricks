"""Databricks job entry point for the levekar_lav_utdanning dataset.

Reads an Excel file from S3, resolves geography, pivots education levels,
transforms the data through the standard bydelsfakta aggregation pipeline, and
writes per-district JSON files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate
from bydelsfakta.geography import (
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

EDUCATION_CATEGORIES = [
    "ingen_utdanning_uoppgitt",
    "grunnskole",
    "videregaende",
    "universitet_hogskole_kort",
    "universitet_hogskole_lang",
]


def read_lav_utdanning_excel(path):
    """Read lav-utdanning Excel from S3, resolve geography, pivot.

    Input columns (Excel):
        År, Bydel, Delbydel, Høyeste fullførte utdanning,
        Antall personer

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        ingen_utdanning_uoppgitt, grunnskole, videregaende,
        universitet_hogskole_kort, universitet_hogskole_lang
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel from name, trying "Bydel " prefix
    bydel = df["Bydel"].map(
        lambda n: (bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}"))
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    # Pivot education levels into columns
    df = df.pivot_table(
        values="Antall personer",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="Høyeste fullførte utdanning",
        aggfunc="sum",
    ).reset_index()

    # Flatten column index after pivot
    df.columns.name = None

    # Rename pivoted education columns to internal names
    col_map = {
        "Ingen utdanning/Uoppgitt utdanning": "ingen_utdanning_uoppgitt",
        "Grunnskole": "grunnskole",
        "Videregående skolenivå": "videregaende",
        "Universitets- og høgskolenivå, kort": ("universitet_hogskole_kort"),
        "Universitets- og høgskolenivå, lang": ("universitet_hogskole_lang"),
    }
    df = df.rename(columns=col_map)

    # Ensure all expected columns exist (fill with 0 if missing)
    for col in EDUCATION_CATEGORIES:
        if col not in df.columns:
            df[col] = 0

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            *EDUCATION_CATEGORIES,
        ]
    ]


def transform_lav_utdanning(df, type_of_ds):
    """Pure transform logic, ported from levekar_lav_utdanning."""
    data_point = "lav_utdanning"

    df["total"] = df[EDUCATION_CATEGORIES].sum(axis=1)
    df[data_point] = df["ingen_utdanning_uoppgitt"] + df["grunnskole"]

    aggregations = {data_point: "sum", "total": "sum"}
    agg = Aggregate(aggregations)
    df = agg.aggregate(df)
    df = agg.add_ratios(df, data_points=[data_point], ratio_of=["total"])

    series = [
        {
            "heading": ("Kun grunnskole eller ingen utdanning oppgitt"),
            "subheading": "",
        }
    ]

    if type_of_ds == "status":
        scale = get_min_max_values_and_ratios(df, data_point)
        [df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "historisk":
        scale = []
        [df] = transform.historic(df)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading=("Personer mellom 30-59 år med lav utdanning"),
        series=series,
        scale=scale,
    )

    return Output(
        df=df,
        values=[data_point],
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
    df = read_lav_utdanning_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/lav-utdanning.csv")

    output = transform_lav_utdanning(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
