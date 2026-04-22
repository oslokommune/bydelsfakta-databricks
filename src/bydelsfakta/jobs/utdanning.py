"""Databricks job entry point for the utdanning dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate
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
from bydelsfakta.templates import TemplateA, TemplateC

DATA_POINTS_STATUS = ["grunnskole", "vgs", "uni_kort", "uni_lang", "ingen"]
DATA_POINTS_ALL = [*DATA_POINTS_STATUS, "total"]


def read_utdanning_excel(path):
    """Read utdanning Excel from S3, resolve geography, rename columns.

    Input columns (SSB format):
        aargang, delbydel2017_01, bydel2_fmt, bu1_gr5_imp_fmt,
        kjoenn_fmt, aldgr06a__fmt, antall

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        nivaa, kjonn, alder, antall
    """
    df = read_excel(path, date_column="aargang")

    # Resolve delbydel from numeric ID
    delbydel = df["delbydel2017_01"].map(delbydel_by_id, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to name
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]), na_action="ignore"
    )
    bydel_via_name = df["bydel2_fmt"].map(
        lambda n: (bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}"))
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(
        columns={
            "bu1_gr5_imp_fmt": "nivaa",
            "kjoenn_fmt": "kjonn",
            "aldgr06a__fmt": "alder",
        }
    )

    # Keep only the columns the transform expects
    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "nivaa",
            "kjonn",
            "alder",
            "antall",
        ]
    ]


def transform_utdanning(df, type_of_ds):
    """Pure transformation logic, ported from functions/utdanning.py."""

    # Aggregate on "nivaa", disregarding "kjoenn" and "alder" for now.
    df = df.pivot_table(
        values="antall",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "nivaa",
        ],
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Explode the "nivaa" column.
    df = df.pivot_table(
        values="antall",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="nivaa",
        aggfunc="sum",
    ).reset_index()

    df = df.rename(
        columns={
            "Grunnskole": "grunnskole",
            "Videregående skolenivå": "vgs",
            "Universitets- og høgskolenivå, kort": "uni_kort",
            "Universitets- og høgskolenivå, lang": "uni_lang",
            "Ingen utdanning/Uoppgitt utdanning": "ingen",
        }
    )

    df["total"] = (
        df["grunnskole"]
        + df["vgs"]
        + df["uni_kort"]
        + df["uni_lang"]
        + df["ingen"]
    )

    agg = Aggregate("sum")
    df = agg.aggregate(df)
    aggregated = agg.add_ratios(
        df, data_points=DATA_POINTS_ALL, ratio_of=["total"]
    )
    scale = get_min_max_values_and_ratios(aggregated, "total")
    series = [
        {"heading": "Grunnskole", "subheading": ""},
        {"heading": "Videregående skolenivå", "subheading": ""},
        {"heading": "Universitets- og høgskolenivå", "subheading": "kort"},
        {"heading": "Universitets- og høgskolenivå", "subheading": "lang"},
        {"heading": "Ingen/uoppgitt utdanning", "subheading": ""},
    ]

    if type_of_ds == "status":
        [df] = transform.status(df)
        template = TemplateA()
        data_points = DATA_POINTS_STATUS
    elif type_of_ds == "historisk":
        [df] = transform.historic(df)
        template = TemplateC()
        data_points = DATA_POINTS_ALL
        series += [{"heading": "Totalt", "subheading": ""}]
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(heading="Utdanning", series=series, scale=scale)

    return Output(
        df=df, values=data_points, template=template, metadata=metadata
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
    df = read_utdanning_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/utdanning.csv")

    output = transform_utdanning(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
