"""Databricks job entry point for the innvandring_under16 dataset.

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

METADATA = {
    "en-innvandrer_historisk": Metadata(
        heading="Personer (under 16) med én innvandrerforelder",
        series=[],
    ),
    "en-innvandrer_status": Metadata(
        heading="Personer (under 16) med én innvandrerforelder",
        series=[],
    ),
    "to-innvandrer_historisk": Metadata(
        heading="Personer (under 16) med to innvandrerforeldre",
        series=[],
    ),
    "to-innvandrer_status": Metadata(
        heading="Personer (under 16) med to innvandrerforeldre",
        series=[],
    ),
    "innvandrer_status": Metadata(
        heading="Innvandrere (under 16)",
        series=[],
    ),
    "innvandrer_historisk": Metadata(
        heading="Andel innvandrere (under 16)",
        series=[],
    ),
}

DATA_POINTS = {
    "en-innvandrer_historisk": ["en_forelder"],
    "en-innvandrer_status": ["en_forelder"],
    "to-innvandrer_historisk": ["to_foreldre"],
    "to-innvandrer_status": ["to_foreldre"],
    "innvandrer_status": ["innvandrer"],
    "innvandrer_historisk": ["innvandrer"],
}

VALUE_POINTS = ["to_foreldre", "en_forelder", "innvandrer"]

TYPE_OF_DS_CHOICES = [
    "en-innvandrer_status",
    "en-innvandrer_historisk",
    "to-innvandrer_status",
    "to-innvandrer_historisk",
    "innvandrer_status",
    "innvandrer_historisk",
]


def read_innvandring_under16_excel(path):
    """Read innvandrer-befolkningen-0-15-ar Excel, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Alder, Innvandringskategori,
        Antall personer

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        alder, plus one column per Innvandringskategori
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(delbydel_by_name, na_action="ignore")
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

    # Pivot Innvandringskategori into columns
    df = df.pivot_table(
        values="Antall personer",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "Alder",
        ],
        columns="Innvandringskategori",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Alder": "alder",
            "Uten innvandringsbakgrunn": ("uten_innvandringsbakgrunn"),
            "Innvandrer": "innvandrer",
            "Norskfødt med innvandrerforeldre": (
                "norskfodt_med_innvandrerforeldre"
            ),
            "Utenlandsfødt m/en norsk forelder": (
                "utenlandsfodt_med_en_norsk_forelder"
            ),
            "Norskfødt med en utenlandskfødt forelder": (
                "norskfodt_med_en_utenlandskfodt_forelder"
            ),
            "Født i utlandet av norskfødte foreldre": (
                "fodt_i_utlandet_av_norskfodte_foreldre"
            ),
        }
    )

    return df


def transform_innvandring_under16(df, type_of_ds):
    """Transformation logic, ported from innvandring_under16.py."""
    df = df[df["alder"] == "0-15 år"].reset_index(drop=True)

    df["en_forelder"] = (
        df["norskfodt_med_en_utenlandskfodt_forelder"]
        + df["utenlandsfodt_med_en_norsk_forelder"]
    )
    df = df.rename(
        columns={
            "norskfodt_med_innvandrerforeldre": "to_foreldre",
        }
    )

    df["total"] = (
        df["fodt_i_utlandet_av_norskfodte_foreldre"]
        + df["innvandrer"]
        + df["uten_innvandringsbakgrunn"]
        + df["to_foreldre"]
        + df["en_forelder"]
    )

    agg = Aggregate(
        {
            "to_foreldre": "sum",
            "en_forelder": "sum",
            "innvandrer": "sum",
            "total": "sum",
        }
    )

    df = agg.aggregate(df)
    df = agg.add_ratios(df, VALUE_POINTS, ["total"])

    data_points = DATA_POINTS[type_of_ds]

    if type_of_ds.endswith("_historisk"):
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds.endswith("_status"):
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, data_points[0])
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=out_df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=data_points,
    ).generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=TYPE_OF_DS_CHOICES,
    )
    args = parser.parse_args()

    input_path = resolve_input(args.input_dir)
    df = read_innvandring_under16_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/innvandrer-befolkningen-0-15-ar.csv",
        )

    output = transform_innvandring_under16(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
