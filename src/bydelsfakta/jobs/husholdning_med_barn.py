"""Databricks job entry point for the husholdning_med_barn dataset.

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
from bydelsfakta.templates import TemplateA, TemplateB, TemplateC

METADATA = {
    "alle_status": Metadata(
        heading="Husholdninger med barn",
        series=[
            {
                "heading": "Husholdninger",
                "subheading": "med 1 barn",
            },
            {
                "heading": "Husholdninger",
                "subheading": "med 2 barn",
            },
            {
                "heading": "Husholdninger",
                "subheading": "med 3 barn eller flere",
            },
        ],
    ),
    "alle_historisk": Metadata(
        heading="Husholdninger med barn",
        series=[
            {
                "heading": "Husholdninger",
                "subheading": "med 1 barn",
            },
            {
                "heading": "Husholdninger",
                "subheading": "med 2 barn",
            },
            {
                "heading": "Husholdninger",
                "subheading": "med 3 barn eller flere",
            },
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "1barn_status": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "1barn_historisk": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "2barn_status": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "2barn_historisk": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "3barn_status": Metadata(heading="Husholdninger med 3 barn", series=[]),
    "3barn_historisk": Metadata(heading="Husholdninger med 3 barn", series=[]),
}

VALUE_CATEGORY = {
    "alle_status": ["one_child", "two_child", "three_or_more"],
    "alle_historisk": [
        "one_child",
        "two_child",
        "three_or_more",
        "total",
    ],
    "1barn_status": ["one_child"],
    "1barn_historisk": ["one_child"],
    "2barn_status": ["two_child"],
    "2barn_historisk": ["two_child"],
    "3barn_status": ["three_or_more"],
    "3barn_historisk": ["three_or_more"],
}

DATA_POINTS = [
    "one_child",
    "two_child",
    "three_or_more",
    "no_children",
    "total",
]

TYPE_OF_DS_CHOICES = [
    "alle_status",
    "alle_historisk",
    "1barn_status",
    "1barn_historisk",
    "2barn_status",
    "2barn_historisk",
    "3barn_status",
    "3barn_historisk",
]


def read_husholdning_med_barn_excel(path):
    """Read husholdninger-med-barn Excel from S3, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Barn i husholdningen, Antall

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        one_child, two_child, three_or_more, no_children
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(delbydel_by_name, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel from delbydel's parent, fall back to name
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )
    bydel_via_name = df["Bydel"].map(lambda n: bydel_by_name(str(n)))
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    # Pivot "Barn i husholdningen" into columns
    df = df.pivot_table(
        values="Antall",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="Barn i husholdningen",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Flatten MultiIndex columns from pivot
    df.columns.name = None

    df = df.rename(
        columns={
            "1 barn i HH": "ett_barn_i_hh",
            "2 barn i HH": "to_barn_i_hh",
            "3 barn i HH": "tre_barn_i_hh",
            "4 barn eller mer": "fire_barn_eller_mer",
            "Ingen barn i HH": "ingen_barn_i_hh",
        }
    )

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df


def transform_husholdning_med_barn(df, type_of_ds):
    """Pure transformation logic, ported from husholdning_med_barn.py."""
    df = df.copy()

    # Compute derived columns
    df["one_child"] = df["ett_barn_i_hh"]
    df["two_child"] = df["to_barn_i_hh"]
    df["three_or_more"] = df["tre_barn_i_hh"] + df["fire_barn_eller_mer"]
    df["no_children"] = df["ingen_barn_i_hh"]

    df = df.drop(
        columns=[
            "ett_barn_i_hh",
            "to_barn_i_hh",
            "tre_barn_i_hh",
            "fire_barn_eller_mer",
            "ingen_barn_i_hh",
        ]
    )

    df["total"] = (
        df["one_child"]
        + df["two_child"]
        + df["three_or_more"]
        + df["no_children"]
    )

    agg = Aggregate("sum")
    df = agg.aggregate(df)
    df = agg.add_ratios(df, data_points=DATA_POINTS, ratio_of=["total"])

    if type_of_ds == "alle_status":
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "alle_historisk":
        [out_df] = transform.historic(df)
        template = TemplateC()
    elif type_of_ds == "1barn_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "one_child")
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "1barn_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "2barn_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "two_child")
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "2barn_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "3barn_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "three_or_more")
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "3barn_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=out_df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=VALUE_CATEGORY[type_of_ds],
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
    df = read_husholdning_med_barn_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/husholdninger-med-barn.csv")

    output = transform_husholdning_med_barn(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
