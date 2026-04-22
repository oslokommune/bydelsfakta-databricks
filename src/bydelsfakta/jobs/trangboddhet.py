"""Databricks job entry point for the trangboddhet dataset.

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
from bydelsfakta.output import Metadata, Output, get_min_max_values_and_ratios
from bydelsfakta.templates import (
    TemplateA,
    TemplateB,
    TemplateC,
    TemplateJ,
)

METADATA = {
    "0-5-0-9_status": Metadata(heading="0,5–0,9 rom per person", series=[]),
    "0-5-0-9_historisk": Metadata(heading="0,5–0,9 rom per person", series=[]),
    "1-0-1-9_status": Metadata(heading="1,0–1,9 rom per person", series=[]),
    "1-0-1-9_historisk": Metadata(
        heading="Personer per rom - 1,5 - 1,9", series=[]
    ),
    "over-2_status": Metadata(
        heading="2 rom eller flere per person", series=[]
    ),
    "over-2_historisk": Metadata(
        heading="2 rom eller flere per person", series=[]
    ),
    "under-0-5_status": Metadata(heading="Under 0,5 rom per preson", series=[]),
    "under-0-5_historisk": Metadata(
        heading="Personer per rom - under 0.5", series=[]
    ),
    "alle_status": Metadata(
        heading="Husstander fordelt på personer per rom",
        series=[
            {
                "heading": "Under 0,5 rom per person",
                "subheading": "",
            },
            {
                "heading": "0,5–0,9 rom per person",
                "subheading": "",
            },
            {
                "heading": "1,0–1,9 rom per person",
                "subheading": "",
            },
            {
                "heading": "2 rom eller flere per person",
                "subheading": "",
            },
        ],
    ),
    "alle_historisk": Metadata(
        heading="",
        series=[
            {
                "heading": "Under 0,5 rom per person",
                "subheading": "",
            },
            {
                "heading": "0,5–0,9 rom per person",
                "subheading": "",
            },
            {
                "heading": "1,0–1,9 rom per person",
                "subheading": "",
            },
            {
                "heading": "2 rom eller flere per person",
                "subheading": "",
            },
        ],
    ),
}

DATA_POINTS = {
    "0-5-0-9_status": ["rom_per_person_0_5_til_0_9"],
    "0-5-0-9_historisk": ["rom_per_person_0_5_til_0_9"],
    "1-0-1-9_status": ["rom_per_person_1_0_til_1_9"],
    "1-0-1-9_historisk": ["rom_per_person_1_0_til_1_9"],
    "over-2_status": ["rom_per_person_2_0_og_over"],
    "over-2_historisk": ["rom_per_person_2_0_og_over"],
    "under-0-5_status": ["rom_per_person_under_0_5"],
    "under-0-5_historisk": ["rom_per_person_under_0_5"],
    "alle_status": [
        "rom_per_person_under_0_5",
        "rom_per_person_0_5_til_0_9",
        "rom_per_person_1_0_til_1_9",
        "rom_per_person_2_0_og_over",
    ],
    "alle_historisk": [
        "rom_per_person_under_0_5",
        "rom_per_person_0_5_til_0_9",
        "rom_per_person_1_0_til_1_9",
        "rom_per_person_2_0_og_over",
    ],
}

VALUE_POINTS = [
    "rom_per_person_under_0_5",
    "rom_per_person_0_5_til_0_9",
    "rom_per_person_1_0_til_1_9",
    "rom_per_person_2_0_og_over",
]

TYPE_OF_DS_CHOICES = [
    "0-5-0-9_status",
    "0-5-0-9_historisk",
    "1-0-1-9_status",
    "1-0-1-9_historisk",
    "over-2_status",
    "over-2_historisk",
    "under-0-5_status",
    "under-0-5_historisk",
    "alle_status",
    "alle_historisk",
]


def _resolve_geo(geo_id):
    """Resolve geography from a numeric Geografi code.

    Special codes:
    - 10000: Oslo i alt (bydel 00, no delbydel)
    - Other values: delbydel numeric IDs
    """
    geo_int = int(geo_id)

    if geo_int == 10000:
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": "00",
            "bydel_navn": "Oslo i alt",
        }

    delbydel = delbydel_by_id(geo_int)
    if delbydel:
        bydel = bydel_by_id(delbydel["bydel_id"])
        return {
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": (bydel["name"] if bydel else None),
        }

    return None


def read_trangboddhet_excel(path):
    """Read trangboddhet Excel from S3, resolve geography.

    Input columns (Excel):
        År, Geografi, Rom per person - Under 0,5,
        Rom per person - 0,5 - 0,9, Rom per person - 1,0 - 1,9,
        Rom per person - 2,0 og over, Ukjent antall rom,
        Rom per person - I alt

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        rom_per_person_under_0_5, rom_per_person_0_5_til_0_9,
        rom_per_person_1_0_til_1_9, rom_per_person_2_0_og_over,
        rom_per_person_i_alt
    """
    df = read_excel(path, date_column="År")

    # Resolve geography from numeric Geografi column
    geo = df["Geografi"].map(_resolve_geo)
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"], na_action="ignore")
    df["delbydel_navn"] = geo.map(
        lambda g: g["delbydel_navn"], na_action="ignore"
    )
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"], na_action="ignore")
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"], na_action="ignore")

    df = df.rename(
        columns={
            "Rom per person - Under 0,5": "rom_per_person_under_0_5",
            "Rom per person - 0,5 - 0,9": "rom_per_person_0_5_til_0_9",
            "Rom per person - 1,0 - 1,9": "rom_per_person_1_0_til_1_9",
            "Rom per person - 2,0 og over": "rom_per_person_2_0_og_over",
            "Rom per person - I alt": "rom_per_person_i_alt",
        }
    )

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "rom_per_person_under_0_5",
            "rom_per_person_0_5_til_0_9",
            "rom_per_person_1_0_til_1_9",
            "rom_per_person_2_0_og_over",
            "rom_per_person_i_alt",
        ]
    ]


def transform_trangboddhet(df, type_of_ds):
    """Pure transformation logic, ported from functions/trangboddhet.py."""
    agg = Aggregate(
        {
            "rom_per_person_under_0_5": "sum",
            "rom_per_person_0_5_til_0_9": "sum",
            "rom_per_person_1_0_til_1_9": "sum",
            "rom_per_person_2_0_og_over": "sum",
            "rom_per_person_i_alt": "sum",
        }
    )

    # Filter out bydel_id "00" before aggregation
    df = df[df["bydel_id"] != "00"]
    df = agg.aggregate(df)
    df = agg.add_ratios(df, VALUE_POINTS, VALUE_POINTS)

    if type_of_ds == "0-5-0-9_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "0-5-0-9_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "1-0-1-9_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "1-0-1-9_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "over-2_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "over-2_historisk":
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "under-0-5_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        [out_df] = transform.status(df)
        template = TemplateA()
    elif type_of_ds == "under-0-5_historisk":
        # NOTE: Original uses TemplateA here, not TemplateB
        [out_df] = transform.historic(df)
        template = TemplateA()
    elif type_of_ds == "alle_status":
        [out_df] = transform.status(df)
        template = TemplateJ()
    elif type_of_ds == "alle_historisk":
        [out_df] = transform.historic(df)
        template = TemplateC()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=out_df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=DATA_POINTS[type_of_ds],
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
    df = read_trangboddhet_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/husholdninger-etter-rom-per-person.csv",
        )

    output = transform_trangboddhet(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
