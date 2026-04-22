"""Databricks job entry point for the husholdningstyper dataset.

Reads an Excel file from S3, resolves geography, transforms the data through
the standard bydelsfakta aggregation pipeline, and writes per-district JSON
files back to S3.
"""

import argparse

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate, ColumnNames
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
from bydelsfakta.templates import TemplateA, TemplateC

DATA_POINTS_ALL = [
    "aleneboende",
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
    "flerfamiliehusholdninger",
    "total",
]

DATA_POINTS_STATUS = [
    "aleneboende",
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
    "flerfamiliehusholdninger",
]

column_names = ColumnNames()


def read_husholdningstyper_excel(path):
    """Read husholdningstyper Excel from S3, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Husholdningstype, Antall

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        aleneboende, par_uten_barn, par_med_barn,
        mor_far_med_barn, flerfamiliehusholdninger
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

    # Pivot Husholdningstype into columns
    df = df.pivot_table(
        values="Antall",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="Husholdningstype",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Flatten MultiIndex columns from pivot
    df.columns.name = None

    df = df.rename(
        columns={
            "Aleneboende": "aleneboende",
            "Par uten hjemmeboende barn": "par_uten_hjemmeboende_barn",
            "Par med små barn": "par_med_smaa_barn",
            "Par med store barn": "par_med_store_barn",
            "Mor/far med små barn": "mor_eller_far_med_smaa_barn",
            "Mor/far med store barn": "mor_eller_far_med_store_barn",
            "Enfamiliehusholdninger med voksne barn": (
                "enfamiliehusholdninger_med_voksne_barn"
            ),
            "Flerfamiliehusholdninger med små barn": (
                "flerfamiliehusholdninger_med_smaa_barn"
            ),
            "Flerfamiliehusholdninger med store barn": (
                "flerfamiliehusholdninger_med_store_barn"
            ),
            "Flerfamiliehusholdninger uten barn 0-17 år": (
                "flerfamiliehusholdninger_uten_barn_0_til_17_aar"
            ),
        }
    )

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df


def transform_husholdningstyper(df, type_of_ds):
    """Pure transformation logic, ported from husholdningstyper.py."""
    df = df.copy()

    # Compute derived columns
    df["par_uten_barn"] = df["par_uten_hjemmeboende_barn"]
    df["par_med_barn"] = df["par_med_smaa_barn"] + df["par_med_store_barn"]
    df["mor_far_med_barn"] = (
        df["mor_eller_far_med_smaa_barn"] + df["mor_eller_far_med_store_barn"]
    )
    df["flerfamiliehusholdninger"] = (
        df["enfamiliehusholdninger_med_voksne_barn"]
        + df["flerfamiliehusholdninger_med_smaa_barn"]
        + df["flerfamiliehusholdninger_med_store_barn"]
        + df["flerfamiliehusholdninger_uten_barn_0_til_17_aar"]
    )

    df = df.drop(
        columns=[
            "par_uten_hjemmeboende_barn",
            "par_med_smaa_barn",
            "par_med_store_barn",
            "mor_eller_far_med_smaa_barn",
            "mor_eller_far_med_store_barn",
            "enfamiliehusholdninger_med_voksne_barn",
            "flerfamiliehusholdninger_med_store_barn",
            "flerfamiliehusholdninger_med_smaa_barn",
            "flerfamiliehusholdninger_uten_barn_0_til_17_aar",
        ]
    )

    df["total"] = (
        df["par_uten_barn"]
        + df["par_med_barn"]
        + df["mor_far_med_barn"]
        + df["flerfamiliehusholdninger"]
        + df["aleneboende"]
    )

    df = df.groupby(column_names.default_groupby_columns(), as_index=False).agg(
        "sum"
    )

    agg = Aggregate("sum")
    df = agg.aggregate(df)
    aggregated = agg.add_ratios(
        df, data_points=DATA_POINTS_ALL, ratio_of=["total"]
    )

    scale = get_min_max_values_and_ratios(aggregated, "aleneboende")

    if type_of_ds == "status":
        [out_df] = transform.status(aggregated)
        template = TemplateA()
        data_points = DATA_POINTS_STATUS
        series = [
            {
                "heading": "Énpersonshusholdninger",
                "subheading": "",
            },
            {"heading": "Par uten barn", "subheading": ""},
            {"heading": "Par med barn", "subheading": ""},
            {
                "heading": "Mor eller far",
                "subheading": "med barn",
            },
            {
                "heading": "Flerfamiliehusholdninger",
                "subheading": "",
            },
        ]
    elif type_of_ds == "historisk":
        [out_df] = transform.historic(aggregated)
        template = TemplateC()
        data_points = DATA_POINTS_ALL
        series = [
            {
                "heading": "Énpersonshusholdninger",
                "subheading": "",
            },
            {"heading": "Par uten barn", "subheading": ""},
            {"heading": "Par med barn", "subheading": ""},
            {
                "heading": "Mor eller far",
                "subheading": "med barn",
            },
            {
                "heading": "Flerfamiliehusholdninger",
                "subheading": "",
            },
            {"heading": "Totalt", "subheading": ""},
        ]
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading="Husholdninger etter husholdningstype",
        series=series,
        scale=scale,
    )

    return Output(
        df=out_df,
        values=data_points,
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
    df = read_husholdningstyper_excel(input_path)

    if args.silver_dir:
        write_csv_output(df, f"{args.silver_dir}/husholdningstyper.csv")

    output = transform_husholdningstyper(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
