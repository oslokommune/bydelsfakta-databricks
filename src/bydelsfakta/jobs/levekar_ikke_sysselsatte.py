"""Databricks job entry point for the levekar_ikke_sysselsatte dataset.

Reads two Excel files (sysselsatte + befolkning-etter-kjonn-og-alder) from S3,
resolves geography, computes non-employed population, transforms through the
standard bydelsfakta aggregation pipeline, and writes per-district JSON files
back to S3.
"""

import argparse

import pandas as pd

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate
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
from bydelsfakta.population_utils import generate_population_df
from bydelsfakta.templates import TemplateA, TemplateB


def read_sysselsatte_excel(path):
    """Read sysselsatte Excel from S3, resolve geography.

    Input columns (Excel):
        År, Bydel, Delbydel, Antall sysselsatte

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        antall_sysselsatte
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
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

    # Clean "Antall sysselsatte": remove spaces from values
    col = "Antall sysselsatte"
    df[col] = df[col].astype(str).str.replace(" ", "", regex=False)
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns={col: "antall_sysselsatte"})

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "antall_sysselsatte",
        ]
    ]


def _resolve_befolkning_geo(df):
    """Resolve geography for befolkning-etter-kjonn-og-alder.

    Bydel column has 301XX codes, Delbydel has numeric IDs.
    """
    delbydel = df["Delbydel"].map(
        lambda v: delbydel_by_id(int(v)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    def _bydel_from_code(val):
        bydel_str = str(int(val))
        if bydel_str.startswith("301"):
            local = bydel_str[3:]
            bydel_id = local.zfill(2)
        else:
            bydel_id = bydel_str.zfill(2)
        return bydel_by_id(bydel_id)

    bydel = df["Bydel"].map(_bydel_from_code)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")
    return df


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel from S3.

    Input columns (Excel):
        År, Bydel, Delbydel, Kjønn, Alder, Antall personer

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        kjonn, "0" through "120"
    """
    df = read_excel(path, date_column="År")
    df = _resolve_befolkning_geo(df)

    df = df.rename(
        columns={
            "Kjønn": "kjonn",
            "Alder": "alder",
            "Antall personer": "antall_personer",
        }
    )

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

    df.columns.name = None

    # Ensure age columns are strings
    age_cols = [
        c
        for c in df.columns
        if c
        not in {
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "kjonn",
        }
    ]
    df = df.rename(columns={c: str(c) for c in age_cols})

    # Drop rows without resolved geography
    df = df.dropna(subset=["bydel_id"])

    return df


def transform_ikke_sysselsatte(sysselsatte_df, befolkning_df, type_of_ds):
    """Pure transform, ported from levekar_ikke_sysselsatte."""
    data_point = "antall_ikke_sysselsatte"
    population_col = "population"

    # Population for age 30-59
    pop_df = generate_population_df(befolkning_df, min_age=30, max_age=59)

    # Sysselsatte measured Q4, befolkning measured 1.1 next year
    pop_df["date"] = pop_df["date"] - 1

    sub_districts = pop_df["delbydel_id"].unique()
    sysselsatte_df = sysselsatte_df[
        sysselsatte_df["delbydel_id"].isin(sub_districts)
    ]

    merged = pd.merge(
        sysselsatte_df,
        pop_df[["date", "delbydel_id", "population"]],
        how="inner",
        on=["date", "delbydel_id"],
    )

    # Ignore Sentrum, Marka, Uten registrert adresse
    ignore_districts = ["16", "17", "99"]
    merged = merged[~merged["bydel_id"].isin(ignore_districts)]

    merged[data_point] = merged[population_col] - merged["antall_sysselsatte"]

    agg = Aggregate({population_col: "sum", data_point: "sum"})
    aggregated = agg.aggregate(merged)
    aggregated = agg.add_ratios(
        aggregated,
        data_points=[data_point],
        ratio_of=[population_col],
    )

    series = [{"heading": "Ikke sysselsatte", "subheading": ""}]

    if type_of_ds == "status":
        scale = get_min_max_values_and_ratios(aggregated, data_point)
        [aggregated] = transform.status(aggregated)
        template = TemplateA()
    elif type_of_ds == "historisk":
        scale = []
        [aggregated] = transform.historic(aggregated)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading="Antall ikke-sysselsatte personer mellom 30-59 år",
        series=series,
        scale=scale,
    )

    return Output(
        df=aggregated,
        values=[data_point],
        template=template,
        metadata=metadata,
    ).generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--befolkning-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status", "historisk"],
    )
    args = parser.parse_args()

    sysselsatte_path = resolve_input(args.input_dir)
    befolkning_path = resolve_input(args.befolkning_dir)

    sysselsatte_df = read_sysselsatte_excel(sysselsatte_path)
    befolkning_df = read_befolkning_excel(befolkning_path)

    if args.silver_dir:
        write_csv_output(
            sysselsatte_df,
            f"{args.silver_dir}/sysselsatte.csv",
        )
        write_csv_output(
            befolkning_df,
            f"{args.silver_dir}/befolkning.csv",
        )

    output = transform_ikke_sysselsatte(
        sysselsatte_df, befolkning_df, args.type_of_ds
    )
    write_json_output(output, args.output_dir)
