"""Databricks job entry point for befolkningsutvkl_forv_utvkl.

Reads five Excel files (befolkning-etter-kjonn-og-alder, dode, fodte, flytting-
til-etter-alder, flytting-fra-etter-alder), processes population, births,
deaths, and migration data, and writes per-district JSON files.

The original Lambda also consumed befolkningsframskrivninger (population
projections), but the frontend graphs don't use projection data, so it is
omitted here.
"""

import argparse

import numpy as np
import pandas as pd

from bydelsfakta import transform
from bydelsfakta.aggregate import Aggregate, ColumnNames, merge_all
from bydelsfakta.geography import bydel_by_name, delbydel_by_name
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output
from bydelsfakta.population_utils import generate_population_df
from bydelsfakta.templates import TemplateH

column_names = ColumnNames()
agg_sum = Aggregate("sum")


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel.

    Reuses the same reader as folkemengde — pivots age into columns
    ("0" through "120") for use with generate_population_df.
    """
    from bydelsfakta.jobs.folkemengde import read_befolkning_excel

    return read_befolkning_excel(path)


def read_dode_excel(path):
    """Read dode Excel, resolve geography.

    Input columns (Excel): År, Bydel, Delbydel, Antall døde
    Output columns: date, bydel_id, bydel_navn, delbydel_id,
                    delbydel_navn, antall_dode
    """
    df = read_excel(path, date_column="År")

    geo = df.apply(
        lambda row: _resolve_bydel_delbydel(row["Bydel"], row["Delbydel"]),
        axis=1,
    )
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"])
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"])
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"])
    df["delbydel_navn"] = geo.map(lambda g: g["delbydel_navn"])

    df = df.rename(columns={"Antall døde": "antall_dode"})
    df = df.dropna(subset=["bydel_id"])

    return df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            "delbydel_id",
            "delbydel_navn",
            "antall_dode",
        ]
    ]


def read_fodte_excel(path):
    """Read fodte Excel, resolve geography.

    Input columns (Excel): År, Bydel, Delbydel, Antall fødte
    Output columns: date, bydel_id, bydel_navn, delbydel_id,
                    delbydel_navn, antall_fodte
    """
    df = read_excel(path, date_column="År")

    geo = df.apply(
        lambda row: _resolve_bydel_delbydel(row["Bydel"], row["Delbydel"]),
        axis=1,
    )
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"])
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"])
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"])
    df["delbydel_navn"] = geo.map(lambda g: g["delbydel_navn"])

    df = df.rename(columns={"Antall fødte": "antall_fodte"})
    df = df.dropna(subset=["bydel_id"])

    return df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            "delbydel_id",
            "delbydel_navn",
            "antall_fodte",
        ]
    ]


def read_flytting_fra_excel(path):
    """Read flytting-fra-etter-alder Excel for migration totals.

    Reuses the reader from flyttehyppighet_totalt but sums across age
    groups to get district-level totals (utflytting_fra_oslo +
    utflytting_mellom_bydeler).
    """
    from bydelsfakta.jobs.flyttehyppighet_totalt import (
        read_flytting_fra_excel as _read,
    )

    return _read(path)


def read_flytting_til_excel(path):
    """Read flytting-til-etter-alder Excel for migration totals.

    Reuses the reader from flyttehyppighet_totalt but sums across age
    groups to get district-level totals (innflytting_til_oslo +
    innflytting_mellom_bydeler).
    """
    from bydelsfakta.jobs.flyttehyppighet_totalt import (
        read_flytting_til_excel as _read,
    )

    return _read(path)


def _resolve_bydel_delbydel(bydel_val, delbydel_val):
    """Resolve geography from Bydel and Delbydel name columns."""
    bydel_str = str(bydel_val).strip()
    delbydel_str = str(delbydel_val).strip()

    bydel = bydel_by_name(bydel_str) or bydel_by_name(f"Bydel {bydel_str}")
    bydel_id = bydel["id"] if bydel else None
    bydel_navn = bydel["name"] if bydel else None

    delbydel = delbydel_by_name(delbydel_str)
    if delbydel:
        return {
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": bydel_navn,
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
        }

    return {
        "bydel_id": bydel_id,
        "bydel_navn": bydel_navn,
        "delbydel_id": None,
        "delbydel_navn": None,
    }


def process_population(population):
    """Sum age columns into population, aggregate, compute pct_change."""
    population = population.dropna()
    aggregate = agg_sum.aggregate(generate_population_df(population))
    aggregate = aggregate.sort_values(
        ["bydel_id", "delbydel_id", "date"]
    ).reset_index(drop=True)

    aggregate["change"] = np.nan

    # Oslo total
    oslo_mask = aggregate["bydel_id"] == "00"
    aggregate.loc[oslo_mask, "change"] = aggregate.loc[
        oslo_mask, "population"
    ].pct_change()

    # District level (no sub-district)
    district_mask = (aggregate["bydel_id"] != "00") & (
        aggregate["delbydel_id"].isna()
    )
    district_rows = aggregate.loc[district_mask].copy()
    district_rows["change"] = district_rows.groupby("bydel_id")[
        "population"
    ].pct_change()
    aggregate.loc[district_mask, "change"] = district_rows["change"].values

    # Sub-district level
    sub_mask = aggregate["delbydel_id"].notna()
    sub_rows = aggregate.loc[sub_mask].copy()
    sub_rows["change"] = sub_rows.groupby("delbydel_id")[
        "population"
    ].pct_change()
    aggregate.loc[sub_mask, "change"] = sub_rows["change"].values

    return aggregate


def process_dead(dead):
    """Aggregate deaths at all geographic levels."""
    dead = dead.dropna()
    return agg_sum.aggregate(dead)


def process_born(born):
    """Aggregate births at all geographic levels."""
    born = born.dropna()
    return agg_sum.aggregate(born)


def process_migration(df, oslo_col, between_col, output_label):
    """Sum migration columns per district and add Oslo total.

    Migration data has no sub-district level — only district and Oslo.
    The input DataFrame has per-age-group rows, so we sum across all
    age groups first.
    """
    value_cols = [
        c
        for c in df.columns
        if c
        not in [
            "date",
            "bydel_id",
            "bydel_navn",
            "aldersgruppe_5_aar",
        ]
    ]
    district = (
        df.groupby(
            [
                column_names.date,
                column_names.district_id,
                column_names.district_name,
            ]
        )[value_cols]
        .sum()
        .reset_index()
    )
    district[output_label] = district[oslo_col] + district[between_col]
    district[column_names.sub_district_id] = np.nan
    district[column_names.sub_district_name] = np.nan

    district = district.astype(
        {"delbydel_id": object, "delbydel_navn": object, "date": int}
    )

    oslo = (
        district.groupby([column_names.date])[output_label].sum().reset_index()
    )
    oslo[column_names.sub_district_id] = np.nan
    oslo[column_names.sub_district_name] = np.nan
    oslo[column_names.district_name] = "Oslo i alt"
    oslo[column_names.district_id] = "00"

    aggregated = pd.concat([oslo, district])[
        [*column_names.default_groupby_columns(), output_label]
    ]

    return aggregated


def generate(population, dead, born, immigration, emigration):
    """Process all inputs and merge into a single DataFrame."""
    population = process_population(population)
    dead = process_dead(dead)
    born = process_born(born)
    immigration = process_migration(
        df=immigration,
        oslo_col="innflytting_til_oslo",
        between_col="innflytting_mellom_bydeler",
        output_label="immigration",
    )
    emigration = process_migration(
        df=emigration,
        oslo_col="utflytting_fra_oslo",
        between_col="utflytting_mellom_bydeler",
        output_label="emigration",
    )

    merged = merge_all(
        population, dead, born, immigration, emigration, how="outer"
    )
    merged = merged.astype(
        {
            "antall_dode": pd.Int64Dtype(),
            "antall_fodte": pd.Int64Dtype(),
            "immigration": pd.Int64Dtype(),
            "emigration": pd.Int64Dtype(),
            "population": pd.Int64Dtype(),
        }
    )
    return merged


def transform_befolkningsutvkl(population, dead, born, immigration, emigration):
    """Transform all inputs and generate output JSON."""
    dfs = transform.historic(population, dead, born, immigration, emigration)

    df = generate(*dfs)

    df = df.rename(columns={"antall_dode": "deaths", "antall_fodte": "births"})

    output = Output(
        df=df,
        values=[
            [
                "deaths",
                "births",
                "emigration",
                "immigration",
                "population",
                "change",
            ],
            [],
        ],
        template=TemplateH(),
        metadata=Metadata(
            heading="Befolkningsutvikling og fremskrivning",
            series=[
                {"heading": "Befolkningsutvikling", "subheading": ""},
                {"heading": "Befolkningsfremskrivning", "subheading": ""},
            ],
        ),
    ).generate_output()

    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--befolkning-dir", required=True)
    parser.add_argument("--dode-dir", required=True)
    parser.add_argument("--fodte-dir", required=True)
    parser.add_argument("--fra-dir", required=True)
    parser.add_argument("--til-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    population = read_befolkning_excel(resolve_input(args.befolkning_dir))
    dead = read_dode_excel(resolve_input(args.dode_dir))
    born = read_fodte_excel(resolve_input(args.fodte_dir))
    fra_df = read_flytting_fra_excel(resolve_input(args.fra_dir))
    til_df = read_flytting_til_excel(resolve_input(args.til_dir))

    if args.silver_dir:
        write_csv_output(dead, f"{args.silver_dir}/dode.csv")
        write_csv_output(born, f"{args.silver_dir}/fodte.csv")

    output = transform_befolkningsutvkl(population, dead, born, til_df, fra_df)
    write_json_output(output, args.output_dir)
