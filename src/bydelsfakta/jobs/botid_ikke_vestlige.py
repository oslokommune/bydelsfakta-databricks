"""Databricks job entry point for the botid_ikke_vestlige dataset.

Reads Excel files from S3, resolves geography, transforms the data through the
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

GRAPH_METADATA = Metadata(
    heading="Ikke-vestlige innvandrere korttid",
    series=[
        {
            "heading": (
                "Innvandrere fra Asia, Afrika, Latin-Amerika"
                " og Øst-Europa utenfor EU med botid"
                " kortere enn fem år"
            ),
            "subheading": "",
        }
    ],
)


def _resolve_geo_from_name(df, delbydel_col, bydel_col):
    """Resolve geography from Delbydel and Bydel name columns."""
    delbydel = df[delbydel_col].map(delbydel_by_name, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )
    bydel_via_name = df[bydel_col].map(
        lambda n: (bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}"))
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")
    return df


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


def read_botid_ikke_vestlige_excel(path):
    """Read botid-ikke-vestlige Excel, resolve geography, pivot.

    Input columns (Excel):
        År, Bydel, Delbydel, Botid, Landbakgrunn, antall

    Pivots Landbakgrunn into columns.
    """
    df = read_excel(path, date_column="År")
    df = _resolve_geo_from_name(df, "Delbydel", "Bydel")

    # Pivot Landbakgrunn into columns
    df = df.pivot_table(
        values="antall",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "Botid",
        ],
        columns="Landbakgrunn",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Botid": "botid",
            ("Asia, Afrika, Latin-Amerika og Øst-Europa utenfor EU"): (
                "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu"
            ),
            "Norge": "norge",
            ("Vest-Europa, Usa, Canada, Australia og New Zealand"): (
                "vest_europa_usa_canada_australia_og_new_zealand"
            ),
            "Øst-europeiske EU-land": "ost_europeiske_eu_land",
        }
    )

    return df


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel, resolve geo.

    Pivots Alder into columns to match the format expected by
    generate_population_df.
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

    # Filter rows without valid geography
    df = df.dropna(subset=["delbydel_id"])

    return df


def _pivot_botid(df, value_column):
    """Pivot botid column, keeping value_column values split by botid.

    Returns a DataFrame with botid values as separate columns.
    """
    key_columns = [c for c in df.columns if c not in ["botid", value_column]]
    df_pivot = pd.concat(
        (
            df[key_columns],
            df.pivot(columns="botid", values=value_column),
        ),
        axis=1,
    )
    return df_pivot.groupby(key_columns).sum().reset_index()


def transform_botid_ikke_vestlige(botid_df, befolkning_df, type_of_ds):
    """Transformation logic, ported from botid_ikke_vestlige.py."""
    data_point = "ikke_vestlig_kort"
    kort_botid = "Innvandrer, kort botid (<=5 år)"
    ikke_vestlig = "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu"

    # Pivot on botid using the ikke_vestlig column as value
    df = _pivot_botid(botid_df, ikke_vestlig)
    df[data_point] = df[kort_botid]
    df = Aggregate({data_point: "sum"}).aggregate(df)

    # Generate population from befolkning data
    population_df = generate_population_df(befolkning_df)
    population_district_df = Aggregate({"population": "sum"}).aggregate(
        df=population_df
    )

    df = pd.merge(
        df,
        population_district_df[
            ["date", "bydel_id", "delbydel_id", "population"]
        ],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df = Aggregate({}).add_ratios(
        df=df, data_points=[data_point], ratio_of=["population"]
    )

    series = [
        {
            "heading": (
                "Innvandrere fra Asia, Afrika, Latin-Amerika"
                " og Øst-Europa utenfor EU med botid"
                " kortere enn fem år"
            ),
            "subheading": "",
        }
    ]

    if type_of_ds == "historisk":
        scale = []
        [out_df] = transform.historic(df)
        template = TemplateB()
    elif type_of_ds == "status":
        scale = get_min_max_values_and_ratios(df, data_point)
        [out_df] = transform.status(df)
        template = TemplateA()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    metadata = Metadata(
        heading="Ikke-vestlige innvandrere korttid",
        series=series,
        scale=scale,
    )

    return Output(
        df=out_df,
        template=template,
        metadata=metadata,
        values=[data_point],
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

    botid_path = resolve_input(args.input_dir)
    befolkning_path = resolve_input(args.befolkning_dir)

    botid_df = read_botid_ikke_vestlige_excel(botid_path)
    befolkning_df = read_befolkning_excel(befolkning_path)

    if args.silver_dir:
        write_csv_output(
            botid_df,
            f"{args.silver_dir}/botid-ikke-vestlige.csv",
        )
        write_csv_output(
            befolkning_df,
            f"{args.silver_dir}/befolkning.csv",
        )

    output = transform_botid_ikke_vestlige(
        botid_df, befolkning_df, args.type_of_ds
    )
    write_json_output(output, args.output_dir)
