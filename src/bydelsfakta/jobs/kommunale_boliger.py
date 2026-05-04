"""Databricks job entry point for the kommunale_boliger dataset.

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
from bydelsfakta.templates import TemplateA, TemplateB

METADATA = {
    "status": Metadata(
        heading="Kommunale boliger av boligmassen i alt",
        series=[],
    ),
    "historisk": Metadata(
        heading="Kommunale boliger av boligmassen i alt",
        series=[],
    ),
}


def read_kommunale_boliger_excel(path):
    """Read kommunale-boliger Excel, resolve geography.

    Input columns (Excel):
        år, Bydelsnr, Bydel, Delbydelsnr, Delbydel, Antall boliger

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        antall_boliger
    """
    df = read_excel(path, date_column="år")

    # Resolve delbydel from numeric ID
    delbydel = df["Delbydelsnr"].map(delbydel_by_id, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel: prefer delbydel's parent, fall back to name
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )
    bydel_via_name = df["Bydel"].map(
        lambda n: bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}")
    )
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(columns={"Antall boliger": "antall_boliger"})

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "antall_boliger",
        ]
    ]


def read_boligmengde_excel(path):
    """Read boligmengde-etter-boligtype Excel, resolve geography.

    Input columns (Excel):
        År, Geografi, Boligtype, Antall boliger

    Pivots Boligtype into columns, then sums all housing types
    into a "total" column.
    """
    df = read_excel(path, date_column="År")

    # Resolve delbydel from name
    delbydel = df["Geografi"].map(delbydel_by_name, na_action="ignore")
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    # Resolve bydel from delbydel's parent
    bydel_via_delbydel = delbydel.map(
        lambda d: bydel_by_id(d["bydel_id"]),
        na_action="ignore",
    )
    bydel_via_name = df["Geografi"].map(lambda n: bydel_by_name(str(n)))
    bydel = bydel_via_delbydel.fillna(bydel_via_name)
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    # Pivot Boligtype into columns
    df = df.pivot_table(
        values="Antall boliger",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="Boligtype",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Blokk, leiegård e.l.": "blokk_leiegaard_el",
            ("Forretningsgård, bygg for felleshusholdning e.l."): (
                "forretningsgaard_bygg_for_felleshusholdning_el"
            ),
            "Frittliggende enebolig eller våningshus": (
                "frittliggende_enebolig_eller_vaaningshus"
            ),
            (
                "Horisontaldelt tomannsbolig eller annet"
                " boligbygg med mindre enn 3 etasjer"
            ): (
                "horisontaldelt_tomannsbolig_eller"
                "_annet_boligbygg_med_mindre_enn_3_etasjer"
            ),
            ("Hus i kjede, rekke-/terasse-hus, vertikaldelt tomannsbolig"): (
                "hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"
            ),
        }
    )

    # Sum all housing types into total (matching original Lambda)
    df["total"] = (
        df["blokk_leiegaard_el"]
        + df["forretningsgaard_bygg_for_felleshusholdning_el"]
        + df["frittliggende_enebolig_eller_vaaningshus"]
        + df[
            "horisontaldelt_tomannsbolig_eller"
            "_annet_boligbygg_med_mindre_enn_3_etasjer"
        ]
        + df["hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"]
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "total",
        ]
    ]


def transform_kommunale_boliger(df_municipal, df_housing, type_of_ds):
    """Transformation logic, ported from kommunale_boliger.py."""
    if type_of_ds == "status":
        dfs = transform.status(df_municipal, df_housing)
        template = TemplateA()
    elif type_of_ds == "historisk":
        dfs = transform.historic(df_municipal, df_housing)
        template = TemplateB()
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    df = pd.merge(dfs[0], dfs[1])

    agg = Aggregate({"antall_boliger": "sum", "total": "sum"})
    df = agg.aggregate(df)
    df = agg.add_ratios(df, ["antall_boliger"], ["total"])

    METADATA[type_of_ds].add_scale(
        get_min_max_values_and_ratios(df, "antall_boliger")
    )

    return Output(
        values=["antall_boliger"],
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
    ).generate_output()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--boligmengde-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status", "historisk"],
    )
    args = parser.parse_args()

    df_municipal = read_kommunale_boliger_excel(resolve_input(args.input_dir))
    df_housing = read_boligmengde_excel(resolve_input(args.boligmengde_dir))

    if args.silver_dir:
        write_csv_output(
            df_municipal,
            f"{args.silver_dir}/kommunale-boliger.csv",
        )
        write_csv_output(
            df_housing,
            f"{args.silver_dir}/boligmengde.csv",
        )

    output = transform_kommunale_boliger(
        df_municipal, df_housing, args.type_of_ds
    )
    write_json_output(output, args.output_dir)
