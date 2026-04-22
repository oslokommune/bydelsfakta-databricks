"""Databricks job entry point for the bygningstyper dataset.

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
    "blokk_status": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_status": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_status": Metadata(heading="Rekkehus/tomannsboliger", series=[]),
    "blokk_historisk": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_historisk": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_historisk": Metadata(
        heading="Rekkehus/tomannsboliger", series=[]
    ),
    "alle_status": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {
                "heading": "Rekkehus, tomannsboliger o.l",
                "subheading": "",
            },
            {"heading": "Enebolig", "subheading": ""},
        ],
    ),
    "alle_historisk": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {
                "heading": "Rekkehus, tomannsboliger o.l",
                "subheading": "",
            },
            {"heading": "Enebolig", "subheading": ""},
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "totalt_status": Metadata(heading="Antall boliger", series=[]),
    "totalt_historisk": Metadata(heading="Antall boliger", series=[]),
}

TYPE_OF_DS_CHOICES = [
    "alle_status",
    "alle_historisk",
    "blokk_status",
    "blokk_historisk",
    "enebolig_status",
    "enebolig_historisk",
    "rekkehus_status",
    "rekkehus_historisk",
    "totalt_status",
    "totalt_historisk",
]


def read_bygningstyper_excel(path):
    """Read bygningstyper Excel from S3, resolve geography.

    Input columns (Excel):
        År, Geografi, Boligtype, Antall boliger

    Output columns (normalized):
        date, delbydel_id, delbydel_navn, bydel_id, bydel_navn,
        blokk_leiegaard_el,
        forretningsgaard_bygg_for_felleshusholdning_el,
        frittliggende_enebolig_eller_vaaningshus,
        horisontaldelt_tomannsbolig_eller_annet_boligbygg_med_mindre_enn_3_etasjer,
        hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig
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

    # Flatten MultiIndex columns from pivot
    df.columns.name = None

    df = df.rename(
        columns={
            "Blokk, leiegård e.l.": "blokk_leiegaard_el",
            (
                "Forretningsgård, bygg for felleshusholdning e.l."
            ): "forretningsgaard_bygg_for_felleshusholdning_el",
            "Frittliggende enebolig eller våningshus": (
                "frittliggende_enebolig_eller_vaaningshus"
            ),
            (
                "Horisontaldelt tomannsbolig eller annet"
                " boligbygg med mindre enn 3 etasjer"
            ): (
                "horisontaldelt_tomannsbolig_eller_annet"
                "_boligbygg_med_mindre_enn_3_etasjer"
            ),
            ("Hus i kjede, rekke-/terasse-hus, vertikaldelt tomannsbolig"): (
                "hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"
            ),
        }
    )

    return df


def transform_bygningstyper(df, type_of_ds):
    """Pure transformation logic, ported from functions/bygningstyper.py."""
    df = df.copy()

    df["enebolig"] = df["frittliggende_enebolig_eller_vaaningshus"]
    df["rekkehus"] = (
        df[
            "horisontaldelt_tomannsbolig_eller_annet"
            "_boligbygg_med_mindre_enn_3_etasjer"
        ]
        + df["hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"]
    )
    df["blokk"] = (
        df["blokk_leiegaard_el"]
        + df["forretningsgaard_bygg_for_felleshusholdning_el"]
    )
    df = df.drop(
        columns=[
            "frittliggende_enebolig_eller_vaaningshus",
            "horisontaldelt_tomannsbolig_eller_annet"
            "_boligbygg_med_mindre_enn_3_etasjer",
            "hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig",
            "blokk_leiegaard_el",
            "forretningsgaard_bygg_for_felleshusholdning_el",
        ]
    )

    df["total"] = df["rekkehus"] + df["blokk"] + df["enebolig"]

    agg = Aggregate(
        {
            "total": "sum",
            "rekkehus": "sum",
            "enebolig": "sum",
            "blokk": "sum",
        }
    )

    df = agg.aggregate(df)
    df = agg.add_ratios(
        df,
        ["rekkehus", "blokk", "enebolig", "total"],
        ["total"],
    )

    df_status = transform.status(df)
    df_historic = transform.historic(df)

    if type_of_ds == "alle_status":
        [out_df] = df_status
        template = TemplateA()
        values = ["blokk", "rekkehus", "enebolig"]
    elif type_of_ds == "alle_historisk":
        [out_df] = df_historic
        template = TemplateC()
        values = ["blokk", "rekkehus", "enebolig", "total"]
    elif type_of_ds == "blokk_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "blokk")
        )
        [out_df] = df_status
        template = TemplateA()
        values = ["blokk"]
    elif type_of_ds == "blokk_historisk":
        [out_df] = df_historic
        template = TemplateB()
        values = ["blokk"]
    elif type_of_ds == "enebolig_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "enebolig")
        )
        [out_df] = df_status
        template = TemplateA()
        values = ["enebolig"]
    elif type_of_ds == "enebolig_historisk":
        [out_df] = df_historic
        template = TemplateB()
        values = ["enebolig"]
    elif type_of_ds == "rekkehus_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "rekkehus")
        )
        [out_df] = df_status
        template = TemplateA()
        values = ["rekkehus"]
    elif type_of_ds == "rekkehus_historisk":
        [out_df] = df_historic
        template = TemplateB()
        values = ["rekkehus"]
    elif type_of_ds == "totalt_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "totalt")
        )
        [out_df] = df_status
        template = TemplateA()
        values = ["total"]
    elif type_of_ds == "totalt_historisk":
        [out_df] = df_historic
        template = TemplateB()
        values = ["total"]
    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")

    return Output(
        df=out_df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=values,
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
    df = read_bygningstyper_excel(input_path)

    if args.silver_dir:
        write_csv_output(
            df,
            f"{args.silver_dir}/boligmengde-etter-boligtype.csv",
        )

    output = transform_bygningstyper(df, args.type_of_ds)
    write_json_output(output, args.output_dir)
