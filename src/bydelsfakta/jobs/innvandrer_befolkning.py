"""Databricks job entry point for the innvandrer_befolkning dataset.

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
from bydelsfakta.templates import TemplateA, TemplateB, TemplateC

METADATA = {
    "alle_status": Metadata(
        heading="Innvandring befolkning",
        series=[
            {
                "heading": "Innvandrer",
                "subheading": "kort botid (<=5 år)",
            },
            {
                "heading": "Innvandrer",
                "subheading": "lang botid (>5 år)",
            },
            {
                "heading": "Norskfødt",
                "subheading": "med innvandrerforeldre",
            },
        ],
    ),
    "alle_historisk": Metadata(
        heading="Innvandring befolkning",
        series=[
            {
                "heading": "Innvandrer",
                "subheading": "kort botid (<=5 år)",
            },
            {
                "heading": "Innvandrer",
                "subheading": "lang botid (>5 år)",
            },
            {
                "heading": "Norskfødt",
                "subheading": "med innvandrerforeldre",
            },
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "kort_status": Metadata(
        heading="Innvandrer med kort botid (<=5 år)",
        series=[],
    ),
    "kort_historisk": Metadata(
        heading="Innvandrer med kort botid (<=5 år)",
        series=[],
    ),
    "lang_status": Metadata(
        heading="Innvandrer med lang botid (>5 år)",
        series=[],
    ),
    "lang_historisk": Metadata(
        heading="Innvandrer med lang botid (>5 år)",
        series=[],
    ),
    "to_foreldre_status": Metadata(
        heading="Norskfødt med innvandrerforeldre",
        series=[],
    ),
    "to_foreldre_historisk": Metadata(
        heading="Norskfødt med innvandrerforeldre",
        series=[],
    ),
}

DATA_POINTS = {
    "alle_status": ["short", "long", "two_parents"],
    "alle_historisk": ["short", "long", "two_parents", "total_cat"],
    "kort_status": ["short"],
    "kort_historisk": ["short"],
    "lang_status": ["long"],
    "lang_historisk": ["long"],
    "to_foreldre_status": ["two_parents"],
    "to_foreldre_historisk": ["two_parents"],
}

TYPE_OF_DS_CHOICES = [
    "alle_status",
    "alle_historisk",
    "kort_status",
    "kort_historisk",
    "lang_status",
    "lang_historisk",
    "to_foreldre_status",
    "to_foreldre_historisk",
]


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


def read_botid_excel(path):
    """Read botid Excel from S3, resolve geography, pivot.

    Input columns (Excel):
        År, Bydel, Delbydel, Botid, Innvandringskategori, antall

    After pivot on Innvandringskategori, columns include
    the immigration categories as separate value columns.
    """
    df = read_excel(path, date_column="År")
    df = _resolve_geo_from_name(df, "Delbydel", "Bydel")

    # Pivot Innvandringskategori into columns
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
        columns="Innvandringskategori",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Botid": "botid",
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

    # Filter out rows without valid geography
    df = df[df["delbydel_id"].notnull()]

    return df


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel, resolve geo.

    Pivots Alder into columns, sums across gender to get total
    population per geographic unit.
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

    # Filter out rows without valid geography
    df = df[df["delbydel_id"].notnull()]

    # Sum all age columns into total
    age_cols = [c for c in df.columns if c.isdigit()]
    df["total"] = df[age_cols].sum(axis=1)

    # Group by geo (sum across gender)
    population = (
        df.groupby(["date", "bydel_id", "delbydel_id"])["total"]
        .sum()
        .reset_index()
    )

    return population


def _by_liveage(livage):
    """Replicate the by_liveage function from the original Lambda.

    Drops unnecessary immigration categories, groups by botid,
    sums innvandrer + norskfodt_med_innvandrerforeldre, then
    pivots on botid to get short/long/two_parents columns.
    """
    livage = livage.drop(
        columns=[
            "fodt_i_utlandet_av_norskfodte_foreldre",
            "norskfodt_med_en_utenlandskfodt_forelder",
            "uten_innvandringsbakgrunn",
            "utenlandsfodt_med_en_norsk_forelder",
        ]
    )

    meta_columns = [
        "date",
        "delbydel_id",
        "delbydel_navn",
        "bydel_id",
        "bydel_navn",
    ]

    livage = livage.groupby([*meta_columns, "botid"]).sum(numeric_only=True)

    total = (
        livage[["innvandrer", "norskfodt_med_innvandrerforeldre"]]
        .sum(axis=1)
        .reset_index()
    )

    pivot = total.pivot_table(index=meta_columns, columns="botid", values=0)
    pivot = pivot.rename(
        columns={
            "Innvandrer, kort botid (<=5 år)": "short",
            "Innvandrer, lang botid (>5 år)": "long",
            "Øvrige befolkning": "two_parents",
        }
    )

    return pivot.reset_index()


def _prepare(livage, population_df):
    """Merge liveage data with population totals."""
    livage = _by_liveage(livage)
    return pd.merge(
        livage,
        population_df,
        on=["date", "delbydel_id", "bydel_id"],
    )


def _generate(livage_df, population_df):
    """Aggregate and compute ratios."""
    sub_districts = _prepare(livage_df, population_df)

    aggregate_config = {
        "two_parents": "sum",
        "short": "sum",
        "long": "sum",
        "total": "sum",
    }
    agg = Aggregate(aggregate_config=aggregate_config)
    aggregated = agg.aggregate(sub_districts)

    aggregated["total_cat"] = (
        aggregated["two_parents"] + aggregated["short"] + aggregated["long"]
    )

    with_ratios = agg.add_ratios(
        aggregated,
        ["two_parents", "short", "long", "total_cat"],
        ["total"],
    )
    return with_ratios


def transform_innvandrer_befolkning(livage_df, population_df, type_of_ds):
    """Transformation logic, ported from innvandrer_befolkning.py."""
    df_status_sources = transform.status(livage_df, population_df)
    df_historic_sources = transform.historic(livage_df, population_df)

    generated_status = _generate(*df_status_sources)
    generated_historic = _generate(*df_historic_sources)

    if type_of_ds == "alle_status":
        out_df = generated_status
        template = TemplateA()
    elif type_of_ds == "alle_historisk":
        out_df = generated_historic
        template = TemplateC()
    elif type_of_ds == "kort_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(generated_status, "short")
        )
        out_df = generated_status
        template = TemplateA()
    elif type_of_ds == "kort_historisk":
        out_df = generated_historic
        template = TemplateB()
    elif type_of_ds == "lang_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(generated_status, "long")
        )
        out_df = generated_status
        template = TemplateA()
    elif type_of_ds == "lang_historisk":
        out_df = generated_historic
        template = TemplateB()
    elif type_of_ds == "to_foreldre_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(generated_status, "two_parents")
        )
        out_df = generated_status
        template = TemplateA()
    elif type_of_ds == "to_foreldre_historisk":
        out_df = generated_historic
        template = TemplateB()
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
    parser.add_argument("--befolkning-dir", required=True)
    parser.add_argument("--silver-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=TYPE_OF_DS_CHOICES,
    )
    args = parser.parse_args()

    botid_path = resolve_input(args.input_dir)
    befolkning_path = resolve_input(args.befolkning_dir)

    livage_df = read_botid_excel(botid_path)
    population_df = read_befolkning_excel(befolkning_path)

    if args.silver_dir:
        write_csv_output(livage_df, f"{args.silver_dir}/botid.csv")
        write_csv_output(
            population_df,
            f"{args.silver_dir}/befolkning.csv",
        )

    output = transform_innvandrer_befolkning(
        livage_df, population_df, args.type_of_ds
    )
    write_json_output(output, args.output_dir)
