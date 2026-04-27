"""Databricks job entry point for landbakgrunn_innvandringsgrupper.

Reads two Excel files (landbakgrunn-storste-innvandringsgrupper + befolkning-
etter-kjonn-og-alder) from S3, resolves geography, computes top-N immigration
countries per district, and writes per-district JSON files back to S3.

This dataset uses a CUSTOM output format (not Output.generate_output) with
top-N countries per district.
"""

import argparse

import pandas as pd

from bydelsfakta.aggregate import Aggregate
from bydelsfakta.geography import (
    bydel_by_id,
    bydel_by_name,
    delbydel_by_id,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)
from bydelsfakta.output import Metadata
from bydelsfakta.population_utils import generate_population_df


def read_landbakgrunn_excel(path):
    """Read landbakgrunn-storste-innvandringsgrupper Excel.

    Input columns (Excel):
        Ar, Bydel, Innvandringskategori, Landbakgrunn,
        Antall personer

    Pivots Innvandringskategori into columns:
        innvandrer, norskfodt_med_innvandrerforeldre

    Output columns (normalized):
        date, bydel_id, bydel_navn, landbakgrunn,
        innvandrer, norskfodt_med_innvandrerforeldre
    """
    df = read_excel(path, date_column="År")

    # Resolve bydel from name (Excel has bare names like
    # "Gamle Oslo", not "Bydel Gamle Oslo")
    bydel = df["Bydel"].map(
        lambda n: bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}")
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    df = df.rename(columns={"Landbakgrunn": "landbakgrunn"})

    # Pivot Innvandringskategori to get innvandrer and
    # norskfodt_med_innvandrerforeldre as separate columns
    df = df.pivot_table(
        values="Antall personer",
        index=["date", "bydel_id", "bydel_navn", "landbakgrunn"],
        columns="Innvandringskategori",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    df.columns.name = None

    df = df.rename(
        columns={
            "Innvandrer": "innvandrer",
            "Norskfødt med innvandrerforeldre": (
                "norskfodt_med_innvandrerforeldre"
            ),
        }
    )

    return df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            "landbakgrunn",
            "innvandrer",
            "norskfodt_med_innvandrerforeldre",
        ]
    ]


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel.

    Pivots age into columns ("0" through "120") for use with
    generate_population_df.
    """
    df = read_excel(path, date_column="År")

    # Resolve geography from numeric IDs
    geo = df.apply(
        lambda row: _resolve_befolkning_geo(row["Bydel"], row["Delbydel"]),
        axis=1,
    )
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"])
    df["delbydel_navn"] = geo.map(lambda g: g["delbydel_navn"])
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"])
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"])

    df = df.rename(
        columns={
            "Kjønn": "kjonn",
            "Alder": "alder",
            "Antall personer": "antall_personer",
        }
    )

    df["alder"] = df["alder"].astype(str)

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

    for age in [str(i) for i in range(0, 121)]:
        if age not in df.columns:
            df[age] = 0

    df = df.dropna(subset=["bydel_id"])
    return df


def _resolve_befolkning_geo(bydel_val, delbydel_val):
    """Resolve geography from Bydel (301XX) and Delbydel IDs."""
    bydel_str = str(int(bydel_val))

    if bydel_str.startswith("301"):
        local = bydel_str[3:]
        bydel_id = local.zfill(2)
    else:
        bydel_id = bydel_str.zfill(2)

    bydel = bydel_by_id(bydel_id)
    bydel_navn = bydel["name"] if bydel else None

    delbydel = delbydel_by_id(int(delbydel_val))
    if delbydel:
        return {
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": bydel_navn,
        }

    return {
        "delbydel_id": None,
        "delbydel_navn": None,
        "bydel_id": bydel_id,
        "bydel_navn": bydel_navn,
    }


def process_country_df(df):
    """Add total column and create Oslo total via groupby."""
    data_points = ["innvandrer", "norskfodt_med_innvandrerforeldre"]
    df["total"] = df[data_points].sum(axis=1)

    oslo_total_df = (
        df.groupby(["date", "landbakgrunn"]).sum(numeric_only=True)
    ).reset_index()
    oslo_total_df["bydel_id"] = "00"
    oslo_total_df["bydel_navn"] = "Oslo i alt"

    country_df = pd.concat((df, oslo_total_df), sort=False, ignore_index=True)
    return country_df[
        [
            "bydel_id",
            "bydel_navn",
            "date",
            "landbakgrunn",
            "innvandrer",
            "norskfodt_med_innvandrerforeldre",
            "total",
        ]
    ]


def population_district_only(population_df, ignore_districts=None):
    """Aggregate population, filter to district level only."""
    if ignore_districts is None:
        ignore_districts = []
    population_df = population_df[
        ~population_df["bydel_id"].isin(ignore_districts)
    ]
    agg = {"population": "sum"}
    population_aggregated_df = Aggregate(agg).aggregate(df=population_df)
    return population_aggregated_df[
        population_aggregated_df["delbydel_id"].isnull()
    ]


def add_ratios(df, data_points, ratio_of):
    """Add ratio columns for data_points relative to ratio_of."""
    sums = df[ratio_of].sum(axis=1)
    for dp in data_points:
        col_name = f"{dp}_ratio"
        df[col_name] = df[dp] / sums
    return df


def get_top_n_countries(df, n):
    """Get top N countries by total for each district."""
    top_n = {}
    for district in df["bydel_id"].unique():
        district_df = df[df["bydel_id"] == district]
        district_df = district_df[
            district_df["date"] == district_df["date"].max()
        ]
        district_df = district_df.nlargest(n, "total")
        top_n[district] = district_df["landbakgrunn"].tolist()
    return top_n


def generate_geo_obj_status(df, geography, data_points):
    """Generate geography object for status (latest date only)."""
    series = {}
    for value in df.to_dict("records"):
        for data_point in data_points:
            series[data_point] = {
                "date": value["date"],
                "value": value[data_point],
                "ratio": value[f"{data_point}_ratio"],
            }
    values = [series[data_point] for data_point in data_points if series]
    return {"geography": geography, "values": values}


def generate_geo_obj_historic(df, geography, data_points):
    """Generate geography object for historisk (all dates)."""
    series = {data_point: [] for data_point in data_points}
    for value in df.to_dict("records"):
        for data_point in data_points:
            series[data_point].append(
                {
                    "date": value["date"],
                    "value": value[data_point],
                    "ratio": value[f"{data_point}_ratio"],
                }
            )
    values = [series[data_point] for data_point in data_points if series]
    return {"geography": geography, "values": values}


def generate_output_list(
    input_df, data_points, top_n, template_fun, graph_metadata
):
    """Generate custom output list with top-N countries per district."""
    graph_metadata_as_dict = {
        "series": graph_metadata.series,
        "heading": graph_metadata.heading,
    }

    top_n_countries = get_top_n_countries(input_df, top_n)

    district_list = [
        (x.bydel_id, x.bydel_navn)
        for x in set(
            input_df.loc[:, ["bydel_id", "bydel_navn"]].itertuples(index=False)
        )
    ]
    output_list = []
    for district_id, district_name in district_list:
        district_obj = {
            "district": district_name,
            "id": district_id,
            "data": [],
            "meta": graph_metadata_as_dict,
        }
        district_df = input_df[input_df["bydel_id"] == district_id]
        for geography in top_n_countries[district_id]:
            geo_df = district_df[district_df["landbakgrunn"] == geography]
            geo_obj = template_fun(geo_df, geography, data_points)
            district_obj["data"].append(geo_obj)
        output_list.append(district_obj)

    return output_list


def transform_landbakgrunn(landbakgrunn_df, befolkning_df, type_of_ds):
    """Transform landbakgrunn data with population ratios."""
    data_points_status = [
        "innvandrer",
        "norskfodt_med_innvandrerforeldre",
    ]
    data_points = [
        "total",
        "innvandrer",
        "norskfodt_med_innvandrerforeldre",
    ]

    # Process country data: add total, create Oslo total
    country_df = process_country_df(landbakgrunn_df)

    # Process population: aggregate, keep district-level only
    population_df = generate_population_df(befolkning_df)
    ignore_districts = ["16", "17"]
    befolkning_district_df = population_district_only(
        population_df, ignore_districts=ignore_districts
    )

    # Merge landbakgrunn with population
    input_df = pd.merge(
        country_df,
        befolkning_district_df,
        how="inner",
        on=["bydel_id", "date", "bydel_navn"],
    )
    input_df = add_ratios(input_df, data_points, ratio_of=["population"])

    # Round to remove IEEE 754 noise from reading Excel directly
    input_df = input_df.round(4)

    if type_of_ds == "status":
        graph_metadata = Metadata(
            series=[
                {"heading": "Innvandrer", "subheading": ""},
                {
                    "heading": "Norskfødt med innvandrerforeldre",
                    "subheading": "",
                },
            ],
            heading="10 største innvandringsgrupper",
        )

        input_df_status = input_df[input_df["date"] == input_df["date"].max()]
        return generate_output_list(
            input_df_status,
            data_points_status,
            top_n=10,
            template_fun=generate_geo_obj_status,
            graph_metadata=graph_metadata,
        )

    elif type_of_ds == "historisk":
        graph_metadata = Metadata(
            series=[
                {"heading": "Totalt", "subheading": ""},
                {"heading": "Innvandrer", "subheading": ""},
                {
                    "heading": "Norskfødt med innvandrerforeldre",
                    "subheading": "",
                },
            ],
            heading="10 største innvandringsgrupper",
        )

        return generate_output_list(
            input_df,
            data_points,
            top_n=10,
            template_fun=generate_geo_obj_historic,
            graph_metadata=graph_metadata,
        )

    else:
        raise ValueError(f"Unknown dataset type: {type_of_ds!r}")


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

    landbakgrunn_path = resolve_input(args.input_dir)
    befolkning_path = resolve_input(args.befolkning_dir)

    landbakgrunn_df = read_landbakgrunn_excel(landbakgrunn_path)
    befolkning_df = read_befolkning_excel(befolkning_path)

    if args.silver_dir:
        write_csv_output(
            landbakgrunn_df,
            f"{args.silver_dir}/landbakgrunn-storste-innvandringsgrupper.csv",
        )
        write_csv_output(
            befolkning_df,
            f"{args.silver_dir}/befolkning.csv",
        )

    output = transform_landbakgrunn(
        landbakgrunn_df, befolkning_df, args.type_of_ds
    )
    write_json_output(output, args.output_dir)
