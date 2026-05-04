"""Databricks job entry point for levekar_totalt.

Reads 7 Excel files from S3, computes relative ratios for each sub-indicator,
merges them, and writes per-district JSON files back to S3. Status only (no
historisk).

Uses TemplateK which produces {districtRatio, osloRatio} values.
"""

import argparse
import re

import pandas as pd

from bydelsfakta.aggregate import Aggregate, ColumnNames, merge_all
from bydelsfakta.geography import (
    bydel_by_id,
    bydel_by_name,
    delbydel_by_id,
    delbydel_by_name,
)
from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_json_output,
)
from bydelsfakta.output import Metadata, Output
from bydelsfakta.population_utils import generate_population_df
from bydelsfakta.templates import TemplateK

value_columns = [
    "ikke_vestlig_kort",
    "lav_utdanning",
    "antall_fattige_barnehusholdninger",
    "antall_ikke_sysselsatte",
    "neets",
    "antall_trangbodde",
]

graph_metadata = Metadata(
    heading="",
    series=[
        {
            "heading": ("Innvandrere fra Afrika, Asia mv. med kort botid"),
            "subheading": "",
        },
        {"heading": "Lav utdanning", "subheading": ""},
        {
            "heading": "Lavinntektshusholdninger med barn",
            "subheading": "",
        },
        {"heading": "Ikke sysselsatte", "subheading": ""},
        {"heading": "NEETs", "subheading": ""},
        {"heading": "Trangbodde", "subheading": ""},
    ],
)

key_cols = ColumnNames().default_groupby_columns()


# --- Excel reading functions ---


def read_botid_excel(path):
    """Read botid-ikke-vestlige Excel, resolve geography.

    Input columns: Ar, Bydel, Delbydel, Botid, Landbakgrunn, antall
    """
    df = read_excel(path, date_column="År")

    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

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

    df = df.rename(
        columns={
            "Botid": "botid",
            "Landbakgrunn": "landbakgrunn",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "botid",
            "landbakgrunn",
            "antall",
        ]
    ]


def read_lav_utdanning_excel(path):
    """Read lav-utdanning Excel, resolve geography, pivot."""
    df = read_excel(path, date_column="År")

    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

    bydel = df["Bydel"].map(
        lambda n: bydel_by_name(str(n)) or bydel_by_name(f"Bydel {n}")
    )
    df["bydel_id"] = bydel.map(lambda d: d["id"], na_action="ignore")
    df["bydel_navn"] = bydel.map(lambda d: d["name"], na_action="ignore")

    # Pivot education levels into columns
    df = df.pivot_table(
        values="Antall personer",
        index=[
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
        ],
        columns="Høyeste fullførte utdanning",
        aggfunc="sum",
    ).reset_index()

    df.columns.name = None

    col_map = {
        "Ingen utdanning/Uoppgitt utdanning": ("ingen_utdanning_uoppgitt"),
        "Grunnskole": "grunnskole",
        "Videregående skolenivå": "videregaende",
        "Universitets- og høgskolenivå, kort": ("universitet_hogskole_kort"),
        "Universitets- og høgskolenivå, lang": ("universitet_hogskole_lang"),
    }
    df = df.rename(columns=col_map)

    return df


def read_fattige_excel(path):
    """Read fattige-husholdninger Excel, resolve geography."""
    df = read_excel(path, date_column="År")

    geo = df["Geografi"].apply(_resolve_fattige_geo)
    df["delbydel_id"] = geo.map(lambda g: g[0])
    df["delbydel_navn"] = geo.map(lambda g: g[1])
    df["bydel_id"] = geo.map(lambda g: g[2])
    df["bydel_navn"] = geo.map(lambda g: g[3])

    andel_col = "Husholdninger med barn <18 år EU-skala -andel"
    df = df.rename(
        columns={
            andel_col: ("husholdninger_med_barn_under_18_aar_eu_skala_andel"),
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "husholdninger_med_barn_under_18_aar_eu_skala_andel",
        ]
    ]


def _resolve_fattige_geo(geografi):
    """Resolve geography from Geografi string."""
    if geografi == "Oslo i alt":
        return None, None, "00", "Oslo i alt"

    if geografi == "Uoppgitt":
        return (
            None,
            None,
            "99",
            "Uten registrert adresse",
        )

    m = re.match(r"^Delbydel (.+)$", geografi)
    if m:
        name = m.group(1)
        delbydel = delbydel_by_name(name)
        if delbydel:
            bydel = bydel_by_id(delbydel["bydel_id"])
            return (
                delbydel["id"],
                delbydel["name"],
                bydel["id"],
                bydel["name"],
            )

    bydel = bydel_by_name(geografi)
    if bydel:
        return None, None, bydel["id"], bydel["name"]

    return None, None, None, None


def read_sysselsatte_excel(path):
    """Read sysselsatte Excel, resolve geography."""
    df = read_excel(path, date_column="År")

    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

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


def read_befolkning_excel(path):
    """Read befolkning-etter-kjonn-og-alder Excel.

    Pivots age into columns for generate_population_df.
    """
    df = read_excel(path, date_column="År")

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


def _resolve_neets_geo(geo_num):
    """Resolve geography from Geografi_num for neets data."""
    num = int(geo_num)

    if num == 100000:
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": "00",
            "bydel_navn": "Oslo i alt",
        }

    s = str(num)
    if s.startswith("301") and len(s) == 5:
        bydel_id = s[3:].zfill(2)
        bydel = bydel_by_id(bydel_id)
        if bydel_id == "99":
            bydel_navn = "Uten registrert adresse"
        elif bydel:
            bydel_navn = bydel["name"]
        else:
            bydel_navn = None
        return {
            "delbydel_id": None,
            "delbydel_navn": None,
            "bydel_id": bydel_id,
            "bydel_navn": bydel_navn,
        }

    delbydel = delbydel_by_id(num)
    if delbydel:
        bydel = bydel_by_id(delbydel["bydel_id"])
        return {
            "delbydel_id": delbydel["id"],
            "delbydel_navn": delbydel["name"],
            "bydel_id": delbydel["bydel_id"],
            "bydel_navn": bydel["name"] if bydel else None,
        }

    return None


def read_neets_excel(path):
    """Read neets Excel, resolve geography."""
    df = read_excel(path, date_column="aar")

    geo = df["Geografi_num"].map(_resolve_neets_geo)
    df["delbydel_id"] = geo.map(lambda g: g["delbydel_id"], na_action="ignore")
    df["delbydel_navn"] = geo.map(
        lambda g: g["delbydel_navn"], na_action="ignore"
    )
    df["bydel_id"] = geo.map(lambda g: g["bydel_id"], na_action="ignore")
    df["bydel_navn"] = geo.map(lambda g: g["bydel_navn"], na_action="ignore")

    df = df.dropna(subset=["bydel_id"])

    df = df.rename(
        columns={
            "Andel NEETs (15-29 år)": "andel",
            "Antall NEETs": "antall",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "andel",
            "antall",
        ]
    ]


def read_trangbodde_excel(path):
    """Read trangbodde Excel, resolve geography."""
    df = read_excel(path, date_column="År")

    delbydel = df["Delbydel"].map(
        lambda n: delbydel_by_name(str(n)), na_action="ignore"
    )
    df["delbydel_id"] = delbydel.map(lambda d: d["id"], na_action="ignore")
    df["delbydel_navn"] = delbydel.map(lambda d: d["name"], na_action="ignore")

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

    df = df.rename(
        columns={
            "Andel som bor trangt": "andel_som_bor_trangt",
        }
    )

    return df[
        [
            "date",
            "delbydel_id",
            "delbydel_navn",
            "bydel_id",
            "bydel_navn",
            "andel_som_bor_trangt",
        ]
    ]


# --- Core logic functions ---


def keep_newest(df):
    """Return rows where date equals the newest date.

    Also drops the date column.
    """
    return df[df.date == max(df["date"])].drop(columns=["date"])


def add_relative_ratio(df, ratio_col):
    """Compute district and Oslo relative ratios.

    For each (date, bydel_id) group, computes:
    - district_ratio = value / district_mean
    - oslo_ratio = value / oslo_mean

    Excludes districts 16, 17, 99.
    """
    df = df[~df["bydel_id"].isin(["16", "17", "99"])]
    oslo_ratio_col = f"{ratio_col}_oslo"
    district_ratio_col = f"{ratio_col}_district"
    oslo_total_df = df[df["bydel_id"] == "00"]

    result_frames = []
    for (_date, _district_id), group_df in df.groupby(by=["date", "bydel_id"]):
        tmp_df = group_df.copy()
        district_mean = tmp_df[tmp_df["delbydel_id"].isnull()][
            ratio_col
        ].unique()[0]
        oslo_mean = oslo_total_df[oslo_total_df["date"] == _date][
            ratio_col
        ].unique()[0]
        tmp_df[district_ratio_col] = tmp_df[ratio_col] / district_mean
        tmp_df[oslo_ratio_col] = tmp_df[ratio_col] / oslo_mean
        result_frames.append(
            tmp_df[[*key_cols, district_ratio_col, oslo_ratio_col]]
        )

    if result_frames:
        return pd.concat(result_frames, ignore_index=True)
    return pd.DataFrame(columns=[*key_cols, district_ratio_col, oslo_ratio_col])


def pivot_table(df, pivot_column, value_columns):
    """Pivot and re-aggregate, replicating the Lambda pattern."""
    key_columns = list(
        filter(
            lambda x: x not in [pivot_column, *value_columns],
            list(df),
        )
    )
    df_pivot = pd.concat(
        (
            df[key_columns],
            df.pivot(columns=pivot_column, values=value_columns),
        ),
        axis=1,
    )
    return df_pivot.groupby(key_columns).sum().reset_index()


# --- Sub-indicator generators ---


def generate_ikke_vestlig_kort_botid(botid_df, befolkning_df):
    """Generate ikke_vestlig_kort sub-indicator."""
    data_point = "ikke_vestlig_kort"
    kort_botid = "Innvandrer, kort botid (<=5 år)"
    ikke_vestlig = "Asia, Afrika, Latin-Amerika og Øst-Europa utenfor EU"

    # Drop "Norge" landbakgrunn
    df = botid_df[botid_df["landbakgrunn"] != "Norge"].copy()

    # Pivot botid with value = ikke_vestlig landbakgrunn
    # The old code: pivot_table(df, "botid", ikke_vestlig)
    # which pivots botid column using ikke_vestlig as values
    # First, filter to only the ikke_vestlig landbakgrunn
    df_ikke_vestlig = df[df["landbakgrunn"] == ikke_vestlig].copy()
    df_ikke_vestlig = pivot_table(df_ikke_vestlig, "botid", "antall")
    df_ikke_vestlig[data_point] = df_ikke_vestlig.get(kort_botid, 0)

    agg = Aggregate({data_point: "sum"})
    df_agg = agg.aggregate(df_ikke_vestlig)

    population_df = generate_population_df(befolkning_df.copy())
    population_district_df = Aggregate({"population": "sum"}).aggregate(
        df=population_df
    )

    df_agg = pd.merge(
        df_agg,
        population_district_df[
            ["date", "bydel_id", "delbydel_id", "population"]
        ],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df_agg = agg.add_ratios(
        df=df_agg,
        data_points=["ikke_vestlig_kort"],
        ratio_of=["population"],
    )

    return add_relative_ratio(df_agg, "ikke_vestlig_kort_ratio")


def generate_lav_utdanning(lav_utdanning_df):
    """Generate lav_utdanning sub-indicator."""
    data_point = "lav_utdanning"
    education_categories = [
        "ingen_utdanning_uoppgitt",
        "grunnskole",
        "videregaende",
        "universitet_hogskole_kort",
        "universitet_hogskole_lang",
    ]

    df = lav_utdanning_df.copy()
    df["total"] = df[education_categories].sum(axis=1)
    df[data_point] = df[["ingen_utdanning_uoppgitt", "grunnskole"]].sum(axis=1)

    aggregations = {data_point: "sum", "total": "sum"}
    agg = Aggregate(aggregations)
    input_df = agg.aggregate(df)

    input_df = agg.add_ratios(
        input_df,
        data_points=[data_point],
        ratio_of=["total"],
    )
    return add_relative_ratio(input_df, f"{data_point}_ratio")


def generate_fattige_barnehusholdninger(fattige_df):
    """Generate antall_fattige_barnehusholdninger sub-indicator."""
    data_point = "antall_fattige_barnehusholdninger"
    data_point_ratio = f"{data_point}_ratio"

    df = fattige_df.copy()
    df[data_point_ratio] = (
        df["husholdninger_med_barn_under_18_aar_eu_skala_andel"] / 100
    )

    return add_relative_ratio(df, data_point_ratio)


def generate_ikke_sysselsatte(sysselsatte_df, befolkning_df):
    """Generate antall_ikke_sysselsatte sub-indicator."""
    data_point = "antall_ikke_sysselsatte"
    population_col = "population"

    # Population for age 30-59
    pop_df = generate_population_df(
        befolkning_df.copy(), min_age=30, max_age=59
    )

    sub_districts = pop_df["delbydel_id"].unique()

    df = sysselsatte_df.copy()
    # Sysselsatte measured Q4, befolkning measured 1.1 next year
    df["date"] = df["date"] + 1
    df = df[df["delbydel_id"].isin(sub_districts)]

    merged = pd.merge(
        df,
        pop_df[["date", "delbydel_id", "population"]],
        how="inner",
        on=["date", "delbydel_id"],
    )

    # Ignore Sentrum, Marka, Uten registrert adresse
    ignore_districts = ["16", "17", "99"]
    merged = merged[~merged["bydel_id"].isin(ignore_districts)]

    merged[data_point] = merged[population_col] - merged["antall_sysselsatte"]

    agg = Aggregate({population_col: "sum", data_point: "sum"})
    aggregated_df = agg.aggregate(merged)

    input_df = agg.add_ratios(
        aggregated_df,
        data_points=[data_point],
        ratio_of=[population_col],
    )
    return add_relative_ratio(input_df, f"{data_point}_ratio")


def generate_neets(neets_df):
    """Generate neets sub-indicator."""
    df = neets_df.copy()
    df = df.rename(columns={"andel": "neets_ratio"})
    return add_relative_ratio(df, "neets_ratio")


def generate_trangbodde(trangbodde_df, befolkning_df):
    """Generate antall_trangbodde sub-indicator."""
    data_point = "antall_trangbodde"
    population_df = generate_population_df(befolkning_df.copy())

    agg = {"population": "sum"}
    population_district_df = Aggregate(agg).aggregate(df=population_df)

    df = pd.merge(
        trangbodde_df,
        population_district_df[
            ["date", "bydel_id", "delbydel_id", "population"]
        ],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df[f"{data_point}_ratio"] = df["andel_som_bor_trangt"] / 100
    df[data_point] = df["population"] * df[f"{data_point}_ratio"]

    return add_relative_ratio(df, f"{data_point}_ratio")


# --- Output ---


def output_status(input_df, data_points):
    """Generate output using TemplateK."""
    return Output(
        values=data_points,
        df=input_df,
        metadata=graph_metadata,
        template=TemplateK(),
    ).generate_output()


def transform_levekar_totalt(
    botid_df,
    lav_utdanning_df,
    fattige_df,
    sysselsatte_df,
    befolkning_df,
    neets_df,
    trangbodde_df,
):
    """Transform all 7 inputs into levekar_totalt output."""
    ikke_vestlig_kort_botid_input_df = generate_ikke_vestlig_kort_botid(
        botid_df, befolkning_df.copy()
    )
    lav_utdanning_input_df = generate_lav_utdanning(lav_utdanning_df)
    fattige_barnehusholdninger_input_df = generate_fattige_barnehusholdninger(
        fattige_df
    )
    ikke_sysselsatte_input_df = generate_ikke_sysselsatte(
        sysselsatte_df, befolkning_df.copy()
    )
    neets_input_df = generate_neets(neets_df)
    trangbodde_input_df = generate_trangbodde(
        trangbodde_df, befolkning_df.copy()
    )

    dfs = [
        ikke_vestlig_kort_botid_input_df,
        lav_utdanning_input_df,
        fattige_barnehusholdninger_input_df,
        ikke_sysselsatte_input_df,
        neets_input_df,
        trangbodde_input_df,
    ]

    return output_status(merge_all(*map(keep_newest, dfs)), value_columns)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--botid-dir", required=True)
    parser.add_argument("--lav-utdanning-dir", required=True)
    parser.add_argument("--fattige-dir", required=True)
    parser.add_argument("--sysselsatte-dir", required=True)
    parser.add_argument("--befolkning-dir", required=True)
    parser.add_argument("--neets-dir", required=True)
    parser.add_argument("--trangbodde-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--type-of-ds",
        required=True,
        choices=["status"],
    )
    args = parser.parse_args()

    botid_df = read_botid_excel(resolve_input(args.botid_dir))
    lav_utdanning_df = read_lav_utdanning_excel(
        resolve_input(args.lav_utdanning_dir)
    )
    fattige_df = read_fattige_excel(resolve_input(args.fattige_dir))
    sysselsatte_df = read_sysselsatte_excel(resolve_input(args.sysselsatte_dir))
    befolkning_df = read_befolkning_excel(resolve_input(args.befolkning_dir))
    neets_df = read_neets_excel(resolve_input(args.neets_dir))
    trangbodde_df = read_trangbodde_excel(resolve_input(args.trangbodde_dir))

    output = transform_levekar_totalt(
        botid_df,
        lav_utdanning_df,
        fattige_df,
        sysselsatte_df,
        befolkning_df,
        neets_df,
        trangbodde_df,
    )
    write_json_output(output, args.output_dir)
