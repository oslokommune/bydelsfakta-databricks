import json
import os
from pathlib import Path

import pandas as pd

_DEFAULT_DTYPE = {
    "delbydel_id": object,
    "delbydel_navn": object,
    "bydel_id": object,
    "bydel_navn": object,
}


def resolve_input(input_dir):
    """Find the latest Excel file in a directory.

    Works with both local paths and Databricks Volume paths
    (`/Volumes/catalog/schema/volume/...`).
    """
    p = Path(input_dir)
    xlsx_files = list(p.glob("*.xlsx"))

    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files in {input_dir}")

    return str(max(xlsx_files, key=lambda f: f.stat().st_mtime))


def read_excel(path, date_column="aar"):
    """Read an Excel file and return it as a dataframe."""
    df = pd.read_excel(path, engine="openpyxl", dtype=_DEFAULT_DTYPE)

    if date_column in df.columns:
        return df.rename(columns={date_column: "date"})

    return df


def _ensure_dir(path):
    """Create directory if it doesn't exist.

    Handles UC Volumes where `os.makedirs` on the volume root raises `OSError`
    (operation not supported).
    """
    try:
        os.makedirs(path, exist_ok=True)
    except OSError:
        if not os.path.isdir(path):
            raise


def write_csv_output(df, path, sep=";"):
    """Write a DataFrame as CSV.

    This writes the intermediate (silver) representation of the data after
    geography resolution and column renaming, but before transform.
    """
    _ensure_dir(os.path.dirname(path))
    df.to_csv(path, sep=sep, index=False)


def write_json_output(output_list, output_dir):
    """Write JSON files (one per district) to a directory.

    Replicates the exact output format of aws.write_to_intermediate: one JSON
    file per entry in `output_list`, named `{id}.json` or `{district}.json`.
    """
    _ensure_dir(output_dir)

    for output in output_list:
        filename = "{}.json".format(output.get("id") or output["district"])
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w") as f:
            json.dump(output, f, ensure_ascii=False, allow_nan=False)
