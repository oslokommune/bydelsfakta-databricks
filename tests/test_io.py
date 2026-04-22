import json
import os
import time

import pandas as pd
import pytest

from bydelsfakta.io import (
    read_excel,
    resolve_input,
    write_csv_output,
    write_json_output,
)


@pytest.fixture
def xlsx_dir(tmp_path):
    df = pd.DataFrame({"aar": [2020, 2021], "value": [10, 20]})
    path = tmp_path / "data.xlsx"
    df.to_excel(path, index=False)
    return tmp_path


@pytest.fixture
def xlsx_path(xlsx_dir):
    return str(xlsx_dir / "data.xlsx")


class TestResolveInput:
    def test_finds_xlsx(self, xlsx_dir):
        result = resolve_input(str(xlsx_dir))
        assert result.endswith("data.xlsx")

    def test_finds_latest(self, xlsx_dir):
        older = xlsx_dir / "old.xlsx"
        pd.DataFrame({"x": [1]}).to_excel(older, index=False)

        time.sleep(0.05)

        newer = xlsx_dir / "new.xlsx"
        pd.DataFrame({"x": [2]}).to_excel(newer, index=False)

        result = resolve_input(str(xlsx_dir))
        assert result.endswith("new.xlsx")

    def test_no_files(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No .xlsx files"):
            resolve_input(str(tmp_path))


class TestReadExcel:
    def test_renames_date_column(self, xlsx_path):
        df = read_excel(xlsx_path)
        assert "date" in df.columns
        assert "aar" not in df.columns

    def test_custom_date_column(self, tmp_path):
        df = pd.DataFrame({"År": [2020], "v": [1]})
        path = str(tmp_path / "custom.xlsx")
        df.to_excel(path, index=False)
        result = read_excel(path, date_column="År")
        assert "date" in result.columns

    def test_default_dtype(self, tmp_path):
        df = pd.DataFrame(
            {"aar": [2020], "delbydel_id": [101], "bydel_id": [1]}
        )
        path = str(tmp_path / "geo.xlsx")
        df.to_excel(path, index=False)
        result = read_excel(path)
        assert result["delbydel_id"].dtype == object
        assert result["bydel_id"].dtype == object


class TestWriteJsonOutput:
    def test_writes_per_district(self, tmp_path):
        output_list = [
            {"id": "01", "data": [1, 2]},
            {"id": "02", "data": [3, 4]},
        ]
        output_dir = str(tmp_path / "output")
        write_json_output(output_list, output_dir)

        with open(os.path.join(output_dir, "01.json")) as f:
            assert json.load(f) == {"id": "01", "data": [1, 2]}
        with open(os.path.join(output_dir, "02.json")) as f:
            assert json.load(f) == {"id": "02", "data": [3, 4]}

    def test_uses_district_key_fallback(self, tmp_path):
        output_list = [{"district": "test_district", "data": []}]
        output_dir = str(tmp_path / "output")
        write_json_output(output_list, output_dir)
        assert os.path.exists(
            os.path.join(output_dir, "test_district.json")
        )

    def test_ensure_ascii_false(self, tmp_path):
        output_list = [{"id": "01", "name": "Østensjø"}]
        output_dir = str(tmp_path / "output")
        write_json_output(output_list, output_dir)
        with open(os.path.join(output_dir, "01.json")) as f:
            content = f.read()
        assert "Østensjø" in content
        assert "\\u" not in content


class TestWriteCsvOutput:
    def test_semicolon_separator(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        path = str(tmp_path / "out.csv")
        write_csv_output(df, path)
        with open(path) as f:
            lines = f.readlines()
        assert lines[0].strip() == "a;b"

    def test_creates_parent_dir(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        path = str(tmp_path / "nested" / "dir" / "out.csv")
        write_csv_output(df, path)
        assert os.path.exists(path)

    def test_no_index(self, tmp_path):
        df = pd.DataFrame({"col": [10, 20]})
        path = str(tmp_path / "out.csv")
        write_csv_output(df, path)
        result = pd.read_csv(path, sep=";")
        assert list(result.columns) == ["col"]
