import datetime
from dataclasses import dataclass, field

import pandas

from bydelsfakta.aggregate import ColumnNames
from bydelsfakta.templates import Template

NON_RESIDENTIAL_DISTRICTS = ["16", "17", "99"]


def get_min_max_values_and_ratios(df, column):
    """Return min and max of values and ratios on the latest date in `df`."""
    min_value, min_ratio, max_value, max_ratio = 0, 0, 0, 0

    for district_id in NON_RESIDENTIAL_DISTRICTS:
        df = df.drop(df[df.bydel_id == district_id].index)

    data_on_latest_date = df.groupby([df.date == df.date.max()])

    if f"{column}_ratio" in df:
        min_ratio = data_on_latest_date[f"{column}_ratio"].min()[True]
        max_ratio = data_on_latest_date[f"{column}_ratio"].max()[True]
    if column in df:
        min_value = data_on_latest_date[column].min()[True]
        max_value = data_on_latest_date[column].max()[True]

    return {
        "value": [float(min_value), float(max_value)],
        "ratio": [float(min_ratio), float(max_ratio)],
    }


@dataclass
class Metadata:
    heading: str
    series: list
    published_date: str = str(datetime.date.today())
    help: str = "Dette er en beskrivelse for hvordan dataene leses"
    scope: str = "bydel"
    scale: list = field(default_factory=list)

    def add_scale(self, scale):
        self.scale = scale

    def to_dict(self):
        return {
            "heading": self.heading,
            "series": self.series,
            "publishedDate": self.published_date,
            "help": self.help,
            "scope": self.scope,
            "scale": self.scale,
        }


@dataclass
class Output:
    values: list
    df: pandas.DataFrame
    template: Template
    metadata: Metadata
    column_names: ColumnNames = field(default_factory=ColumnNames)

    def generate_output(self) -> list:
        # Round floats to remove IEEE 754 noise (e.g. 3224999.9999999995
        # instead of 3225000.0) introduced by reading Excel directly
        # instead of going through the old CSV intermediate step.
        self.df = self.df.round(4)

        districts = [
            district_id
            for district_id in self.df[self.column_names.district_id]
            .dropna()
            .unique()
            if district_id not in ["16", "17", "99", "00"]
        ]

        output_list = [
            self._generate_district(district, self.metadata.to_dict())
            for district in districts
        ]
        output_list.append(
            self._generate_oslo_i_alt("00", self.metadata.to_dict())
        )

        return output_list

    def _generate_district(self, district_id, metadata):
        df = self.df
        district = df[df[self.column_names.district_id] == district_id]
        sub_districts = (
            district[self.column_names.sub_district_id].dropna().unique()
        )

        district_name = (
            district[self.column_names.district_name].dropna().unique()[0]
        )

        oslo = df[df[self.column_names.district_id] == "00"]

        data = [
            self._generate_data(
                district[
                    district[self.column_names.sub_district_id] == sub_district
                ],
                district_id=district_id,
                geography_id=sub_district,
                name_column=self.column_names.sub_district_name,
            )
            for sub_district in sub_districts
        ]

        data.append(
            self._generate_data(
                district[district[self.column_names.sub_district_id].isna()],
                district_id=district_id,
                geography_id=district_id,
                geography_name=district_name,
            )
        )

        data.append(
            self._generate_data(
                oslo,
                district_id=district_id,
                geography_id="00",
                geography_name=district_name,
            )
        )

        return {
            "district": district_name,
            "id": district_id,
            "data": data,
            "meta": metadata,
        }

    def _generate_oslo_i_alt(self, district_id, metadata):
        df = self.df
        city_wide = df[df[self.column_names.sub_district_name].isna()]
        districts = city_wide[self.column_names.district_id].dropna().unique()

        data = [
            self._generate_data(
                city_wide[city_wide[self.column_names.district_id] == district],
                district_id=district_id,
                geography_id=district,
                name_column=self.column_names.district_name,
            )
            for district in districts
            if district not in ["16", "17", "99"]
        ]
        metadata = {**metadata, "scope": "oslo i alt"}
        return {
            "district": "Oslo i alt",
            "id": district_id,
            "data": data,
            "meta": metadata,
        }

    def _generate_data(
        self,
        df,
        district_id,
        geography_id,
        name_column=None,
        geography_name=None,
    ):
        if not geography_name:
            if len(df[name_column].unique()) > 1:
                raise Exception("Multiple names for one geography id")
            geography_name = df[name_column].unique()[0]

        avg_row = False
        total_row = False

        if len(geography_id) == 2:
            if geography_id == "00":
                total_row = True
                geography_name = "Oslo i alt"

            elif district_id != "00":
                avg_row = True

        if geography_id == "00":
            avg_row = False
            total_row = True

        values = self.template.values(
            df=df, series=self.values, column_names=self.column_names
        )

        data = {
            "id": geography_id,
            "geography": geography_name,
            "values": values,
            "avgRow": avg_row,
            "totalRow": total_row,
        }
        return data
