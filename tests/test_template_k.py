import operator

from bydelsfakta.output import Metadata, Output
from bydelsfakta.templates import TemplateK
from tests.transform_output_test_data import output_list_k

template = TemplateK()


def test_df_to_template_k(template_k_df):
    data_points = ["d1", "d2"]
    output = Output(
        values=data_points,
        df=template_k_df,
        metadata=Metadata("", []),
        template=template,
    ).generate_output()

    expected_0101 = {
        "id": "0101",
        "geography": "Lodalen",
        "values": [
            {
                "districtRatio": "d1_0101_2018_ratio_district",
                "osloRatio": "d1_0101_2018_ratio_oslo",
            },
            {
                "districtRatio": "d2_0101_2018_ratio_district",
                "osloRatio": "d2_0101_2018_ratio_oslo",
            },
        ],
        "avgRow": False,
        "totalRow": False,
    }

    expected_01 = {
        "id": "01",
        "geography": "Bydel Gamle Oslo",
        "values": [
            {
                "districtRatio": "d1_01_2018_ratio_district",
                "osloRatio": "d1_01_2018_ratio_oslo",
            },
            {
                "districtRatio": "d2_01_2018_ratio_district",
                "osloRatio": "d2_01_2018_ratio_oslo",
            },
        ],
        "avgRow": True,
        "totalRow": False,
    }

    expected_00 = {
        "id": "00",
        "geography": "Oslo i alt",
        "values": [
            {
                "districtRatio": "d1_00_2018_ratio_district",
                "osloRatio": "d1_00_2018_ratio_oslo",
            },
            {
                "districtRatio": "d2_00_2018_ratio_district",
                "osloRatio": "d2_00_2018_ratio_oslo",
            },
        ],
        "avgRow": False,
        "totalRow": True,
    }

    file_01_data = next(obj for obj in output if obj["id"] == "01")["data"]

    sub_district_0101 = next(data for data in file_01_data if data["id"] == "0101")
    district_01 = next(data for data in file_01_data if data["id"] == "01")
    oslo = next(data for data in file_01_data if data["id"] == "00")

    assert sub_district_0101 == expected_0101
    assert district_01 == expected_01
    assert oslo == expected_00


def test_output_list(template_k_df):
    data_points = ["d1", "d2"]
    output = Output(
        values=data_points,
        df=template_k_df,
        metadata=Metadata("", []),
        template=template,
    ).generate_output()

    output = sorted(output, key=operator.itemgetter("id"))
    output = list(
        map(lambda x: sorted(x["data"], key=operator.itemgetter("id")), output)
    )

    expected = sorted(output_list_k, key=operator.itemgetter("id"))
    expected = list(
        map(lambda x: sorted(x["data"], key=operator.itemgetter("id")), expected)
    )

    assert output == expected
