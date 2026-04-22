from bydelsfakta.templates import TemplateG

template = TemplateG(history_columns=["d2"])


def test_df_to_template_g(template_df):
    input_df = template_df[
        (template_df.delbydel_id == "0202")
        & (template_df.date == template_df.date.max())
    ]
    value = template.values(input_df, ["d1"])
    assert value == ["d1_0202_2018", ["d2_0202_2018"], 2018]
