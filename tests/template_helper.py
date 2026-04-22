def without_ratios(template, df, flatten=False):
    output_values = template.values(df, series=["d2"])
    if flatten:
        output_values = [obj for values_list in output_values for obj in values_list]
    assert all("ratio" not in obj for obj in output_values)
    assert all("date" in obj for obj in output_values)
    assert all("value" in obj for obj in output_values)


def with_ratios(template, df, flatten=False):
    output_values = template.values(df, series=["d1"])
    if flatten:
        output_values = [obj for values_list in output_values for obj in values_list]
    assert all("ratio" in obj for obj in output_values)
    assert all("date" in obj for obj in output_values)
    assert all("value" in obj for obj in output_values)


def values_structure(template, df, value_type):
    output_values = template.values(df, series=["d1"])
    assert isinstance(output_values, list)
    assert all(isinstance(obj, value_type) for obj in output_values)


def count_values(template, df, expected):
    output_values = template.values(df, series=["d1"])
    assert len(output_values) == expected
