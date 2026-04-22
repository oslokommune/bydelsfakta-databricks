"""Utility for computing total population from age-distributed data."""


def generate_population_df(population_raw, min_age=0, max_age=200):
    """Sum age columns into a single 'population' column.

    Takes a DataFrame with age columns ("0" through "120") and geography
    columns, sums the specified age range, and groups by geography (summing
    across genders).

    Returns a DataFrame with: date, delbydel_id, delbydel_navn, bydel_id,
    bydel_navn, population.
    """
    max_col = min(max_age, 120)
    age_cols = [str(i) for i in range(min_age, max_col + 1)]
    existing_cols = [c for c in age_cols if c in population_raw.columns]

    groupby_cols = [
        "date",
        "delbydel_id",
        "delbydel_navn",
        "bydel_id",
        "bydel_navn",
    ]

    df = population_raw[groupby_cols].copy()
    df["population"] = population_raw[existing_cols].sum(axis=1).values
    return df.groupby(groupby_cols, as_index=False)["population"].sum()
