import altair as alt
import pandas as pd


def average_lice(
    df, year_range, week_range, lice_type, location_level, location, y_scale=None
):
    if location_level == "Nasjonal":
        location = "Norge"
    if location_level == "Nasjonal":
        df_plot = df[(df["year"].between(year_range[0], year_range[1]))]
    else:
        # df_plot = df[(df[location_level.lower()] == location) & (df["year"].between(year_range[0], year_range[1]))]
        if location_level == "Produksjonsområde":
            df_plot = df.loc[
                (df["produksjonsomrade"] == location)
                & (df["year"].between(year_range[0], year_range[1]))
            ]
        elif location_level == "Område":
            df_plot = df.loc[
                (df["omrade"] == location)
                & (df["year"].between(year_range[0], year_range[1]))
            ]
        else:
            df_plot = df[
                (df[location_level.lower()] == location)
                & (df["year"].between(year_range[0], year_range[1]))
            ]

    df_plot_agg = (
        df_plot.groupby(["year", "uke", location_level])
        .agg({"total_{}".format(lice_type): "sum", "beholdning": "sum"})
        .reset_index()
    )
    df_plot_agg = df_plot_agg.astype({"uke": int, "year": int})
    df_plot_agg.sort_values(["year", "uke"])
    df_plot_agg["Mean {} {}".format(lice_type, "per fish")] = (
        df_plot_agg["total_{}".format(lice_type)] / df_plot_agg["beholdning"]
    )
    df_plot_agg["y"] = df_plot_agg["year"]

    selection = alt.selection_multi(fields=["year"], bind="legend")
    y_value = "Mean {} {}".format(lice_type, "per fish")
    chart = (
        alt.Chart(df_plot_agg)
        .mark_line(point={"filled": False, "fill": "white", "strokeWidth": 5})
        .encode(
            x=alt.X(
                "uke",
                title="Uke",
                sort="ascending",
                scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
            ),
            y=alt.Y("Mean {} {}".format(lice_type, "per fish")),
            size=alt.Size("y:N", legend=None),
            color=alt.Color("year:N", title="År", scale=alt.Scale(scheme="paired")),
            opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            tooltip=["year"],
        )
        .properties(width=500, height=350)
        .configure_axis(labelFontSize=15, titleFontSize=15)
        .configure_legend(labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100)
        .add_selection(selection)
        .interactive()
    )

    df_plot_agg.drop("y", axis=1)
    return df_plot_agg, chart


def average_lice_compare(
    df, year, week_range, lice_type, location_level, location, y_scale=None
):

    # df_plot = df[(df[location_level.lower()] == location) & (df["year"].between(year_range[0], year_range[1]))]
    df_plot = df[
        (df[location_level].isin(location))
        & (df.year == year)
        & (df.uke.between(week_range[0], week_range[1]))
    ]

    df_plot_agg = (
        df_plot.groupby(["year", "uke", location_level])
        .agg({"total_{}".format(lice_type): "sum", "beholdning": "sum"})
        .reset_index()
    )

    df_plot_agg = df_plot_agg.astype({"uke": int})
    df_plot_agg.sort_values(["uke"])

    df_plot_agg["Mean {} {}".format(lice_type, "per fish")] = (
        df_plot_agg["total_{}".format(lice_type)] / df_plot_agg["beholdning"]
    )

    selection = alt.selection_multi(fields=[location_level], bind="legend")
    y_value = "Mean {} {}".format(lice_type, "per fish")
    chart = (
        alt.Chart(df_plot_agg)
        .mark_line(point={"filled": False, "fill": "white", "strokeWidth": 2})
        .encode(
            x=alt.X(
                "uke",
                title="Uke",
                sort="ascending",
                scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
            ),
            y=alt.Y("Mean {} {}".format(lice_type, "per fish")),
            # size = alt.Size('y:N', legend=None),
            color=alt.Color(
                location_level, title="Luse", scale=alt.Scale(scheme="paired")
            ),
            opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            tooltip=[location_level],
        )
        .properties(width=500, height=350)
        .configure_axis(labelFontSize=15, titleFontSize=15)
        .configure_legend(labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100)
        .add_selection(selection)
        .interactive()
    )

    return df_plot_agg, chart
