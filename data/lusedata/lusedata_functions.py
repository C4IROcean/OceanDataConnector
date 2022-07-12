import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import altair as alt
import geopandas as gpd
import pydeck as pdk
from PIL import Image
from os import environ, getenv, path
from datetime import date, datetime, timedelta
from jinjasql import JinjaSql
import unidecode
import warnings
import os
from os import environ, getenv, path


def make_engine():
    """Connect to database"""

    engine = create_engine(
        getenv("DATABASE_URL"),
        connect_args={"application_name": "hub_explorer", "sslmode": "require"},
    )

    return engine


def pull_region():
    return ["Nord-Norge", "Midt-Norge", "Vest-Norge"]


def pull_produksjonomrade():
    engine = make_engine()
    query = """select distinct produksjonsomrade, produksjonsomradenr
               from raw_ops_seafood.lokalitet
               order by  produksjonsomradenr"""
    df_prod = pd.read_sql(query, engine)
    df_prod["prod_nummer"] = (
        df_prod["produksjonsomradenr"].astype(int).astype(str)
        + " "
        + df_prod["produksjonsomrade"]
    )
    return df_prod


def pull_subregion_list():
    engine = make_engine()
    query = """select distinct subregion
               from raw_ops_seafood.lokalitet 
               order by subregion"""
    df = pd.read_sql(query, engine)
    return df["subregion"]


def pull_zone_list():
    engine = make_engine()
    query = """select distinct sone
               from raw_ops_seafood.lokalitet 
               order by sone"""
    df = pd.read_sql(query, engine)
    return df


def pull_location_list_map(sone: str, year=None):
    engine = make_engine()
    # if len(year_range) > 1:
    #     years = list(range(year_range[0], year_range[1] + 1))
    # else:
    # years = year_range
    if year:
        params = {"sone": sone, "year": year}
        query = """select distinct lokalitet
                   from raw_ops_seafood.joined_salmon_lice_data 
                   where sone = {{sone}}
                   and year ={{year}}
                   order by lokalitet"""

    else:
        params = {"sone": sone}
        query = """select distinct lokalitet
                   from raw_ops_seafood.joined_salmon_lice_data 
                   where sone = {{sone}}
                   order by lokalitet"""

    j = JinjaSql(param_style="pyformat")
    query, bind_params = j.prepare_query(query, params)
    df = pd.read_sql(query, engine, params=bind_params)
    df["lokalitet"] = df["lokalitet"].str.lower().str.title()
    return df["lokalitet"].to_list()


def get_location_list(location_level, sone=None, year=None):
    # location = "Norge"
    # locality = "-"

    if location_level == "Region":
        location = ["Nord-Norge", "Midt-Norge", "Vest-Norge"]
    elif location_level == "Produksjonsområde":
        df_prod = pull_produksjonomrade()
        df_prod = df_prod[["prod_nummer"]]
        # dd = dict(zip(df_prod.numbered, df_prod.produksjonsomrade))
        # location = sidebar.selectbox("Produksjonsområde", df_prod['numbered'])
        location = df_prod
    elif location_level == "Område":
        location = pull_subregion_list()
    elif location_level == "Sone":
        location = pull_zone_list()
    elif location_level == "Lokalitet":
        location = pull_location_list_map(sone)

    return location


def df_avg_lice(year_range, week_range, location_level, location):
    engine = make_engine()

    weeks = list(range(week_range[0], week_range[1] + 1))
    if len(year_range) > 1:
        years = list(range(year_range[0], year_range[1] + 1))
    else:
        years = year_range
    if location_level == "Nasjonal":
        params = {"year": years, "weeks": weeks}

        query = """
        with aggregate_query as (
          select uke,
            year,
            avg(sjotemperatur) as sjotemperatur,
            sum(total_voksne_hunnlus) as total_voksne_hunnlus,
            sum(total_bevegelige_lus) as total_bevegelige_lus,
            sum(total_fastsittende_lus) as total_fastsittende_lus,
            sum(beholdning) as beholdning
          from raw_ops_seafood.joined_salmon_lice_data
            where uke in {{ weeks | inclause }}
            and year in {{ year | inclause }}
          group by
            uke,
            year
        )
        select uke,
          year,
          sjotemperatur,
          total_voksne_hunnlus / beholdning as voksne_hunnlus,
          total_bevegelige_lus / beholdning as bevegelige_lus,
          total_fastsittende_lus / beholdning as fastsittende_lus
        from aggregate_query
        order by uke
        """

    else:
        params = {
            "year": years,
            "weeks": weeks,
            "location_level": unidecode.unidecode(location_level.lower()),
            "location": location,
            "table": "joined_salmon_lice_data",
        }

        query = """
        with aggregate_query as (
          select uke,
            year,
            avg(sjotemperatur) as sjotemperatur,
            sum(total_voksne_hunnlus) as total_voksne_hunnlus,
            sum(total_bevegelige_lus) as total_bevegelige_lus,
            sum(total_fastsittende_lus) as total_fastsittende_lus,
            sum(beholdning) as beholdning
          from raw_ops_seafood.joined_salmon_lice_data
          where {{table|sqlsafe}}.{{location_level | sqlsafe}} = '{{location|sqlsafe}}'
            and uke in {{ weeks | inclause }}
            and year in {{ year | inclause }}
          group by
            uke,
            year
        )
        select uke,
          year,
          sjotemperatur,
          total_voksne_hunnlus / beholdning as voksne_hunnlus,
          total_bevegelige_lus / beholdning as bevegelige_lus,
          total_fastsittende_lus / beholdning as fastsittende_lus
        from aggregate_query
        order by uke
    """

    j = JinjaSql(param_style="pyformat")
    query, bind_params = j.prepare_query(query, params)
    df = pd.read_sql(query, engine, params=bind_params)

    df = df.rename(columns={"year": "år"})
    df["uke"] = df["uke"].astype(int)
    df = df.sort_values("uke")
    df = df.reset_index(drop=True)
    return df.round(2)


def average_lice_plots(
    year_range, week_range, lice_type, location_level, location, y_scale=None
):
    df_plot_agg = df_avg_lice(year_range, week_range, location_level, location)
    df_plot_agg["year"] = df_plot_agg["år"]
    df_plot_agg = df_plot_agg.astype({"uke": int, "year": int})
    df_plot_agg.sort_values(["year", "uke"])

    selection = alt.selection_multi(fields=["year"], bind="legend")
    y_value = lice_type

    if lice_type == "voksne_hunnlus":
        plot_title = "Voksne hunnlus per fisk i {}".format(location)
        y_title = "Voksne hunnlus per fisk"
    elif lice_type == "fastsittende_lus":
        plot_title = "Fastsittende lus per fisk i {}".format(location)
        y_title = "Fastsittende lus per fisk"
    else:
        plot_title = "Bevegelige lus per fisk i {}".format(location)
        y_title = "Bevegelige lus per fisk"
    df_plot_agg["y"] = df_plot_agg["year"]

    if y_scale:
        chart = (
            alt.Chart(df_plot_agg)
            .mark_line()
            .encode(
                x=alt.X(
                    "uke",
                    title="Uke",
                    sort="ascending",
                    scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
                ),
                y=alt.Y(
                    lice_type,
                    title=y_title,
                    scale=alt.Scale(domain=[y_scale[0], y_scale[1]]),
                ),
                size=alt.Size("y:N", legend=None),
                color=alt.Color(
                    "year:N",
                    title="År",
                    scale=alt.Scale(
                        domain=df_plot_agg["year"].unique(), scheme="paired"
                    ),
                ),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
                tooltip=[alt.Tooltip(lice_type, format=",.2f", title="Lus per fisk")],
            )
            .properties(title=plot_title)
            .configure_axis(labelFontSize=15, titleFontSize=15)
            .configure_legend(
                labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100
            )
            .add_selection(selection)
            .interactive()
        )

    else:
        chart = (
            alt.Chart(df_plot_agg)
            .mark_line()
            .encode(
                x=alt.X(
                    "uke",
                    title="Uke",
                    sort="ascending",
                    scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
                ),
                y=alt.Y(lice_type, title=y_title),
                size=alt.Size("y:N", legend=None),
                color=alt.Color(
                    "year:N",
                    title="År",
                    scale=alt.Scale(
                        domain=df_plot_agg["year"].unique(), scheme="paired"
                    ),
                ),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
                tooltip=[alt.Tooltip(lice_type, format=",.2f", title="Lus per fisk")],
            )
            .properties(title=plot_title)
            .configure_axis(labelFontSize=15, titleFontSize=15)
            .configure_legend(
                labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100
            )
            .add_selection(selection)
            .interactive()
        )
    return chart


def average_sjotemp(year_range, week_range, location_level, location, y_scale=None):
    if location_level == "Nasjonal":
        plot_title = "Havtemperatur i Norge"
    else:
        plot_title = "Havtemperatur i {}".format(location)

    df_plot_agg = df_avg_lice(year_range, week_range, location_level, location)
    df_plot_agg["year"] = df_plot_agg["år"]

    df_plot_agg = df_plot_agg.astype({"uke": int, "year": int})
    df_plot_agg.sort_values(["year", "uke"])
    df_plot_agg["y"] = df_plot_agg["year"]
    df_plot_agg["sjotemperatur"] = df_plot_agg["sjotemperatur"].round(2)

    selection = alt.selection_multi(fields=["year"], bind="legend")

    if y_scale:
        chart = (
            alt.Chart(df_plot_agg)
            .mark_line()
            .encode(
                x=alt.X(
                    "uke",
                    title="Uke",
                    sort="ascending",
                    scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
                ),
                y=alt.Y(
                    "sjotemperatur",
                    title="Gjennomsnittlig havtemperatur",
                    scale=alt.Scale(domain=[y_scale[0], y_scale[1]]),
                ),
                size=alt.Size("y:N", legend=None),
                color=alt.Color("year:N", title="År"),
                scale=alt.Scale(domain=df_plot_agg["year"].unique(), scheme="paired"),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
                tooltip=[alt.Tooltip("sjotemperatur")],
            )
            .properties(title=plot_title)
            .configure_axis(labelFontSize=15, titleFontSize=15)
            .configure_legend(
                labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100
            )
            .add_selection(selection)
            .interactive()
        )

    else:
        chart = (
            alt.Chart(df_plot_agg)
            .mark_line()
            .encode(
                x=alt.X(
                    "uke",
                    title="Uke",
                    sort="ascending",
                    scale=alt.Scale(domain=(week_range[0] - 1, week_range[1] + 1)),
                ),
                y=alt.Y("sjotemperatur", title="Gjennomsnittlig havtemperatur"),
                size=alt.Size("y:N", legend=None),
                color=alt.Color(
                    "year:N",
                    title="År",
                    scale=alt.Scale(
                        domain=df_plot_agg["year"].unique(), scheme="paired"
                    ),
                ),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
                tooltip=[alt.Tooltip("sjotemperatur")],
            )
            .properties(title=plot_title)
            .configure_axis(labelFontSize=15, titleFontSize=15)
            .configure_legend(
                labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100
            )
            .add_selection(selection)
            .interactive()
        )

    return chart


def plot_treatments(
    year_range, week_range, location_level, location, treatments, y_scale
):
    engine = make_engine()

    if location_level == "Produksjonsområde":
        location_level = "Produksjonsomrade"
    elif location_level == "Område":
        location_level = "Omrade"
    else:
        location_level = location_level

    weeks = list(range(week_range[0], week_range[1] + 1))
    if len(year_range) > 1:
        years = list(range(year_range[0], year_range[1] + 1))
    else:
        years = year_range

    if location_level == "Nasjonal":
        params = {"year": years, "week": weeks}

        query = """
        with aggregate_query as (
          select uke,
            year,
            badebehandling,
            forbehandling,
            mekanisk_fjerning,
            utsett_av_rensefisk,
            sum(total_voksne_hunnlus) as total_voksne_hunnlus,
            sum(beholdning) as beholdning
          from raw_ops_seafood.joined_salmon_lice_data
            where uke in {{ week | inclause }}
            and year in {{ year | inclause }}
          group by
            uke,
            year,
            badebehandling,
            forbehandling,
            mekanisk_fjerning,
            utsett_av_rensefisk
        )
        select uke,
          year,
          badebehandling,
          forbehandling,
          mekanisk_fjerning,
          utsett_av_rensefisk,
          beholdning,
          total_voksne_hunnlus
        from aggregate_query
        order by uke
        """

    else:
        params = {
            "year": years,
            "week": weeks,
            "location_level": location_level.lower(),
            "location": location,
            "table": "joined_salmon_lice_data",
        }

        query = """
        with aggregate_query as (
          select uke,
            year,
            badebehandling,
            forbehandling,
            mekanisk_fjerning,
            utsett_av_rensefisk,
            sum(total_voksne_hunnlus) as total_voksne_hunnlus,
            sum(beholdning) as beholdning
          from raw_ops_seafood.joined_salmon_lice_data
            where {{table|sqlsafe}}.{{location_level | sqlsafe}} = '{{location|sqlsafe}}'
            and uke in {{ week | inclause }}
            and year in {{ year | inclause }}
          group by
            uke,
            year,
            badebehandling,
            forbehandling,
            mekanisk_fjerning,
            utsett_av_rensefisk
        )
        select uke,
          year,
          badebehandling,
          forbehandling,
          mekanisk_fjerning,
          utsett_av_rensefisk,
          beholdning,
          total_voksne_hunnlus
        from aggregate_query
        order by uke
        """

    j = JinjaSql(param_style="pyformat")
    query, bind_params = j.prepare_query(query, params)
    df = pd.read_sql(query, engine, params=bind_params)

    agg_dict = {}
    for treatment_type in treatments:
        # Need to cast the numbers in the different treatments to ints
        df[treatment_type] = pd.to_numeric(df[treatment_type])
        # find how many fish that are in treatment
        df["total_{}".format(treatment_type)] = df["beholdning"] * df[treatment_type]
        agg_dict["total_{}".format(treatment_type)] = "sum"

    agg_dict["beholdning"] = "sum"
    agg_dict["total_voksne_hunnlus"] = "sum"

    df_plot_agg = df.groupby(["uke"]).agg(agg_dict).reset_index()

    # need a dataframe w the columns week, number of fish treated and treatment_type
    df_pivot = pd.DataFrame()
    for treatment_type in treatments:
        df_temp = df_plot_agg[
            [
                "uke",
                "total_{}".format(treatment_type),
                "total_voksne_hunnlus",
                "beholdning",
            ]
        ]
        df_temp.insert(0, "type_behandling", treatment_type)
        df_temp = df_temp.rename(columns={"total_{}".format(treatment_type): "antall"})
        df_temp["average"] = df_temp["total_voksne_hunnlus"] / df_temp["beholdning"]
        df_pivot = pd.concat([df_pivot, df_temp])

    base = alt.Chart(df_pivot).encode(
        alt.X(
            "uke",
            axis=alt.Axis(title="Uke"),
            scale=alt.Scale(domain=(week_range[0], week_range[1] + 1)),
        )
    )

    selection = alt.selection_multi(fields=["type_behandling"], bind="legend")

    if y_scale > 0:
        treatments = (
            base.mark_bar()
            .encode(
                alt.Y(
                    "sum(antall)",
                    title="Antall behandlinger",
                    scale=alt.Scale(domain=[0, y_scale]),
                ),
                tooltip=[alt.Tooltip("sum(antall)", title="antall behandlinger")],
                color=alt.Color(
                    "type_behandling",
                    title="Type behandling",
                    scale=alt.Scale(scheme="paired"),
                ),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            )
            .add_selection(selection)
        )

        total = base.mark_line(stroke="red", strokeDash=[7, 2]).encode(
            y=alt.Y("average", title="Gjennomsnittlig antall voksne hunnlus")
        )

    else:
        treatments = (
            base.mark_bar()
            .encode(
                alt.Y("sum(antall)", title="Antall behandlinger"),
                tooltip=[alt.Tooltip("sum(antall)", title="antall behandlinger")],
                color=alt.Color(
                    "type_behandling",
                    title="Type behandlinger",
                    scale=alt.Scale(scheme="paired"),
                ),
                opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            )
            .add_selection(selection)
        )

        total = base.mark_line(stroke="red", strokeDash=[7, 2]).encode(
            y=alt.Y("average", title="Gjennomsnittlig antall voksne hunnlus")
        )

    chart = (
        alt.layer(treatments, total)
        .resolve_scale(y="independent")
        .properties(
            width=800,
            height=500,
            title="Totalt antall behandlinger og voksne hunnlus i {}, {}".format(
                location, str(year_range[0])
            ),
        )
        .add_selection(selection)
        .interactive()
    )

    return chart


def average_lice_compare(
    years, week_range, lice_type, location_level, location_comp, location, y_scale=None
):
    engine = make_engine()
    weeks = list(range(week_range[0], week_range[1] + 1))
    params = {
        "year": years,
        "weeks": weeks,
        "location_level": unidecode.unidecode(location_level.lower()),
        "location": location_comp,
        "table": "joined_salmon_lice_data",
    }

    query = """
    with aggregate_query as (
      select uke,
        year,
        lokalitet,
        sum(total_voksne_hunnlus) as total_voksne_hunnlus,
        sum(total_bevegelige_lus) as total_bevegelige_lus,
        sum(total_fastsittende_lus) as total_fastsittende_lus,
        sum(beholdning) as beholdning
      from raw_ops_seafood.joined_salmon_lice_data
      where {{table|sqlsafe}}.{{location_level | sqlsafe}} = '{{location|sqlsafe}}'
        and uke in {{ weeks | inclause }}
        and year = {{ year }}
      group by
        uke,
        year,
        lokalitet
    )
    select uke,
      year,
      lokalitet,
      total_voksne_hunnlus / beholdning as voksne_hunnlus,
      total_bevegelige_lus / beholdning as bevegelige_lus,
      total_fastsittende_lus / beholdning as fastsittende_lus
    from aggregate_query
    order by uke
    """

    j = JinjaSql(param_style="pyformat")
    query, bind_params = j.prepare_query(query, params)
    df = pd.read_sql(query, engine, params=bind_params)
    df = df[df["lokalitet"].isin(location)]
    df = df.astype({"uke": int})
    df.sort_values(["uke"])

    if lice_type == "voksne_hunnlus":
        plot_title = "Voksne hunnlus per fisk i {}".format(location_comp)
        y_title = "Voksne hunnlus per fisk"
    elif lice_type == "fastsittende_lus":
        plot_title = "Fastsittende lus per fisk i {}".format(location_comp)
        y_title = "Fastsittende lus per fisk"
    else:
        plot_title = "Bevegelige lus per fisk i {}".format(location_comp)
        y_title = "Bevegelige lus per fisk"

    selection = alt.selection_multi(fields=["lokalitet"], bind="legend")
    y_value = lice_type
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "uke", title="Uke", sort="ascending", scale=alt.Scale(domain=(0, 52))
            ),
            y=alt.Y(lice_type, title=y_title),
            color=alt.Color(
                "lokalitet", title="Lokalitet", scale=alt.Scale(scheme="paired")
            ),
            opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            tooltip=[alt.Tooltip(lice_type, title="Lus per fisk")],
        )
        .properties(title=plot_title)
        .configure_axis(labelFontSize=15, titleFontSize=15)
        .configure_legend(labelFontSize=12, columns=1, labelLimit=500, symbolLimit=100)
        .add_selection(selection)
        .interactive()
    )

    return chart


def get_data(year_range, week_range, location_level, location, limit=45000):
    engine = make_engine()

    weeks = list(range(week_range[0], week_range[1] + 1))
    if len(year_range) > 1:
        years = list(range(year_range[0], year_range[1] + 1))
    else:
        years = year_range

    if location_level == "Nasjonal":

        params = {"year": years, "weeks": weeks, "limit": limit}
        query = """
          select *
          from raw_ops_seafood.joined_salmon_lice_data
            where uke in {{ weeks | inclause }}
            and year in {{ year | inclause }}
        order by year, uke
        limit {{limit}}
        """

    else:
        params = {
            "year": years,
            "weeks": weeks,
            "location_level": unidecode.unidecode(location_level.lower()),
            "location": location,
            "limit": limit,
            "table": "joined_salmon_lice_data",
        }

        query = """
          select *
          from raw_ops_seafood.joined_salmon_lice_data
          where {{table|sqlsafe}}.{{location_level | sqlsafe}} = '{{location|sqlsafe}}'
            and uke in {{ weeks | inclause }}
            and year in {{ year | inclause }}
        order by year, uke
        limit {{limit}}
    """

    j = JinjaSql(param_style="pyformat")
    query, bind_params = j.prepare_query(query, params)
    df = pd.read_sql(query, engine, params=bind_params)

    if df.shape[0] == limit:
        warnings.warn(
            "You have reached the memory limit, please make a smaller selection"
        )

    else:
        return df.drop(["index", "_last_modified"], axis=1)
