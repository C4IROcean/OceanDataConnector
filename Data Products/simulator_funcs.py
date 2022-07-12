import typing
import pandas as pd
import numpy as np
import pydeck as pdk
import geopandas as gpd
from sqlalchemy import create_engine
from os import environ, getenv, path
from typing import TypedDict, Dict, Union, List
from geopy.distance import great_circle
from shapely.ops import nearest_points
from shapely.geometry import LineString
import sys
import logging as log


from odp.geospatial.odp_vessel_simulator.models.icct.database_functions.lookup_ship_data import (
    fetch_ship_data,
)
from odp.geospatial.odp_vessel_simulator.models.icct.database_functions.database import (
    get_connection_pool,
)
from odp.geospatial.odp_vessel_simulator.models.icct.database_functions.database import (
    get_engine,
)
from odp.geospatial.odp_vessel_simulator.models.icct.database_functions.ais_from_db import (
    get_vessel_type,
    get_closest_node_from_coord,
    get_best_path,
    check_in_regions,
)
from odp.geospatial.odp_vessel_simulator.models.icct.classes.ship import Ship
from odp.geospatial.odp_vessel_simulator.models.icct.classes.pollutants import Pollutant
import odp.geospatial.odp_vessel_simulator as odp_vessel_simulator


SDK_DIR = path.dirname(odp_vessel_simulator.__file__)


def distance_from_shore(df):
    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
    land = gpd.read_file(path.join(SDK_DIR, "models/icct/data/geometry_data/land"))
    island = gpd.read_file(
        path.join(SDK_DIR, "models/icct/data/geometry_data/minor_islands")
    )
    world = gpd.GeoDataFrame(pd.concat([land, island], ignore_index=True))
    polys = world["geometry"]
    df["min_poly"] = df["geometry"].apply(lambda x: min(polys, key=x.distance))
    df["p1"] = df.apply(lambda x: nearest_points(x.min_poly, x.geometry)[0], axis=1)
    df["distance_from_shore_m"] = df.apply(
        lambda x: great_circle((x.geometry.y, x.geometry.x), (x.p1.y, x.p1.x)).meters,
        axis=1,
    )
    return df.drop(["p1", "min_poly"], axis=1)


def distance_from_port(cur_pos, start_port, end_port):
    # for now, just finding the great circle distance, not taking ant land into account.
    # Distance to port is only relevant when you are very close to the port, and then it's not that
    # relevant to account for land
    return min(
        great_circle(cur_pos, start_port).km * 1000,
        great_circle(cur_pos, end_port).km * 1000,
    )


def get_lon_lat_ports(_ports):
    # ports is a list of ports on the format ['port1','port2']

    ports = [port.upper() for port in _ports]

    ports_string = "|".join(ports)

    df_all_ports = pd.read_csv("wpi.csv").drop(columns="Unnamed: 0")
    df_ports = df_all_ports[df_all_ports["port_name"].str.fullmatch(ports_string)][
        ["port_name", "country", "longitude", "latitude"]
    ].reset_index(drop=True)

    for port in ports:
        if not df_ports["port_name"].astype(str).str.fullmatch(port).any():
            # Try and find the port using str.contains. If not trying a broader search.
            print(f"Could not find {port}, trying a broader search".format(port))
            df_broader = df_all_ports[
                df_all_ports["port_name"].str.contains(str(port))
            ][["port_name", "country", "longitude", "latitude"]].reset_index(drop=True)
            if len(df_broader) > 0:
                broader_port_list = list(df_broader["port_name"])
                print(f"Found the ports {broader_port_list}".format(broader_port_list))

                num_ports = len(df_broader)
                possible_input = list(range(1, num_ports + 1))
                possible_input.append("N")
                possible_input.append("n")
                port_index = input(
                    f"""Is one of these the right port? 
                                    Type in the index of the right port, 
                                    that is a number between 1 and {num_ports}. 
                                    Press N if all are wrong.""".format(
                        num_ports
                    )
                )
                if port_index != "n" and port_index != "N":
                    port_index = int(port_index)
                while port_index not in possible_input:
                    port_index = input(
                        f"Wrong input, choose between {possible_input}.".format(
                            possible_input
                        )
                    )
                    if port_index != "n" and port_index != "N":
                        port_index = int(port_index)
                if port_index == "N" or port_index == "n":
                    print(" ")
                    print(
                        f"{port} is not in our database. Add its coordinates manually. \n".format(
                            port
                        )
                    )
                    print(" ")
                    ports.remove(port)
                else:
                    port_index = int(port_index) - 1
                    df_ports = pd.concat(
                        [df_ports, df_broader.iloc[[port_index]]], axis=0
                    )
                    ports[ports.index(port)] = df_broader["port_name"][port_index]
            else:
                print(" ")
                print(
                    f"{port} is not in our database. Add its coordinates manually".format(
                        port
                    )
                )
                ports.remove(port)

    df_ports.set_index("port_name", inplace=True)
    df_ports = df_ports.loc[ports].reset_index(drop=False)

    coordinates = list(
        df_ports.apply(lambda row: [row["longitude"], row["latitude"]], axis=1)
    )

    return df_ports, coordinates


def load_ship(mmsi: int):

    sql = f"""
    select "LRIMOShipNo" as imo,
    "ShipName" as name,
    "YearOfBuild" as year,
    "ShiptypeLevel5" as ship_type,
    "ICCT_class" as icct_class,
    "GrossTonnage" as gross_tonnage,
    "FlagName" as flag_name,
    "FuelType1First" as fuel_type,
    "LengthRegistered" as length,
    "PropulsionType" as propulsion
    from ship_data_ihs where "MaritimeMobileServiceIdentityMMSINumber"={mmsi}
    """
    engine = get_engine()
    df = pd.read_sql(sql, engine)
    engine.dispose()

    return df.iloc[0]


def get_routing_table_from_mmsi(mmsi: int) -> str:
    tcp = get_connection_pool()
    engine = tcp.getconn()

    vessel_type = get_vessel_type(mmsi, engine)
    if vessel_type in ["fishing", "trawlers"]:
        vessel_type_routing = "fishing"
    elif vessel_type not in ["passenger", "cargo", "tanker", "tug"]:
        log.warning(
            f"Not implemented routing graph for vessel type {vessel_type}, using cargo"
        )
        vessel_type_routing = "cargo"
    else:

        vessel_type_routing = vessel_type

    return vessel_type_routing


def plot_df(df_points: pd.DataFrame, col: float = "speed_knots", norm: int = 25):
    mid_lon = sum(df_points["lon"]) / len(df_points)
    mid_lat = sum(df_points["lat"]) / len(df_points)

    view_state = pdk.ViewState(longitude=mid_lon, latitude=mid_lat, zoom=3)

    layers = []
    path_layer = pdk.Layer(
        "PathLayer",
        [{"path": list(zip(df_points.lon, df_points.lat))}],
        width_min_pixels=2,
        get_color="[0,0,0, 150]",
    )
    layers.append(path_layer)

    point_cloud_layer = pdk.Layer(
        "PointCloudLayer",
        data=df_points,
        get_position=["lon", "lat"],
        get_color=[
            "10+%s*255/%s*2" % (col, norm),
            "10+%s*255/%s" % (col, norm),
            "10+%s*255/%s*4" % (col, norm),
        ],
        get_normal=[0, 0, 15],
        auto_highlight=True,
        pickable=True,
        point_size=5,
    )
    layers.append(point_cloud_layer)

    r = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style="road",
    )

    return r


def get_end_nodes(
    lon0: float,
    lat0: float,
    lon1: float,
    lat1: float,
    table_routing: str,
    engine,
    ddeg0: float,
):

    ddeg = ddeg0
    i0 = get_closest_node_from_coord(lon0, lat0, table_routing, engine, ddeg=ddeg0)

    while i0 is None:
        ddeg2 = ddeg * 1.1
        print(
            f"""Could not find the port at [{lon0},{lat0}] 
                with a {ddeg} degrees radius, 
                trying with {ddeg2:.4f}."""
        )
        ddeg = ddeg2
        i0 = get_closest_node_from_coord(lon0, lat0, table_routing, engine, ddeg=ddeg)
    if ddeg > ddeg0:
        print(
            f"""The closest node to the port at [{lon0:.4f},{lat0:.4f}] 
                is at [{i0.lon:.4f},{i0.lat:.4f}] using a 
                {ddeg:.4f} degree radius."""
        )

    ddeg = ddeg0
    i1 = get_closest_node_from_coord(lon1, lat1, table_routing, engine, ddeg=ddeg)
    while i1 is None:
        ddeg2 = ddeg * 1.1
        ddeg = ddeg2
        i1 = get_closest_node_from_coord(lon1, lat1, table_routing, engine, ddeg=ddeg)
    if ddeg > ddeg0:
        print(
            f"""The closest node to the port at [{lon1:.4f},{lat1:.4f}] 
            is at [{i1.lon:.4f},{i1.lat:.4f}] using a 
            {ddeg:.4f} degree radius."""
        )

    return i0, i1


def simulate_path_fine_routing_0(
    lon0: float,
    lat0: float,
    lon1: float,
    lat1: float,
    routing_speed: float,
    table_routing: str,
    cost_density: float,
    engine,
    ddeg,
) -> pd.DataFrame:

    i0, i1 = get_end_nodes(lon0, lat0, lon1, lat1, table_routing, engine, ddeg0=ddeg)
    if i0 is None or i1 is None:
        raise Exception("End nodes not found")
    for bbox in [0.5, 1, 2, 4]:
        df_path_h4 = get_best_path(
            i0,
            i1,
            routing_speed,
            table_routing,
            engine,
            bbox=bbox,
            buffer=60,
            cost_density=cost_density,
        )
        if df_path_h4 is not None:
            continue
        else:
            print("Expanding bbox on route search")

    df_path = df_path_h4

    return df_path


def simulate_path_fine_routing_1(
    lon0: float,
    lat0: float,
    lon1: float,
    lat1: float,
    routing_speed: float,
    table_routing: str,
    cost_density: float,
    engine,
    ddeg,
) -> pd.DataFrame:

    i0, i1 = get_end_nodes(lon0, lat0, lon1, lat1, table_routing, engine, ddeg0=ddeg)
    if i0 is None or i1 is None:
        raise Exception("End nodes not found")
    df_path_h4 = get_best_path(
        i0,
        i1,
        routing_speed,
        table_routing,
        engine,
        bbox=2,
        buffer=60,
        cost_density=cost_density,
    )
    if df_path_h4 is None:
        raise Exception("No route found")
    i0, i1 = get_end_nodes(
        lon0, lat0, lon1, lat1, table_routing.replace("h4", "h5"), engine, ddeg0=ddeg
    )
    if i0 is None or i1 is None:
        raise Exception("End nodes not found")
    geom = LineString(list(zip(df_path_h4.lon[::4], df_path_h4.lat[::4])))

    df_path_h5 = get_best_path(
        i0,
        i1,
        routing_speed,
        table_routing.replace("h4", "h5"),
        engine,
        bbox=geom.wkt,
        buffer=1.5,
        cost_density=cost_density,
    )
    df_path = df_path_h5
    if df_path_h5 is None:
        df_path = df_path_h4
        print("Fine routing failed")
    return df_path


def simulate_path_fine_routing_2(
    lon0: float,
    lat0: float,
    lon1: float,
    lat1: float,
    routing_speed: float,
    table_routing: str,
    cost_density: float,
    engine,
    ddeg,
) -> pd.DataFrame:

    i0, i1 = get_end_nodes(
        lon0,
        lat0,
        lon1,
        lat1,
        table_routing.replace("h4", "h5"),
        engine,
        ddeg0=ddeg / 10,
    )
    df_path_h5 = get_best_path(
        i0,
        i1,
        routing_speed,
        table_routing.replace("h4", "h5"),
        engine,
        bbox=True,
        buffer=10,
        cost_density=cost_density,
    )
    df_path = df_path_h5
    return df_path


def simulate_path(
    lon0: float,
    lat0: float,
    lon1: float,
    lat1: float,
    tcp,
    routing_speed: float,
    table_routing: str,
    FINE_ROUTING=0,
    ddeg=0.22,
    resolution_minutes=60,
    cost_density=0.3,
    start_time=pd.Timestamp("2022-01-01"),
):

    engine = tcp.getconn()

    if FINE_ROUTING == 1:
        df_path = simulate_path_fine_routing_1(
            lon0,
            lat0,
            lon1,
            lat1,
            routing_speed,
            table_routing,
            cost_density,
            engine,
            ddeg,
        )
    elif FINE_ROUTING == 0:
        df_path = simulate_path_fine_routing_0(
            lon0,
            lat0,
            lon1,
            lat1,
            routing_speed,
            table_routing,
            cost_density,
            engine,
            ddeg,
        )
    else:
        df_path = simulate_path_fine_routing_2(
            lon0,
            lat0,
            lon1,
            lat1,
            routing_speed,
            table_routing,
            cost_density,
            engine,
            ddeg,
        )

    if df_path is None:
        raise Exception("No route found for given coordinates and routing graph")

    df_path["total_length_m"] = df_path.length_m.cumsum()
    df_path["vessel_spd"][df_path["vessel_spd"] < 5] = 5
    df_path["time_s"] = df_path["length_m"] / (df_path["vessel_spd"] / 1.94384449)
    df_path["duration_s"] = df_path.time_s.cumsum()
    df_path["timestamp"] = df_path.duration_s.apply(
        lambda t: start_time + pd.Timedelta(t, unit="seconds")
    )
    df_path["interpolated"] = 2

    df_path["speed_knots"] = df_path["vessel_spd"]
    df_path["implied_speed_knots"] = df_path["vessel_spd"]

    df_path.set_index("timestamp", inplace=True)
    df_path.dropna(inplace=True)
    df = (
        df_path.loc[
            :,
            [
                "lon",
                "lat",
                "speed_knots",
                "implied_speed_knots",
                "interpolated",
            ],
        ]
        .resample(f"{resolution_minutes}min")
        .mean()
    ).iloc[1:-1]
    df.dropna(inplace=True)

    _df = check_in_regions(df, engine)
    df["in_a_eca"] = _df["in_a_eca"].values * 1
    df["in_a_river"] = _df["in_a_river"].values * 1

    df["distance_from_port_m"] = df.apply(
        lambda row: distance_from_port(
            (row["lat"], row["lon"]), (lat0, lon0), (lat1, lon1)
        ),
        axis=1,
    )
    df = distance_from_shore(df)

    return df


# function to make paths from A to B to C to...
def simulate_long_path(
    list_of_ports: list,
    tcp,
    routing_speed: float,
    table_routing: str,
    FINE_ROUTING=0,
    ddeg=0.22,
    resolution_minutes=60,
    cost_density=0.3,
    start_time=pd.Timestamp("2022-01-01"),
):

    for port_index in range(0, len(list_of_ports) - 1):
        lon0 = list_of_ports[port_index][0]
        lat0 = list_of_ports[port_index][1]
        lon1 = list_of_ports[port_index + 1][0]
        lat1 = list_of_ports[port_index + 1][1]
        df_path_2 = simulate_path(
            lon0,
            lat0,
            lon1,
            lat1,
            tcp,
            routing_speed,
            table_routing,
            FINE_ROUTING,
            ddeg,
            resolution_minutes,
            cost_density,
            start_time,
        )
        if port_index == 0:
            df_path = df_path_2
        else:
            df_path = pd.concat([df_path, df_path_2])

    return df_path


# function that finds emissions from paths
# could also be full paths that for A-B-C-...
# argument: dataframe made with simulate_path or the function that
# simualtes paths for multiple ports (but uses simulate_path to do so)
def emissions_on_simulated_path(
    df: pd.DataFrame, ssvid: int, draught, vessel_particulars, resolution_minutes
) -> pd.DataFrame:

    df["ssvid"] = ssvid
    df["draught_recent_m"] = draught

    ship = Ship(vessel_particulars)

    success, df_emissions = ship.compute_emissions(
        df.copy(), pollutants=[p for p in Pollutant if p not in [Pollutant.BC]]
    )

    df_emissions.timestamp = df_emissions.timestamp.astype(str)
    emission_labels = [
        "emissions_CO2",
        "emissions_CO",
        "emissions_SOX",
        "emissions_N2O",
        "emissions_NOX",
        "emissions_PM",
        "emissions_CH4",
    ]
    df_emissions[emission_labels] = df_emissions[emission_labels] * (
        resolution_minutes / 60
    )
    labels = [
        "ssvid",
        "lon",
        "lat",
        "speed_knots",
        "in_a_eca",
        "in_a_river",
        "distance_from_port_m",
        "distance_from_shore_m",
        "draught_recent_m",
        "timestamp",
        "total_power_demand_kw",
    ] + emission_labels
    df_emissions.index = df.index

    return df_emissions[labels]


def find_routing_speed(routing_speed, vessel_particulars):
    if routing_speed == None:
        return float(vessel_particulars["max_speed"][1])


def find_draught(draught, vessel_particulars):
    if draught == None:
        return vessel_particulars["design_draught"]


def sum_emissions_df(df):
    # Keep only the relevant columns
    cols = [
        "ssvid",
        "emissions_CO2",
        "emissions_CO",
        "emissions_SOX",
        "emissions_N2O",
        "emissions_NOX",
        "emissions_PM",
        "emissions_CH4",
    ]
    # Sum the emissions for the whole trip
    return df[cols].groupby(["ssvid"]).sum()


# function that sums all the emissions for one vessel and comapres it to the other emissions
def compare_emissions(list_of_dfs: list):
    summed_emissions = list(map(sum_emissions_df, list_of_dfs))
    # sum all of the emissions for each vessel
    df_emissions = pd.concat(summed_emissions)
    df_emissions = df_emissions.sort_values(by="emissions_CO2", ascending=True)
    return df_emissions


# function to take in more than one vessel, more than two ports and find paths for the vessels if speed is similar
# and emissions on the paths for each vessel
def emissions_and_paths(
    coordinates,
    df_input,
    time_reso=60,
    detailed_routing=1,
    cost_density=0.30,
    start_time=pd.Timestamp("2022-01-01"),
):
    # reset index here and then set it back at the end of this function
    df_input.reset_index(inplace=True)

    dict_of_paths = {}  # key is the graph type
    list_of_emissions_df = []
    summed_emissions = []

    tcp = get_connection_pool()
    # first find all path using find_long_path
    # either the average of teh speeds or chaching the vessels with the same speed.
    # that means that you need to find the speeds here.
    df_input["vessel_particulars"] = df_input.apply(
        lambda row: fetch_ship_data(tcp, row["index"]), axis=1
    )
    df_input["speeds"] = df_input.apply(
        lambda row: find_routing_speed(row["speeds"], row["vessel_particulars"]), axis=1
    )
    df_input["draughts"] = df_input.apply(
        lambda row: find_draught(row["draughts"], row["vessel_particulars"]), axis=1
    )
    df_input["graphs"] = df_input.apply(
        lambda row: get_routing_table_from_mmsi(row["index"]), axis=1
    )

    # loop over df_input['graph'] to find paths for each graph. use average speed for the graphs.
    for graph in df_input["graphs"].unique():
        df_temp = df_input.loc[df_input["graphs"] == graph]
        if len(df_temp) > 1:
            avg_speed = sum(df_temp["speeds"]) / len(df_temp)
        else:
            avg_speed = df_temp["speeds"].iloc[0]
        table_routing_base = "vessel_emissions.ais_gfw_heatmap_h4_{}_crosslines"
        table_routing = table_routing_base.format(graph)
        df_path = simulate_long_path(
            coordinates,
            tcp,
            avg_speed,
            table_routing,
            FINE_ROUTING=detailed_routing,
            ddeg=0.22,
            resolution_minutes=time_reso,
            cost_density=cost_density,
            start_time=start_time,
        )
        dict_of_paths[graph] = df_path

        if len(df_temp) > 1:
            mmsi_strings = [str(x) for x in df_temp["index"]]
            mmsis_str = " ".join(mmsi_strings)
            print(f"Path finding for vessels with mmmsis {mmsis_str} is done. ")
        else:
            mmsi_one = list(df_temp["index"])[0]
            print(f"Path finding for vessels with mmmsi {mmsi_one} is done. ")

    df_input["emissions_on_path"] = df_input.apply(
        lambda row: emissions_on_simulated_path(
            dict_of_paths[row["graphs"]],
            row["index"],
            row["draughts"],
            row["vessel_particulars"],
            time_reso,
        ),
        axis=1,
    )

    df_input.drop(columns=["vessel_particulars"], inplace=True)
    df_input.set_index("index", inplace=True)
    df_summed_emissions = compare_emissions(df_input["emissions_on_path"])

    return df_input, df_summed_emissions
