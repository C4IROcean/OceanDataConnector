import typing
import pandas as pd
import numpy as np
import pydeck as pdk
from sqlalchemy import create_engine
from os import environ, getenv, path
from typing import TypedDict, Dict, Union, List
import os
os.chdir('/Users/johanne.skogvang/Projects.tmp/ODP_2022/app-vessel-simulator')
from odp_vessel_simulator.app.vessel_emissions_simulation.emissions_from_routing import simulate
#from app.vessel_emissions_simulation.app import load_ship

from odp_vessel_simulator.models.icct.database_functions.lookup_ship_data import fetch_ship_data
from odp_vessel_simulator.models.icct.database_functions.database import get_connection_pool
from odp_vessel_simulator.models.icct.database_functions.database import get_engine
from odp_vessel_simulator.models.icct.database_functions.ais_from_db import get_vessel_type

def load_ship(mmsi: int):  # ,engine):

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
    engine = get_engine()
    vessel_type = get_vessel_type(mmsi, engine)
    engine.dispose()
    if vessel_type in ["fishing", "trawlers"]:
        vessel_type_routing = "fishing"
    elif vessel_type not in ["passenger", "cargo", "tanker", "tug"]:
        log.warning(
            f"Not implemented routing graph for vessel type {vessel_type}, using cargo")
        vessel_type_routing = "cargo"
    else:

        vessel_type_routing = vessel_type

    return vessel_type_routing

def plot_df(df_points: pd.DataFrame, col:float="speed_knots", norm:int=25):
    view_state = pdk.ViewState(
        longitude=10,
        latitude=57.2,
        zoom=3
#        min_zoom=5,
#        max_zoom=15,
        )
        #pitch=40.5,
        #bearing=-27.36)

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
        get_color=["10+%s*255/%s*2" % (col, norm), "10+%s*255/%s" % (col, norm), "10+%s*255/%s*4" % (col, norm)],
        get_normal=[0, 0, 15],
        auto_highlight=True,
        pickable=True,
        point_size=5,
    )
    layers.append(point_cloud_layer)
    

    r = pdk.Deck(layers=layers,initial_view_state=view_state,map_style="road",)
    
    return r

def get_routing_and_emissions(mmsi, coords, dist_port, dist_shore, routing_speed=None, draught=None, graph=None, time_resolution=60, detailed_routing=0, cost_density=0.3):
    tcp = get_connection_pool()

    vessel_particulars = fetch_ship_data(tcp, mmsi)
    
    if routing_speed == None:
        routing_speed = float(vessel_particulars["max_speed"][1])
        if routing_speed == 0.0 or type(routing_speed)!=float :
            log.warning("Could not find max speed to use for routing speed, setting it to 10. If another value is wanted, specify routing_speed when calling the function.")
            routing_speed=10

    if draught==None:
        draught = vessel_particulars["design_draught"]
        if draught==0.0:
            log.warning("""Could not find draught, setting it to 7. Another value can be cohse by specifying draught when calling the function.""")
            draught=7
    
    if graph == None:
        #Choose graph type:
        #graph_types = ["passenger", "fishing", "cargo", "tanker", "tug"]
        graph = get_routing_table_from_mmsi(mmsi) 

        
    #find the right table to pull from database:
    table_routing_base = "ais_gfw_heatmap_h4_{}_crosslines"
    table_routing = table_routing_base.format(graph)


    
    #Find dataframe with routing values and emissions for the first path
    lon0 = coords[0][0]
    lat0 = coords[0][1]
    lon1 = coords[1][0]
    lat1 = coords[1][1]
    df = simulate(mmsi, lon0, lat0, lon1, lat1, tcp, routing_speed, draught, 
              vessel_particulars, table_routing, dist_port, dist_shore, ddeg=10, FINE_ROUTING=detailed_routing, 
              resolution_minutes=time_resolution, cost_density=cost_density)
    
    #If there are more than two points we need to append that route here
    for i in range(1,(len(coords)-1)):
        df_append = simulate(mmsi, coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1], tcp, routing_speed, draught, 
              vessel_particulars, table_routing, dist_port, dist_shore, ddeg=10, FINE_ROUTING=detailed_routing, 
              resolution_minutes=time_resolution, cost_density=cost_density)
        #taking away the first row in df_append as this is the same as the last row in df
        df = df.append(df_append.iloc[1: , :])

    r = plot_df(df)

    return df, r


def compare_emissions(mmsis, coordinates):
    list_of_df = []
    list_of_path = []
    summed_emissions = []
    for mmsi in mmsis:
        df, r = get_routing_and_emissions(mmsi,coords = coordinates)
        cols = ["ssvid", "emissions_CO2", "emissions_CO","emissions_SOX", 
                "emissions_N2O", "emissions_NOX", "emissions_PM", "emissions_CH4" ]
        df_summed_em = df[cols].groupby(["ssvid"]).sum()

        list_of_df.append(df)
        list_of_path.append(r)
        summed_emissions.append(df_summed_em)
                
    df_emissions = pd.concat(summed_emissions)
    df_emissions = df_emissions.sort_values(by="emissions_CO2", ascending=True)
    
    return df_emissions, list_of_path


def emissions_multiple_vessels(coordinates, df_input,dist_port, dist_shore, time_reso,detailed_routing, cost_density):
    list_of_df = []
    list_of_paths = []
    summed_emissions = []
    for mmsi in df_input.index:
        df, r = get_routing_and_emissions(mmsi,coords = coordinates, dist_port=dist_port, dist_shore=dist_shore,
                                          routing_speed=df_input['speeds'][mmsi], 
                                          draught=df_input['draughts'][mmsi], 
                                          graph=df_input['graphs'][mmsi],
                                          time_resolution=time_reso, 
                                          detailed_routing=detailed_routing, 
                                          cost_density=cost_density)

        #Keep only the relevant columns
        cols = ["ssvid", "emissions_CO2", "emissions_CO","emissions_SOX", 
                "emissions_N2O", "emissions_NOX", "emissions_PM", "emissions_CH4" ]
        #Sum the emissions for the whole trip
        df_summed_em = df[cols].groupby(["ssvid"]).sum()

        #append the results to the lists
        list_of_df.append(df)
        list_of_paths.append(r)
        summed_emissions.append(df_summed_em)
    
    #making a dataframe with all the emissions summed
    df_emissions = pd.concat(summed_emissions)
    df_emissions = df_emissions.sort_values(by="emissions_CO2", ascending=True)
    
    return df_emissions, list_of_df, list_of_paths
