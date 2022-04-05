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
import sys
sys.path.append('/home/jovyan/odp-python-sdk')


from odp_vessel_simulator.models.icct.database_functions.lookup_ship_data import fetch_ship_data
from odp_vessel_simulator.models.icct.database_functions.database import get_connection_pool
from odp_vessel_simulator.models.icct.database_functions.database import get_engine
from odp_vessel_simulator.models.icct.database_functions.ais_from_db import get_vessel_type, get_closest_node_from_coord, get_best_path, check_in_regions
from odp_vessel_simulator.models.icct.classes.ship import Ship
from odp_vessel_simulator.models.icct.classes.pollutants import Pollutant




def distance_from_shore(df):
    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
    land = gpd.read_file('/home/jovyan/odp-python-sdk/odp_vessel_simulator/models/icct/data/geometry_data/land')
    island =  gpd.read_file('/home/jovyan/odp-python-sdk/odp_vessel_simulator/models/icct/data/geometry_data/minor_islands')
    world = gpd.GeoDataFrame( pd.concat( [land, island], ignore_index=True) )
    polys=world['geometry']
    df['min_poly']= df['geometry'].apply(lambda x: min(polys,key=x.distance))
    df['p1']= df.apply(lambda x: nearest_points(x.min_poly, x.geometry)[0], axis=1)
    df['distance_from_shore_m'] = df.apply(lambda x: great_circle((x.geometry.y,x.geometry.x),( x.p1.y, x.p1.x)).meters, axis=1)
    return df.drop(['p1','min_poly'], axis=1)


def distance_from_port(cur_pos, start_port, end_port):
        #for now, just finding the great circle distance, not taking ant land into account. 
        #Distance to port is only relevant when you are very close to the port, and then it's not that 
        #relevant to account for land
        return min(great_circle(cur_pos, start_port).km*1000, great_circle(cur_pos, end_port).km*1000)

    
def simulate(ssvid, lon0, lat0, lon1, lat1, tcp, routing_speed, draught, vessel_particulars, table_routing, dist_port=None, dist_shore=None, ddeg=0.22, FINE_ROUTING=0, resolution_minutes=60, cost_density=0.3):


    engine = tcp.getconn()

    start_time = pd.Timestamp('2022-01-01')

    if FINE_ROUTING == 1:
        i0 = get_closest_node_from_coord(
            lon0, lat0, table_routing, engine, ddeg=ddeg)
        i1 = get_closest_node_from_coord(
            lon1, lat1, table_routing, engine, ddeg=ddeg)
        if i0 is None or i1 is None:
            raise Exception('End nodes not found')
        df_path_h4 = get_best_path(
            i0, i1, routing_speed, table_routing, engine, bbox=2, buffer=60, cost_density=cost_density)
        if df_path_h4 is None:
            raise Exception('No route found')
        i0 = get_closest_node_from_coord(
            lon0, lat0, table_routing.replace('h4', 'h5'), engine, ddeg=ddeg)
        i1 = get_closest_node_from_coord(
            lon1, lat1, table_routing.replace('h4', 'h5'), engine, ddeg=ddeg)
        if i0 is None or i1 is None:
            raise Exception('End nodes not found')
        geom = LineString(list(zip(df_path_h4.lon[::4], df_path_h4.lat[::4])))
        # geom=LineString(list(zip(df_path_h4.lon,df_path_h4.lat)))
        df_path_h5 = get_best_path(i0, i1, routing_speed, table_routing.replace(
            'h4', 'h5'), engine, bbox=geom.wkt, buffer=1.5, cost_density=cost_density)
        df_path = df_path_h5
        if df_path_h5 is None:
            df_path = df_path_h4
            print('Fine routing failed')
    elif FINE_ROUTING == 0:
        i0 = get_closest_node_from_coord(
            lon0, lat0, table_routing, engine, ddeg=ddeg)
        i1 = get_closest_node_from_coord(
            lon1, lat1, table_routing, engine, ddeg=ddeg)
        if i0 is None or i1 is None:
            raise Exception('End nodes not found')
        for bbox in [0.5, 1, 2, 4]:
            df_path_h4 = get_best_path(
                i0, i1, routing_speed, table_routing, engine, bbox=bbox, buffer=60, cost_density=cost_density)
            if df_path_h4 is not None:
                continue
            else:
                print('Expanding bbox on route search')

        df_path = df_path_h4
    elif FINE_ROUTING == 2:
        i0 = get_closest_node_from_coord(
            lon0, lat0, table_routing.replace('h4', 'h5'), engine, ddeg=ddeg/10)
        i1 = get_closest_node_from_coord(
            lon1, lat1, table_routing.replace('h4', 'h5'), engine, ddeg=ddeg/10)
        df_path_h5 = get_best_path(i0, i1, routing_speed, table_routing.replace(
            'h4', 'h5'), engine, bbox=True, buffer=10, cost_density=cost_density)
        df_path = df_path_h5

    if df_path is None:
        raise Exception(
            'No route found for given coordinates and routing graph')


    df_path['total_length_m'] = df_path.length_m.cumsum()
    df_path['vessel_spd'][df_path['vessel_spd'] < 5] = 5
    df_path['time_s'] = df_path['length_m']/(df_path['vessel_spd']/1.94384449)
    df_path['duration_s'] = df_path.time_s.cumsum()
    df_path['timestamp'] = df_path.duration_s.apply(
        lambda t: start_time+pd.Timedelta(t, unit='seconds'))
    df_path['interpolated'] = 2

    df_path['speed_knots'] = df_path['vessel_spd']
    df_path['implied_speed_knots'] = df_path['vessel_spd']
    df_path['ssvid'] = ssvid

    df_path.set_index('timestamp', inplace=True)
    df_path.dropna(inplace=True)
    df = df_path.loc[:, ['ssvid', 'lon', 'lat', 'speed_knots', 'implied_speed_knots',
                         'interpolated']].resample(f'{resolution_minutes}min').mean()  # .iloc[1:-1]
    df.dropna(inplace=True)

    _df = check_in_regions(df, engine)
    df['in_a_eca'] = _df['in_a_eca'].values*1
    df['in_a_river'] = _df['in_a_river'].values*1

    if dist_port == None:
        df['distance_from_port_m'] = df.apply(lambda row: distance_from_port((row['lat'],row['lon']), (lat0,lon0), (lat1,lon1)), axis=1)
    else:
        df['distance_from_port_m'] = dist_port
        print(f'Distance from port set to {dist_port}!!')
    
    if dist_shore == None:
        df = distance_from_shore(df)
    else:
        df['distance_from_shore_m'] = dist_shore
        print(f'Distance from shore set to {dist_shore}!!')
    
    
    
    df['draught_recent_m'] = draught

    ship = Ship(vessel_particulars)

    success, df_emissions = ship.compute_emissions(
        df.copy(), pollutants=[p for p in Pollutant if p not in [Pollutant.BC]])

    df_emissions.timestamp = df_emissions.timestamp.astype(str)
    emission_labels = ['emissions_CO2', 'emissions_CO', 'emissions_SOX',
                       'emissions_N2O', 'emissions_NOX', 'emissions_PM', 'emissions_CH4']
    df_emissions[emission_labels] = df_emissions[emission_labels] * \
        (resolution_minutes/60)
    labels = ['ssvid', 'lon', 'lat', 'speed_knots',
              'in_a_eca', 'in_a_river', 'distance_from_port_m',
              'distance_from_shore_m', 'draught_recent_m', 'timestamp',
              'total_power_demand_kw']+emission_labels
    df_emissions.index = df.index

    tcp.putconn(engine)

    return df_emissions[labels]


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
    table_routing_base = "vessel_emissions.ais_gfw_heatmap_h4_{}_crosslines"
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
