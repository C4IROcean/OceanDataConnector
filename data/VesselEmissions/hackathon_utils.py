import os
import zarr
import azure.storage.blob
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from sqlalchemy.engine import create_engine
import pydeck as pdk

def get_files_from_blob(folder,container='vessel-emissions-2020'):
    
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['HACKATHON_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(container)

    file_list = list(set([os.path.join(f'abfs://{container}/',b.name) for b in container_client.walk_blobs(folder, delimiter='/')  ]))
    file_list.sort()
    
    return file_list

def get_zarr_from_blob(folder,container='vessel-emissions-2020'):
    
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['HACKATHON_CONNECTION_STR'])
    container_client = blob_service_client.get_container_client(container)
    
    file_list = list(
        set(
            [
                b.name
                for b in container_client.walk_blobs(folder, delimiter="/")
            ]
        )
    )
    file_list.sort()

    store_list = []
    for file in file_list:
        store = zarr.ABSStore(prefix=file, client=container_client)
        store_list.append(store)
        
        
    return store_list


def get_engine():
    return create_engine((os.environ['HACKATHON_DB_CONNECTION']))

def load_data(mmsi):  # ,engine):

    sql = f'''select *
           from get_emissions_from_vessel({mmsi}) 
           order by timestamp'''
    engine = get_engine()
    df = pd.read_sql(sql, engine)
    engine.dispose()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['datetime'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.set_index('timestamp', inplace=True)
    emissions_parameters = ['emissions_CO2', 'emissions_CO', 'emissions_SOX',
                            'emissions_N2O', 'emissions_NOX', 'emissions_PM', 'emissions_CH4']
    df.loc[:, emissions_parameters] = df[emissions_parameters].div(1000)
    # df=df[['lon','lat', 'speed_knots','emissions_CO2', 'emissions_CO', 'emissions_SOX',
    # 'emissions_N2O', 'emissions_NOX', 'emissions_PM', 'emissions_CH4']]
    df.sort_index(inplace=True)

    #df['interpolated'][df['interpolated'] == 0] = 'No (reported postion)'
    #df['interpolated'][df['interpolated'] == 1] = 'Linear (small gap)'
    #df['interpolated'][df['interpolated'] == 2] = 'Routed (large gap)'

    return df

def plot_df(point=None, ports=None, col='speed', norm=20):
    layers = []

    point_cloud_layer = pdk.Layer(
        "PointCloudLayer",
        # [['lon', 'lat', 't', 'seabed', 'NASC', 'clusters', 'nm','color']],
        data=point,
        get_position=["lon", "lat"],
        # get_color=[f"{col}*2*{norm}", f"{col}*{norm}", f"255-{col}*{norm}"],#"125+color/2"],#,"60+color*2"],
        get_color=['10+%s*255/%s*2' % (col, norm), '10+%s*255/%s' %
                   (col, norm), '10+%s*255/%s*4' % (col, norm)],
        # get_color=['57', '117', '201','20+%s*255/%s'%(col,norm)],
        # get_color=['40+%s*225/%s*4'%(col,norm), '20+%s*225/%s*2'%(col,norm), '10+%s*225/%s'%(col,norm)],
        get_normal=[0, 0, 15],
        auto_highlight=True,
        pickable=True,
        point_size=5,
    )
    layers.append(point_cloud_layer)

    point['difference'] = (point.lon - point.lon.shift()).abs()
    if point['difference'].max() > 90:
        gaps = point.index[point['difference'] > 90]
        path_list = []
        i0 = 0
        for n in range(len(gaps)):
            i = np.where(point.index == gaps[n])[0][0]
            path_list.append(
                {'path': list(zip(point.lon.iloc[i0:i], point.lat.iloc[i0:i]))})
            i0 = i
        path_list.append(
            {'path': list(zip(point.lon.iloc[i:], point.lat.iloc[i:]))})
    else:
        path_list = [{'path': list(zip(point.lon, point.lat))}]
    point.drop('difference', axis=1, inplace=True)
    layer = pdk.Layer(
        'PathLayer',
        path_list,
        # [{'path':list(zip(point.lon,point.lat))}],
        width_min_pixels=2,
        get_color='[0,0,0, 150]',
    )
    layers.append(layer)

    view = pdk.data_utils.compute_view(point[["lon", "lat"]].dropna())

    if ports is not None:
        ports_layer = pdk.Layer("PointCloudLayer",
                                # [['lon', 'lat', 't', 'seabed', 'NASC', 'clusters', 'nm','color']],
                                data=ports,
                                get_position=["lon", "lat"],
                                # ['246', '51', '102'],#['207', '224', '247'],#
                                get_color=['10', '21', '48'],
                                point_size=13,
                                pickable=True
                                )

        layers.append(ports_layer)
        ##view = pdk.data_utils.compute_view(poly)
    tooltip_str = ""
    for key in point.keys()[2:]:
        tooltip_str += "<b>%s: </b> {%s} <br /> " % (key, key)
    #tooltip_str += "<b>Port: </b> {port_name} <br /> "
    tooltip = {"html": tooltip_str[:-7]}

    r = pdk.Deck(layers=layers[::-1],
                 initial_view_state=view,
                 tooltip=tooltip,
                 map_style='light'
                 )

    return r

def plot_emissions_density_ports(_ds,_df_ports):
    p = _ds.plot.pcolormesh(
        transform=ccrs.PlateCarree(),
        cmap=plt.get_cmap('inferno'),
        vmax=_ds.compute().quantile(0.97),
        subplot_kws={'projection': ccrs.Mercator()}
    )
    ax = p.axes
    ax.figure.set_size_inches(12,12)
    ax.coastlines(color='white')
    ax.scatter(_df_ports.Longitude,_df_ports.Latitude,
                transform=ccrs.PlateCarree(), 
                #color="blue", 
                color="#03FFD1",
                s=150, alpha=1, zorder=2.5)
    
    
def get_lon_lat_ports(_ports):
    #ports is a list of ports on the format ['port1','port2']
    
    ports = [port.title() for port in _ports]
    
    ports_string = '|'.join(ports)
    
    #df_all_ports = pd.read_csv("wpi.csv").drop(columns="Unnamed: 0")
    df_all_ports=pd.read_csv(get_files_from_blob('csv/world_port_index/')[0], storage_options={"connection_string": os.environ['HACKATHON_CONNECTION_STR']})
    
    df_ports = df_all_ports[df_all_ports['Main Port Name'].str.fullmatch(ports_string)][['Main Port Name','Country Code','Latitude','Longitude']].reset_index(drop=True)
    
    
    for port in ports:
        if not df_ports['Main Port Name'].astype(str).str.fullmatch(port).any():
            #Try and find the port using str.contains. Give options if there are more than one???
            print(f'Could not find {port}, trying a broader search'.format(port))
            df_broader = df_all_ports[df_all_ports['Main Port Name'].str.contains(str(port))][['Main Port Name','Country Code','Latitude','Longitude']].reset_index(drop=True)
            if len(df_broader)>0:
                broader_port_list = list(df_broader['Main Port Name'])
                print(f'Found the ports {broader_port_list}'.format(broader_port_list))
                
                num_ports = len(df_broader)
                possible_input = list(range(1,num_ports+1))
                possible_input.append('N')
                possible_input.append('n')
                port_index = input(f'Is one of these the right port? Type in the index of the right port, that is a number between 1 and {num_ports}. Press N if all are wrong.'.format(num_ports))
                if port_index != 'n' and port_index != 'N':
                    port_index = int(port_index)
                while port_index not in possible_input:
                    port_index = input(f'Wrong input, choose between {possible_input}.'.format(possible_input))
                    if port_index != 'n' and port_index != 'N':
                        port_index = int(port_index)
                if port_index == 'N' or port_index == 'n':
                    print(' ')
                    print(f'{port} is not in our database. Add its coordinates manually. \n'.format(port))
                    print(' ')
                    ports.remove(port)
                else:
                    port_index = int(port_index)-1
                    df_ports = pd.concat([df_ports,df_broader.iloc[[port_index]]],axis=0)
                    ports[ports.index(port)] = df_broader['Main Port Name'][port_index]
            else: 
                print(' ')
                print(f'{port} is not in our database. Add its coordinates manually'.format(port))
                ports.remove(port)
                #fix this, so taht not duplicated code!
                
    df_ports.set_index('Main Port Name',inplace=True)
    df_ports=df_ports.loc[ports].reset_index(drop=False)
    
    coordinates = list(df_ports.apply(lambda row:[row['Longitude'],row['Latitude']],axis=1))
    
    return df_ports, coordinates


        
    
