import yaml
import dask_geopandas
from tqdm import tqdm
import pandas as pd

CONFIG_FILE = './config.yaml'

class blob_param:
    def __init__(self, CONFIG_FILE, container_name):
        with open(CONFIG_FILE, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
        self.storage_account_name = config['storage_account_name']
        self.storage_account_key = config['storage_account_key'][container_name]
        self.blob_chunk = config['blob_chunk'][container_name]
        self.container = container_name
        self.sas_url_list = [self.storage_account_name+'/'+self.container+'/'+name_file+'?'+self.storage_account_key \
                        for name_file in self.blob_chunk]
        
def download_obis_within_polygon(blob, polygon):
    gdf_list = []
    for i, sas_url in tqdm(enumerate(blob.sas_url_list), total=len(blob.sas_url_list)):
        gdf = dask_geopandas.read_parquet(sas_url).compute()
        mask = gdf.within(polygon)
        gdf_within_polygon = gdf[mask.values]
        gdf_within_polygon.reset_index(drop=True, inplace=True)
        gdf_list.append(gdf_within_polygon)
    gdf = pd.concat(gdf_list)
    gdf.reset_index(drop=True, inplace=True)
    return gdf