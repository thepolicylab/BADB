# Include Census information to output from 10_Secondary_Address.py
import pandas as pd
from pathlib import Path
import geopandas as gpd

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
DATA_DIR = ROOT_DIR / Path('data')
TOTAL_OUTPUT_FILE = DATA_DIR / Path('10_ss_total.csv.gz')
RI_CRS = 'epsg:6568'
## OUTPUT ##
CENSUS_JOINED_OUTPUT = DATA_DIR / Path('20_ss_census.csv.gz')

df = pd.read_csv(TOTAL_OUTPUT_FILE,
                 compression='gzip')
col_names = list(df.columns) + ['COUNTYFP10', 'TRACTCE10', 'BLKGRPCE10']
geo_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
geo_df.crs = RI_CRS
cbg_shp = geoutils.get_shp_file('RI', 'blockgroup')
cbg_shp.crs = RI_CRS

joined_df = gpd.sjoin(geo_df, cbg_shp, op='within')[col_names]
joined_df.to_csv(CENSUS_JOINED_OUTPUT, compression='gzip', index=False)


