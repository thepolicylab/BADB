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
df.drop(['candidate_index', 'addressee', 'delivery_line_1', 'delivery_line_2', 'input_id',
       'last_line', 'delivery_point_barcode', 'record_type', 'zip_type',
         'county_fips', 'county_name', 'carrier_route', 'elot_sequence', 'elot_sort',
         'precision', 'time_zone', 'utc_offset', 'obeys_dst', 'is_ews_match', 'dpv_footnotes',
         'cmra', 'footnotes', 'lacs_link_code', 'lacs_link_indicator', 'is_suite_link_match',
         'urbanization', 'primary_number', 'street_name', 'street_predirection',
       'street_postdirection', 'street_suffix', 'secondary_number', 'extra_secondary_number',
       'extra_secondary_designator', 'pmb_designator', 'pmb_number',
       'city_name', 'default_city_name', 'state_abbreviation', 'plus4_code', 'delivery_point',
         'delivery_point_check_digit'], axis=1, inplace=True)
col_names = list(df.columns) + ['COUNTYFP10', 'TRACTCE10', 'BLKGRPCE10']
geo_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
geo_df.crs = RI_CRS
cbg_shp = geoutils.get_shp_file('RI', 'blockgroup')
cbg_shp.crs = RI_CRS

joined_df = gpd.sjoin(geo_df, cbg_shp, op='within')[col_names]
joined_df.to_csv(CENSUS_JOINED_OUTPUT, compression='gzip', index=False)


