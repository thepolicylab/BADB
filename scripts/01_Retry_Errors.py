# Rerun residential addresses that were not successful in initial run due to errors in zipcode
import pandas as pd
import yaml

from itertools import repeat
from pathlib import Path
from tqdm.auto import tqdm

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR

DATA_DIR = ROOT_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
INTERMEDIATE_DATA_DIR = DATA_DIR / 'intermediate'
CONFIG_FILE = ROOT_DIR / 'creds.yml'

RAW_OUTPUT = INTERMEDIATE_DATA_DIR / '00_ss_raw.csv.gz'

## OUTPUT ##
FIXED_OUTPUT = INTERMEDIATE_DATA_DIR / '01_ss_fixed.csv.gz'


creds = yaml.load(open(CONFIG_FILE), Loader=yaml.FullLoader)
SS_AUTH_ID, SS_AUTH_TOKEN = creds['AUTH_ID'], creds['AUTH_TOKEN']

df = pd.read_csv(RAW_OUTPUT, compression='gzip')
zip_list = list(geoutils.get_shp_file('RI').ZCTA5CE10)

# redo_df = df[df.output.isna()]
redo_df = pd.DataFrame()
if not redo_df.empty:
  redo_df.city = None

  ss_input = redo_df[['OBJECTID', 'street', 'city', 'state']].values.tolist()
  zip_hacked = pd.concat(list(tqdm(map(geoutils.joining_permutations, ss_input,
                                    repeat(zip_list),
                                    repeat(SS_AUTH_ID),
                                    repeat(SS_AUTH_TOKEN),
                                    repeat(True)))))
  zip_hacked.drop_duplicates(subset=['OBJECTID', 'street', 'city', 'state', 'zipcode'], inplace=True)
  # zip_hacked = None
  # Combine zip_hacked with Output that did not need redo, and output
  df[df.output.notna()].append(zip_hacked).reset_index(drop=True)\
    .to_csv(FIXED_OUTPUT, compression='gzip', index=False)
else:
  df[df.output.notna()].reset_index(drop=True) \
    .to_csv(FIXED_OUTPUT, compression='gzip', index=False)