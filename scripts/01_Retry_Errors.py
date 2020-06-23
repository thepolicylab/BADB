# Rerun residential addresses that were not successful in initial run due to errors in zipcode
import pandas as pd

from itertools import repeat
from pathlib import Path
from tqdm.auto import tqdm

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
CONFIG_FILE = ROOT_DIR / Path('config.csv')
DATA_DIR = ROOT_DIR / Path('data')
RAW_OUTPUT = DATA_DIR / Path('00_ss_raw.csv.gz')

## OUTPUT ##
FIXED_OUTPUT = DATA_DIR / Path('01_ss_fixed.csv.gz')


with open(CONFIG_FILE, 'rt') as infile:
  SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
df = pd.read_csv(RAW_OUTPUT, compression='gzip')
zip_list = list(geoutils.get_shp_file('RI').ZCTA5CE10)

redo_df = df[df.output.isna()]
redo_df.city = None

ss_input = redo_df[['OBJECTID', 'street', 'city', 'state']].values.tolist()
zip_hacked = pd.concat(list(tqdm(map(geoutils.joining_permutations, ss_input,
                                  repeat(zip_list),
                                  repeat(SS_AUTH_ID),
                                  repeat(SS_AUTH_TOKEN),
                                  repeat(True)))))
zip_hacked.drop_duplicates(subset=['OBJECTID', 'street', 'city', 'state', 'zipcode'], inplace=True)
# Combine zip_hacked with Output that did not need redo, and output
df[df.output.notna()].append(zip_hacked).reset_index(drop=True)\
  .to_csv(FIXED_OUTPUT, compression='gzip', index=False)