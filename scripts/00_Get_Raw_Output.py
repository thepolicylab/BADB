import concurrent.futures
import pandas as pd

from itertools import repeat
from pathlib import Path, PurePath
from tqdm.auto import tqdm

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
CONFIG_FILE = ROOT_DIR / Path('config.csv')
DATA_DIR = ROOT_DIR / Path('data')
E911_FILE = ROOT_DIR / Path('data') / Path('E-911_Sites.csv.gz')

## OUTPUT ##
RAW_OUTPUT = DATA_DIR / Path('ss_raw.csv.gz')

with open(CONFIG_FILE, 'rt') as infile:
  SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
df = pd.read_csv(E911_FILE, compression='gzip')
df['State'] = 'RI'
ss_input = df[['OBJECTID', 'PrimaryAdd', 'ZN', 'State', 'Zip']]
ss_list = ss_input.values.tolist()

with concurrent.futures.ThreadPoolExecutor() as executor:
  init_df = pd.DataFrame.from_dict(
    list(tqdm(executor.map(geoutils.smarty_api, ss_list,
                           repeat(SS_AUTH_ID),
                           repeat(SS_AUTH_TOKEN),
                           repeat(True)), total=len(ss_list)
              )
         )
  )

# Output raw output
init_df.to_csv(RAW_OUTPUT, compression='gzip')