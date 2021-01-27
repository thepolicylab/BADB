# Run all addresses in the E911 Dataset and validates input addresses with the SmartyStreets API
import concurrent.futures
import pandas as pd
import yaml

from itertools import repeat
from pathlib import Path
from tqdm.auto import tqdm

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
CONFIG_FILE = ROOT_DIR / 'creds.yml'
DATA_DIR = ROOT_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
E911_FILE = RAW_DATA_DIR / 'E-911_Sites.csv.gz'
STATE = 'RI'
INTERMEDIATE_DATA_DIR = DATA_DIR / 'intermediate'

## OUTPUT ##
RAW_OUTPUT = INTERMEDIATE_DATA_DIR / '00_ss_raw.csv.gz'


# with open(CONFIG_FILE, 'rt') as infile:
#   SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')

creds = yaml.load(open(CONFIG_FILE), Loader=yaml.FullLoader)
SS_AUTH_ID, SS_AUTH_TOKEN = creds['AUTH_ID'], creds['AUTH_TOKEN']

try:
  df = pd.read_csv(E911_FILE)
except:
  print('wrong file')
  exit()
df['State'] = STATE
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
init_df.to_csv(RAW_OUTPUT, compression='gzip', index=False)
