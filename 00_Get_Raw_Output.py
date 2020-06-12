import concurrent.futures
import pandas as pd

from itertools import repeat
from pathlib import Path
from tqdm.auto import tqdm

import ss_function

## INPUT
CONFIG_FILE = 'config.csv'
DATA_DIR = Path('data')
E911_FILE = Path('E-911_Sites.csv.gz')

## OUTPUT
RAW_OUTPUT = Path('ss_raw.csv.gz')

with open(CONFIG_FILE, 'rt') as infile:
  SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
df = pd.read_csv(DATA_DIR / E911_FILE, compression='gzip')
df['State'] = 'RI'
ss_input = df[['OBJECTID', 'PrimaryAdd', 'ZN', 'State', 'Zip']]
ss_list = ss_input.values.tolist()

with concurrent.futures.ThreadPoolExecutor() as executor:
  init_df = pd.DataFrame.from_dict(
    list(tqdm(executor.map(ss_function.smarty_api, ss_list,
                           repeat(SS_AUTH_ID),
                           repeat(SS_AUTH_TOKEN),
                           repeat(True)), total=len(ss_list)
              )
         )
  )

init_df.to_csv(DATA_DIR / RAW_OUTPUT, compression='gzip')
