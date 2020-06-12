# Combine the outputs of 10_Secondary_Addresses.py into a master file of geocoded addresses
import pandas as pd
import ujson
from pathlib import Path

## INPUT

DATA_DIR = Path('data')
SU_DIR = Path('ss_single_raw.csv.gz')
# The verified single units
single_unit = pd.read_csv(DATA_DIR / SU_DIR,
                          compression='gzip',
                          index_col=0)
# The verified multi units
MU_DIR = Path('ss_expanded_raw.csv.gz')
df = pd.read_csv(DATA_DIR / MU_DIR,
                         compression='gzip',
                         index_col=0)
temp = pd.json_normalize(
  df.output.apply(ujson.loads))
df.drop(['output', 'zipcode'], axis=1, inplace=True)
multi_unit = pd.concat([df, temp], axis=1)
multi_unit
