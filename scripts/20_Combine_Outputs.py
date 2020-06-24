# Combine the outputs of 10_Secondary_Addresses.py into a master file of geocoded addresses
import pandas as pd
import ujson
from pathlib import Path

from badb import data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
DATA_DIR = ROOT_DIR / Path('data')

# OUTPUT ##
TOTAL_OUTPUT_FILE = DATA_DIR / Path('20_ss_total.csv.gz')


# The verified single units
single_unit = pd.read_csv(SINGLE_UNIT_FILE,
                          compression='gzip',
                          index_col=0)
# The verified multi units
df = pd.read_csv(MULTI_UNIT_FILE,
                 compression='gzip',
                 index_col=0)
temp = pd.json_normalize(
  df.output.apply(ujson.loads))
df.drop(['output', 'zipcode'], axis=1, inplace=True)
multi_unit = pd.concat([df, temp], axis=1)

pd.concat([single_unit, multi_unit])\
  .to_csv(TOTAL_OUTPUT_FILE, compression='gzip', index=False)