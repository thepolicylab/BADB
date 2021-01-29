# Expands multi-unit addresses from `00_Get_Raw_Output.py`
# by testing out various secondary address formats Re-run Multiple Units with the SmartyStreets API
from itertools import repeat
import pandas as pd
from pathlib import Path
import string
from tqdm import tqdm
import ujson
import yaml

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR

DATA_DIR = ROOT_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
INTERMEDIATE_DATA_DIR = DATA_DIR / 'intermediate'
CONFIG_FILE = ROOT_DIR / 'creds.yml'

FIXED_OUTPUT = INTERMEDIATE_DATA_DIR / '01_ss_fixed.csv.gz'

## OUTPUT ##
TOTAL_OUTPUT_FILE = INTERMEDIATE_DATA_DIR / '20_ss_total.csv.gz'

df = pd.read_csv(FIXED_OUTPUT, compression='gzip')
temp = pd.json_normalize(
  df.output.apply(ujson.loads))
df.drop(['output', 'zipcode'], axis=1, inplace=True) # zipcode is dropped because of overlap
init_df = pd.concat([df, temp], axis=1)

single_units = init_df[(init_df.dpv_match_code == 'Y')].reset_index(drop=True)
multi_units = init_df[(init_df.dpv_match_code == 'S') |
                      (init_df.dpv_match_code == 'D')].reset_index(drop=True)

if multi_units.empty:
  single_units.to_csv(TOTAL_OUTPUT_FILE, compression='gzip', index=False)

if not multi_units.empty:
  # Create a list of possible permutations.
  # Most apartment rooms are either just numeric, just alpha, or a permutation of the two.
  num = [1, 11, 101, 1001]
  alpha = ['A'] # just 'A', realizing additional alphabets dont necessarily help
  perm_list = geoutils.create_perm(num, alpha, separate=False)
  perm_list = sorted(perm_list, key=len, reverse=True)

  creds = yaml.load(open(CONFIG_FILE), Loader=yaml.FullLoader)
  SS_AUTH_ID, SS_AUTH_TOKEN = creds['AUTH_ID'], creds['AUTH_TOKEN']

  # mu_rerun is a list of just the addresses for the multi_unit addresses
  ## mu = multi units
  mu_rerun = multi_units[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()
  mu_init = pd.concat(list(tqdm(map(geoutils.joining_permutations, mu_rerun,
                                    repeat(perm_list),
                                    repeat(SS_AUTH_ID),
                                    repeat(SS_AUTH_TOKEN)))))
  mu_init.drop_duplicates(subset=['street', 'city', 'state', 'zipcode', 'secondary'], inplace=True)

  alpha = list(string.ascii_uppercase[:5])
  perm_total = geoutils.create_perm(geoutils.appropriate_nums(range(1001, 1006)), alpha) \
               + geoutils.create_perm(geoutils.appropriate_nums(range(101, 500)), alpha) \
               + geoutils.create_perm(geoutils.appropriate_nums(range(11, 50)), alpha) \
               + geoutils.create_perm(range(1, 6), alpha) + [alpha]
  perm_dict = dict(zip(perm_list, perm_total))

  # Expansive search
  in_sample_key = list(mu_init.secondary.unique())
  in_sample_val = [perm_dict[perm] for perm in in_sample_key]

  total_hacked = geoutils.address_hacking(in_sample_key, in_sample_val, mu_init, SS_AUTH_ID, SS_AUTH_TOKEN)
  temp = pd.json_normalize(total_hacked.output.apply(ujson.loads))
  total_hacked.drop(['output', 'zipcode'], axis=1, inplace=True)

  pd.concat(
    [pd.concat([total_hacked, temp], axis=1), single_units]).to_csv(
      TOTAL_OUTPUT_FILE, compression='gzip', index=False)