# Analyze the outcome from the 00_Get_Raw_Output.py into Single Units and Multiple Units
# Re-run Multiple Units with the SmartyStreets API
from itertools import repeat
import pandas as pd
from pathlib import Path
import string
from tqdm import tqdm
import ujson

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
CONFIG_FILE = ROOT_DIR / Path('config.csv')
DATA_DIR = ROOT_DIR / Path('data')
FIXED_OUTPUT = DATA_DIR / Path('01_ss_fixed.csv.gz')

## OUTPUT ##
SINGLE_UNIT_FILE = DATA_DIR / Path('10_ss_single.csv.gz')
MULTI_UNIT_FILE = DATA_DIR / Path('11_ss_expanded.csv.gz')


df = pd.read_csv(FIXED_OUTPUT, compression='gzip')
temp = pd.json_normalize(
  df.output.apply(ujson.loads))
df.drop(['output', 'zipcode'], axis = 1, inplace=True) # zipcode is dropped because of overlap
init_df = pd.concat([df, temp], axis=1)

single_units = init_df[(init_df.dpv_match_code == 'Y')].reset_index(drop=True)
single_units.to_csv(SINGLE_UNIT_FILE, compression='gzip', index=False)
multi_units = init_df[init_df.dpv_match_code == ('S' or 'D')].reset_index(drop=True)
if not multi_units.empty:
  # Create a list of possible permutations.
  # Most apartment rooms are either just numeric, just alpha, or a permutation of the two.
  num = [1, 11, 101, 1001]
  alpha = ['A'] # just 'A', realizing additional alphabets dont necessarily help
  perm_list = geoutils.create_perm(num, alpha, separate=False)
  perm_list = sorted(perm_list, key=len, reverse=True)

  with open(CONFIG_FILE, 'rt') as infile:
    SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
  # mu_rerun is a list of just the addresses for the multi_unit addresses
  ## mu = multi units
  mu_rerun = multi_units[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()
  mu_init = pd.concat(list(tqdm(map(geoutils.joining_permutations, mu_rerun,
                                    repeat(perm_list),
                                    repeat(SS_AUTH_ID),
                                    repeat(SS_AUTH_TOKEN)))))

  alpha = list(string.ascii_uppercase[:15])
  perm_total = geoutils.create_perm(geoutils.appropriate_nums(range(1001, 10000)), alpha) \
               + geoutils.create_perm(geoutils.appropriate_nums(range(101, 1000)), alpha) \
               + geoutils.create_perm(geoutils.appropriate_nums(range(11, 100)), alpha) \
               + geoutils.create_perm(range(1, 10), alpha) + [alpha]
  perm_dict = dict(zip(perm_list, perm_total))

  # Expansive search
  in_sample_key = list(mu_init.secondary.unique())
  in_sample_val = [perm_dict[perm] for perm in in_sample_key]
  total_hacked = geoutils.address_hacking(in_sample_key, in_sample_val, mu_init, SS_AUTH_ID, SS_AUTH_TOKEN)

  total_hacked.to_csv(MULTI_UNIT_FILE, compression='gzip', index=False)