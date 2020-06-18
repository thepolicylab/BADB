# Analyze the outcome from the 00_Get_Raw_Output.py into Single Units and Multiple Units
# Re-run Multiple Units with the SmartyStreets API
import pandas as pd
from tqdm import tqdm
from itertools import repeat, product
import ujson
from pathlib import Path

from badb import geoutils, data_utils

## INPUT ##
ROOT_DIR = data_utils.ROOT_DIR
CONFIG_FILE = ROOT_DIR / Path('config.csv')
DATA_DIR = ROOT_DIR / Path('data')
E911_FILE = ROOT_DIR / Path('data') / Path('E-911_Sites.csv.gz')
RAW_OUTPUT = DATA_DIR / Path('ss_raw.csv.gz')

## OUTPUT ##
SINGLE_UNIT_FILE = DATA_DIR / Path('ss_single_raw.csv.gz')
MULTI_UNIT_FILE = DATA_DIR / Path('ss_expanded_raw.csv.gz')


df = pd.read_csv(RAW_OUTPUT, compression='gzip', index_col=0)
na_df = df[df.output.isna()].reset_index(drop = True)
res_df = df[df.output.notna()].reset_index(drop=True)
temp = pd.json_normalize(
  res_df.output.apply(ujson.loads))
res_df.drop(['output', 'zipcode'], axis = 1, inplace=True)
init_df = pd.concat([res_df, temp], axis=1)

single_units = init_df[(init_df.dpv_match_code == 'Y')].reset_index(drop=True)
single_units.to_csv(SINGLE_UNIT_FILE, compression='gzip')
multi_units = init_df[init_df.dpv_match_code == ('S' or 'D')].reset_index(drop=True)


# Create a list of possible permutations.
# Most apartment rooms are either just numeric, just alpha, or a permutation of the two.
num = [1, 11, 101, 1001]
alpha = ['A'] # just 'A', realizing additional alphabets dont necessarily help
[num_first, alpha_first, perm_list] = geoutils.create_perm(num, alpha)

with open(CONFIG_FILE, 'rt') as infile:
  SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
# mu_rerun is a list of just the addresses for the multi_unit addresses
## mu = multi units
mu_rerun = multi_units[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()
mu_init = pd.concat(list(tqdm(map(geoutils.joining_permutations, mu_rerun,
                                  repeat(perm_list),
                                  repeat(SS_AUTH_ID),
                                  repeat(SS_AUTH_TOKEN)))))

perm_total = list(
        set(
            geoutils.create_perm(range(1, 10), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(11, 100)), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(101, 1000)), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(1001, 10000)), alpha)
        )
    )
perm_dict = dict(zip(perm_list, perm_total))

# Expansive search
in_sample_key = list(mu_init.secondary.unique())
in_sample_val = [perm_dict[perm] for perm in in_sample_key]
total_hacked = geoutils.address_hacking(in_sample_key, in_sample_val, mu_init, SS_AUTH_ID, SS_AUTH_TOKEN)

total_hacked.to_csv(MULTI_UNIT_FILE, compression='gzip')