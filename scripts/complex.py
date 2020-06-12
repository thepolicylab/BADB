import pandas as pd
import string
from tqdm import tqdm
from itertools import repeat, product
import json
from pathlib import Path

from badb import geoutils

DATA_DIR = Path('data')
FILE_DIR = Path('ss_raw.csv.gz')
df = pd.read_csv(DATA_DIR / FILE_DIR, compression='gzip', index_col=0)
na_df = df[df.output.isna()].reset_index(drop = True)
res_df = df[df.output.notna()].reset_index(drop=True)
temp = pd.json_normalize(
  res_df.output.apply(json.loads))
res_df.drop(['output', 'zipcode'], axis = 1, inplace=True)
init_df = pd.concat([res_df, temp], axis=1)

single_units = init_df[(init_df.dpv_match_code == 'Y')].reset_index(drop=True)
SU_DIR = Path('ss_single_raw.csv.gz')
single_units.to_csv(DATA_DIR / SU_DIR, compression='gzip')
multi_units = init_df[init_df.dpv_match_code == ('S' or 'D')].reset_index(drop=True)


# Create a list of possible permutations.
# Most apartment rooms are either just numeric, just alpha, or a permutation of the two.
num = [1, 11, 101, 1001]
alpha = ['A'] # just 'A', realizing additional alphabets dont necessarily help
[num_first, alpha_first, perm_list] = geoutils.create_perm(num, alpha)

with open('config.csv', 'rt') as infile:
  SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')
# mu_rerun is a list of just the addresses for the multi_unit addresses
## mu = multi units
mu_rerun = multi_units[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()
mu_init = pd.concat(list(tqdm(map(geoutils.joining_permutations, mu_rerun,
                                  repeat(perm_list),
                                  repeat(SS_AUTH_ID),
                                  repeat(SS_AUTH_TOKEN)))))

# Customize the permutation list
num_list = list(range(1,10))
alpha = list(string.ascii_uppercase)[0:15] ## I've never seen an apartment suite beyond 'O'

# Cross product between num_list and alpha
temp = list(product(num_list, alpha))
num_first = [f'{x}{y}' for (x,y) in temp]
alpha_first = [f'{y}{x}' for (x,y) in temp]
# Cross product between num_list2 and alpha
num_list2 = geoutils.appropriate_nums(range(11, 100))
temp = list(product(num_list2, alpha))
num_first2 = [f'{x}{y}' for (x,y) in temp]
alpha_first2 = [f'{y}{x}' for (x,y) in temp]
# Cross product between num_list3 and alpha
num_list3 = geoutils.appropriate_nums(range(101, 1000))
temp = list(product(num_list3, alpha))
num_first3 = [f'{x}{y}' for (x,y) in temp]
alpha_first3 = [f'{y}{x}' for (x,y) in temp]
# Cross product between num_list4 and alpha
num_list4 = geoutils.appropriate_nums(range(1001, 10000))
temp = list(product(num_list4, alpha))
num_first4 = [f'{x}{y}' for (x,y) in temp]
alpha_first4 = [f'{y}{x}' for (x,y) in temp]

samp_total = [num_list, num_list2, num_list3, num_list4, alpha, num_first, num_first2, num_first3, num_first4, alpha_first, alpha_first2, alpha_first3, alpha_first4]
perm_dict = dict(zip(perm_list, samp_total))

# Expansive search
in_sample_key = list(mu_init.secondary.unique())
in_sample_val = [perm_dict[perm] for perm in in_sample_key]
total_hacked = geoutils.address_hacking(in_sample_key, in_sample_val, mu_init, SS_AUTH_ID, SS_AUTH_TOKEN)

SAVE_DIR = Path('ss_expanded_raw.csv.gz')
total_hacked.to_csv(DATA_DIR / SAVE_DIR, compression='gzip')