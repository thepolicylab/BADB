import boto3
import concurrent.futures
import itertools
import string
import pandas as pd
from tqdm import tqdm

from . import ss_function


def main(*args, **kwargs):
  # read in the access keys for smarty streets
  with open('config.csv', 'rt') as infile:
    SS_AUTH_ID, SS_AUTH_TOKEN = infile.read().strip().split(',')

  print("opening e911 data")
  df = pd.read_csv('E-911_Sites.csv.gz', compression='gzip').head(300)
  df['State'] = 'RI'
  ss_input = df[['OBJECTID', 'PrimaryAdd', 'ZN', 'State', 'Zip']]
  ss_list = ss_input.values.tolist()

  with concurrent.futures.ThreadPoolExecutor() as executor:
    # noinspection PyTypeChecker
    init_df = pd.DataFrame.from_dict(
      list(tqdm(executor.map(ss_function.smarty_api, ss_list,
                             itertools.repeat(SS_AUTH_ID),
                             itertools.repeat(SS_AUTH_TOKEN),
                             itertools.repeat(True)), total=len(ss_list))))

  single_units = init_df[init_df.match == 'Y'].reset_index(drop=True)
  invalid_add = init_df[init_df.match.isnull()].reset_index(drop=True)
  multi_units = init_df[(init_df.match == 'S') | (init_df.match == 'D')].reset_index(drop=True)

  print('creating test permutations')
  num = [1, 11, 101, 1001]
  alpha = ['A']
  temp = list(itertools.product(num, alpha))
  num_first = [f'{x}{y}' for (x, y) in temp]
  alpha_first = [f'{y}{x}' for (x, y) in temp]
  perm_list = num + alpha + num_first + alpha_first

  # mu_rerun is a list of just the addresses for the multi_unit addresses
  ## mu = multi units
  mu_rerun = multi_units[['object_id', 'street', 'city', 'state', 'zipcode']].values.tolist()
  mu_init = pd.concat(list(tqdm(map(ss_function.joining_permutations,
                                    mu_rerun,
                                    itertools.repeat(perm_list),
                                    itertools.repeat(SS_AUTH_ID),
                                    itertools.repeat(SS_AUTH_TOKEN)
                                    ), total=len(mu_rerun))))

  print('creating full permutations')
  num_list = list(range(1, 10))
  alpha = list(string.ascii_uppercase)[0:15] ## I've never seen an apartment suite beyond 'O'
  # Cross product between num_list and alpha
  temp = list(itertools.product(num_list, alpha))
  num_first = [f'{x}{y}' for (x, y) in temp]
  alpha_first = [f'{y}{x}' for (x, y) in temp]
  # Cross product between num_list2 and alpha
  num_list2 = ss_function.appropriate_nums(range(11, 100))
  temp = list(itertools.product(num_list2, alpha))
  num_first2 = [f'{x}{y}' for (x, y) in temp]
  alpha_first2 = [f'{y}{x}' for (x, y) in temp]
  # Cross product between num_list3 and alpha
  num_list3 = ss_function.appropriate_nums(range(101, 1000))
  temp = list(itertools.product(num_list3, alpha))
  num_first3 = [f'{x}{y}' for (x, y) in temp]
  alpha_first3 = [f'{y}{x}' for (x, y) in temp]
  # Cross product between num_list4 and alpha
  num_list4 = ss_function.appropriate_nums(range(1001, 10000))
  temp = list(itertools.product(num_list4, alpha))
  num_first4 = [f'{x}{y}' for (x, y) in temp]
  alpha_first4 = [f'{y}{x}' for (x, y) in temp]

  samp_total = [num_list, num_list2, num_list3, num_list4, alpha, num_first, num_first2, num_first3, num_first4,
                alpha_first, alpha_first2, alpha_first3, alpha_first4]
  perm_dict = dict(zip(perm_list, samp_total))

  # apply hacking to relevant subset of test outcomes
  in_sample_key = list(mu_init.secondary.unique())
  in_sample_val = [perm_dict[perm] for perm in in_sample_key]
  total_hacked = ss_function.address_hacking(in_sample_key, in_sample_val, mu_init, SS_AUTH_ID, SS_AUTH_TOKEN)
  total_hacked.delivery = list(map(ss_function.sec_add, total_hacked.delivery))
  single_units.delivery = "None"
  invalid_add.delivery = "None"
  final_df = pd.concat([total_hacked, single_units, invalid_add]).reset_index(drop=True)
  final_df.rename(columns={'delivery': 'prefix',
                           'object_id': 'OBJECTID'}, inplace=True)

  # merge final_df with df
  badb = final_df[['OBJECTID', 'prefix', 'secondary',
                   'street', 'city', 'state', 'zipcode',
                   'type', 'rdi', 'match', 'active',
                   'vacant', 'latitude', 'longitude',
                   'timezone']].merge(df, on=['OBJECTID'], how='left')
  badb

  # upload to AWS
  s3 = boto3.client('s3')

if __name__ == '__main__':
  main()
