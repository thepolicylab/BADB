import concurrent.futures
from itertools import product, repeat
import pandas as pd

from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup
from tqdm import tqdm
from typing import List, Dict

from badb import data_utils

def smarty_api(
    row: List[str],
    SS_AUTH_ID,
    SS_AUTH_TOKEN,
    primary: bool = False
) -> Dict[str, str]:
  """
  Run addresses through SmartyStreets API with all input addresses.
  Used to identify whether a location is valid single-unit / invalid / valid multi-unit
  Args:
      row: row containing address information
      ex) ['250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905]
      primary: indicates whether the smarty_api is run for primary addresses (True),
      or for secondary addresses (False)
  Returns:
      Dictionary of relevant components from SmartyStreets database
      ex) [street: '250 OCONNOR ST', city: 'PROVIDENCE', state: 'RI',
      zipcode: 2905, type: 'S', rdi: 'Residential', match: 'Y', active: 'Y', match: 'Y']
  """
  # Authenticate to SmartyStreets API
  credentials = StaticCredentials(SS_AUTH_ID, SS_AUTH_TOKEN)
  client = ClientBuilder(credentials).build_us_street_api_client()
  # Lookup the Address with inputs by indexing from input `row`
  lookup = StreetLookup()
  if primary:
    [lookup.input_id, lookup.street, lookup.city, lookup.state, lookup.zipcode] = row
  else:
    [lookup.secondary, lookup.input_id, lookup.street, lookup.city, lookup.state, lookup.zipcode] = row
  lookup.candidates = 1
  lookup.match = "Invalid"  # "invalid" always returns at least one match

  default_output = {
    'OBJECTID': lookup.input_id,
    'street': lookup.street,
    'city': lookup.city,
    'state': lookup.state,
    'zipcode': lookup.zipcode,
    'output': None
  } if primary \
    else {
    'OBJECTID': lookup.input_id,
    'secondary': lookup.secondary,
    'street': lookup.street,
    'city': lookup.city,
    'state': lookup.state,
    'zipcode': lookup.zipcode,
    'output': None
  }
  # make the secondary address itself a NaN.
  # Then, you can just streamline this into a single lookup
  try:
    client.send_lookup(lookup)
  except exceptions.SmartyException as err:
    # if we have exceptions, just return the inputs to retry later
    return default_output

  res = lookup.result
  if not res:
    # if we have exceptions, just return the inputs to retry later
    return default_output

  # res: List[List(str)], so index first element (lookup.candidates : 1)
  result = res[0]
  return {
    'OBJECTID': lookup.input_id,
    'street': lookup.street,
    'city': lookup.city,
    'state': lookup.state,
    'zipcode': lookup.zipcode,
    'output': data_utils.collect_dict(result)
  } if primary \
  else {
    'OBJECTID': lookup.input_id,
    'secondary': lookup.secondary,
    'street': lookup.street,
    'city': lookup.city,
    'state': lookup.state,
    'zipcode': lookup.zipcode,
    'output': data_utils.collect_dict(result)
    }

def joining_permutations(
    row: List[str],
    perm_list: List[str],
    SS_AUTH_ID: str,
    SS_AUTH_TOKEN: str
) -> pd.DataFrame:
    """
    A function that adds all permutations of secondary addresses to a given address.
    Args:
        row: row containing address information
        ex) [250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905]
        perm_list: the list of secondary addresses to permute
        ex) [1, 2, 3, 4]
    Returns:
        a DataFrame of results from SmartyStreets API with inputs of
        [1, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905],
        [2, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905], etc
    """
    secondary_included = [[perm] + row for perm in perm_list]
    # Apply multithreading to accelerate the search
    with concurrent.futures.ThreadPoolExecutor() as executor:
      all_outputs = pd.DataFrame(tqdm(list(executor.map(smarty_api,
                                                   secondary_included,
                                                   repeat(SS_AUTH_ID),
                                                   repeat(SS_AUTH_TOKEN)
                                                   ))))
      # only return the permutations that returned valid entry
      return all_outputs[all_outputs.output.notna()]

def appropriate_nums(num_list):
  """
  A function that ensures that the second digit being checked is less than 40,
  given that most apartments do not have more than 40 rooms per floor
  ex) [100, 101, 102, .... 198, 199] -> [100, 101, ..., 138, 139]
  """
  return [x for x in num_list if x % 100 < 40]

def df_prep(match_cond, df: pd.DataFrame) -> pd.DataFrame:
  """
  Prepares dataframe for matching by subsetting rows that match the appropriate condition
  Args:
      match_cond: conditions to filter dataframe
      ex) 'A1'
      df: Initial results of secondary dataframe
  Returns:
      A DataFrame of address with matching test conditions
  """
  match_df = df[df.secondary == match_cond]
  # Only return address columns, and into a list form
  return match_df[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()

def joining_permutations_runner(input_list,
                                SS_AUTH_ID: str,
                                SS_AUTH_TOKEN: str):
  return pd.concat(
    list(tqdm(
      map(joining_permutations,
          input_list[0],
          repeat(input_list[1]),
          repeat(SS_AUTH_ID),
          repeat(SS_AUTH_TOKEN)
          ), total=len(input_list))))


def address_hacking(test_cond: List[str],
                    perm_list: List[str],
                    mu_init: pd.DataFrame,
                    SS_AUTH_ID: str,
                    SS_AUTH_TOKEN: str
                    ) -> pd.DataFrame:
  """
  Args:
      test_cond: the list of match_cond as inputs for df_prep
      ex) [1, 11, 101]
      perm_list: the list of permutations as inputs for joining_permutations
      ex) [num_list, num_list2, num_list3]
  Returns:
      a DataFrame of results from SmartyStreets API with inputs of
      [1, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905],
      [2, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905], etc
  """
  temp = list(map(df_prep, test_cond, repeat(mu_init)))
  input_list = list(zip(temp, perm_list))
  output_list = list(map(joining_permutations_runner, input_list,
                         repeat(SS_AUTH_ID),
                         repeat(SS_AUTH_TOKEN)))
  df = pd.concat(output_list)
  df.reset_index(drop=True, inplace=True)
  return df

def create_perm(num, alpha):
  temp = list(product(num, alpha))
  num_first = [f'{x}{y}' for (x, y) in temp]
  alpha_first = [f'{y}{x}' for (x, y) in temp]
  perm_list = num + alpha + num_first + alpha_first
  return [num_first, alpha_first, perm_list]

# Customize the permutation list for address_hacking
num_list = list(range(1,10))
alpha = list(string.ascii_uppercase)[0:15] ## I've never seen an apartment suite beyond 'O'
[num_first, alpha_first, _] = create_perm(num_list, alpha)

# Cross product between num_list2 and alpha
num_list2 = appropriate_nums(range(11, 100))
[num_first2, alpha_first2, _] = create_perm(num_list2, alpha)
# Cross product between num_list3 and alpha
num_list3 = appropriate_nums(range(101, 1000))
[num_first3, alpha_first3, _] = create_perm(num_list3, alpha)

# Cross product between num_list4 and alpha
num_list4 = appropriate_nums(range(1001, 10000))
[num_first4, alpha_first4, _] = create_perm(num_list4, alpha)

perm_total = [num_list, num_list2, num_list3,
              num_list4, alpha, num_first, num_first2,
              num_first3, num_first4, alpha_first, alpha_first2,
              alpha_first3, alpha_first4]