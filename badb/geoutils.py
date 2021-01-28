import concurrent.futures
from itertools import product, repeat
from typing import List, Dict, Iterable, Union

import geopandas as gpd
import pandas as pd
import requests
import tempfile
import ujson

from pathlib import Path
from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup
from tqdm import tqdm
from us import states

from . import data_utils

def smarty_api(
    row: List[str], SS_AUTH_ID: str, SS_AUTH_TOKEN: str, primary: bool = False
) -> Dict[str, str]:
    """
    Run addresses through SmartyStreets API with all input addresses.
    Used to identify whether a location is valid single-unit / invalid / valid multi-unit

    Args:
        row: row containing address information
            ex) ['someid', '250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905]
        primary: indicates whether the smarty_api is run for primary addresses (True),
            or for secondary addresses (False)

    Returns:
        Dictionary of relevant components from SmartyStreets database
            ex) {
                street: '250 OCONNOR ST',
                city: 'PROVIDENCE',
                state: 'RI',
                zipcode: 2905,
                type: 'S',
                rdi: 'Residential',
                match: 'Y',
                active: 'Y',
                match: 'Y'
            }
    """
    # Authenticate to SmartyStreets API
    credentials = StaticCredentials(SS_AUTH_ID, SS_AUTH_TOKEN)
    client = ClientBuilder(credentials).build_us_street_api_client()
    # Lookup the Address with inputs by indexing from input `row`
    lookup = StreetLookup()
    if primary:
        lookup.input_id, lookup.street, lookup.city, lookup.state, lookup.zipcode = row
    else:
        (
            lookup.secondary,
            lookup.input_id,
            lookup.street,
            lookup.city,
            lookup.state,
            lookup.zipcode,
        ) = row
    lookup.candidates = 1
    lookup.match = "Invalid"  # "invalid" always returns at least one match

    default_output = {
        "OBJECTID": lookup.input_id,
        "street": lookup.street,
        "city": lookup.city,
        "state": lookup.state,
        "zipcode": lookup.zipcode,
        "output": None,
    }
    if not primary:
        default_output["secondary"] = lookup.secondary

    # make the secondary address itself a NaN.
    # Then, you can just streamline this into a single lookup
    try:
        client.send_lookup(lookup)
    except exceptions.SmartyException:
        # if we have exceptions, just return the inputs to retry later
        return default_output

    res = lookup.result
    if not res:
        # if we have exceptions, just return the inputs to retry later
        return default_output

    # res: List[List(str)], so index first element (lookup.candidates : 1)
    result = res[0]
    output = {
        "OBJECTID": lookup.input_id,
        "street": lookup.street,
        "city": lookup.city,
        "state": lookup.state,
        "zipcode": lookup.zipcode,
        "output": data_utils.collect_dict(result),
    }
    if not primary:
        output["secondary"] = lookup.secondary
    return output


def joining_permutations(
    row: List[str], perm_list: List[str], SS_AUTH_ID: str, SS_AUTH_TOKEN: str, zipcode = False
) -> pd.DataFrame:
    """
    A function that adds all permutations of secondary addresses to a given address.

    Args:
        row: row containing address information
            ex) ['some_id', '250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905]
        perm_list: the list of secondary addresses to permute
            ex) [1, 2, 3, 4]

    Returns:
        a DataFrame of results from SmartyStreets API with inputs of
        [1, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905],
        [2, 250 OCONNOR ST', 'PROVIDENCE', 'RI', 2905], etc
    """
    secondary_included = [[perm] + row for perm in perm_list]
    if zipcode:
        secondary_included = [row + [perm] for perm in perm_list]
    # Apply multithreading to accelerate the search
    with concurrent.futures.ThreadPoolExecutor() as executor:
        all_outputs = pd.DataFrame(
            list(
                executor.map(
                    smarty_api,
                    secondary_included,
                    repeat(SS_AUTH_ID),
                    repeat(SS_AUTH_TOKEN),
                    repeat(zipcode)
                )
            )
        )

        # only return the permutations that returned valid entry
        notna_output = all_outputs[all_outputs.output.notna()].reset_index(drop=True)
        if not notna_output.empty:
            temp = pd.json_normalize(
                notna_output.output.apply(ujson.loads))
            dup_vals = [not dup_val for dup_val in temp.zipcode.duplicated()]
            # remove duplicate matches
            if len(temp.dpv_match_code.unique()) != 1:
                return notna_output[temp.dpv_match_code == 'Y']
            else:
                return notna_output[dup_vals]
        return None


def appropriate_nums(num_list: Iterable[int]) -> List[int]:
    """
    A function that ensures that the second digit being checked is less than 40,
    given that most apartments do not have more than 40 rooms per floor
        ex) [100, 101, 102, .... 198, 199] -> [100, 101, ..., 138, 139]
    """
    return [x for x in num_list if x % 100 < 10]


def df_prep(match_cond: Union[str, int], df: pd.DataFrame) -> pd.DataFrame:
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
    return match_df[["OBJECTID", "street", "city", "state", "zipcode"]].values.tolist()


def joining_permutations_runner(input_list, SS_AUTH_ID: str, SS_AUTH_TOKEN: str):
    return pd.concat(
        list(
            tqdm(
                map(
                    joining_permutations,
                    input_list[0],
                    repeat(input_list[1]),
                    repeat(SS_AUTH_ID),
                    repeat(SS_AUTH_TOKEN),
                ),
                total=len(input_list),
            )
        )
    )


def address_hacking(
    test_cond: List[str],
    perm_list: List[str],
    mu_init: pd.DataFrame,
    SS_AUTH_ID: str,
    SS_AUTH_TOKEN: str,
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
    output_list = list(
        map(
            joining_permutations_runner,
            input_list,
            repeat(SS_AUTH_ID),
            repeat(SS_AUTH_TOKEN),
        )
    )
    df = pd.concat(output_list)
    df.reset_index(drop=True, inplace=True)
    return df


def create_perm(num: List[int], alpha: List[str], separate = True) -> List[str]:
    """
    Create tested permuations from lists and numbers and letters.
    Specifically, for all numbers and letters passed, write potential
    secondary addresses as, e.g., 10A, A10, 10, and A.

    Args:
        num: A list of numbers to try
        alpha: A list of letters to try

    Returns:
        A list of alphanumeric permutations to try
    """
    num = list(map(str, num))
    temp = list(product(num, alpha))
    num_first = [f"{x}{y}" for (x, y) in temp]
    alpha_first = [f"{y}{x}" for (x, y) in temp]
    if separate:
        return [num_first, alpha_first, num]
    else:
        return num + alpha + num_first + alpha_first

def get_shp_file(
        input_state: str,
        geography: str = 'zcta'
) -> gpd.GeoDataFrame:
  """
  Retrieves the zipcode shapefile using US python package

  Args:
    input_state: Name of state to look up
         For example "MA"
    geography: determines which shapefile to download

  Returns:
    The geo dataframe for the given state at the given geography level
  """
  url = states.lookup(input_state).shapefile_urls(geography)
  with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)
    response = requests.get(url, stream=True)
    outfile_name = tmpdir / f'{input_state}.zip'
    with open(outfile_name, 'wb') as outfile:
      for chunk in response.iter_content(chunk_size=8192):
        outfile.write(chunk)
    return gpd.read_file(f'zip://{outfile_name.absolute()}')