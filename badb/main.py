import concurrent.futures
import click
import geopandas as gpd
import pandas as pd
import string
import ujson

from itertools import repeat
from pathlib import Path
from tqdm.auto import tqdm

from badb import geoutils, data_utils

## Default Paths ##
ROOT_DIR = data_utils.ROOT_DIR
DATA_DIR = ROOT_DIR / Path('data')
RAW_OUTPUT = DATA_DIR / Path('00_ss_raw.csv.gz')
FIXED_OUTPUT = DATA_DIR / Path('01_ss_fixed.csv.gz')
TOTAL_OUTPUT_FILE = DATA_DIR / Path('10_ss_total.csv.gz')
CENSUS_JOINED_OUTPUT = DATA_DIR / Path('20_ss_census.csv.gz')

## Default CRS ##
RI_CRS = 'epsg:6568'


def preliminary_test(input_file_dir, state, num_addresses, ss_auth_id, ss_auth_token, raw_output=RAW_OUTPUT):
  click.echo("Opening dataset")
  df = pd.read_csv(
    input_file_dir, nrows=num_addresses
  )  # N.B. if num_addresses is None this will read the whole file  df['State'] = STATE
  df["State"] = state
  try:
    ss_input = df[['OBJECTID', 'PrimaryAdd', 'ZN', 'State', 'Zip']]
  except:
    click.echo('Column names are incorrect')
    click.echo(f'Current Input columns are: {list(ss_input.columns)}')
    exit()

  ss_list = ss_input.dropna(subset=['PrimaryAdd']).values.tolist()

  click.echo('SmartyStreets Run-1 : Validating Addresses')
  with concurrent.futures.ThreadPoolExecutor() as executor:
    init_df = pd.DataFrame.from_dict(
      list(tqdm(executor.map(geoutils.smarty_api, ss_list,
                             repeat(ss_auth_id),
                             repeat(ss_auth_token),
                             repeat(True)), total=len(ss_list)
                )
           )
    )

  # Output raw output
  init_df.to_csv(raw_output, compression='gzip', index=False)


def retry_errors(state, ss_auth_id, ss_auth_token, FIXED_OUTPUT=FIXED_OUTPUT):
  df = pd.read_csv(RAW_OUTPUT, compression='gzip')
  zip_list = list(geoutils.get_shp_file(state).ZCTA5CE10)

  redo_df = df[df.output.isna()]
  # Only run this smartystreets when there are errors
  if redo_df.empty:
    df.to_csv(FIXED_OUTPUT, compression='gzip', index=False)
  if not redo_df.empty:
    redo_df.city = None
    click.echo(f'{len(redo_df)} addresses have errors')
    click.echo(f'Re-running SmartyStreets on {len(redo_df)} addresses')

    ss_input = redo_df[['OBJECTID', 'street', 'city', 'state']].values.tolist()
    zip_hacked = pd.concat(list(tqdm(map(geoutils.joining_permutations, ss_input,
                                         repeat(zip_list),
                                         repeat(ss_auth_id),
                                         repeat(ss_auth_token),
                                         repeat(True)))))
    zip_hacked.drop_duplicates(subset=['OBJECTID', 'street', 'city', 'state', 'zipcode'], inplace=True)
    # Combine zip_hacked with Output that did not need redo, and output
    df[df.output.notna()].append(zip_hacked).reset_index(drop=True) \
      .drop_duplicates() \
      .to_csv(FIXED_OUTPUT, compression='gzip', index=False)


def secondary_addresses(ss_auth_id, ss_auth_token,
                        FIXED_OUTPUT=FIXED_OUTPUT,
                        TOTAL_OUTPUT_FILE=TOTAL_OUTPUT_FILE):
  df = pd.read_csv(FIXED_OUTPUT, compression='gzip')
  temp = pd.json_normalize(
    df.output.apply(ujson.loads))
  df.drop(['output', 'zipcode'], axis=1, inplace=True)  # zipcode is dropped because of overlap
  init_df = pd.concat([df, temp], axis=1)
  single_units = init_df[(init_df.dpv_match_code == 'Y')].reset_index(drop=True)
  multi_units = init_df[(init_df.dpv_match_code == 'S') |
                        (init_df.dpv_match_code == 'D')].reset_index(drop=True)
  click.echo(f'Total Entries: {len(df)}, '
             f'Single Unit Entries: {len(single_units)}, '
             f'Multi Unit Entries: {len(multi_units)}')

  if multi_units.empty:
    click.echo("Writing a combined output file")
    single_units.to_csv(TOTAL_OUTPUT_FILE, compression='gzip', index=False)

  if not multi_units.empty:
    click.echo('Searching for Secondary Addresses')
    # Create a list of possible permutations.
    # Most apartment rooms are either just numeric, just alpha, or a permutation of the two.
    num = [1, 11, 101, 1001]
    alpha = ['A']  # just 'A', realizing additional alphabets dont necessarily help
    perm_list = geoutils.create_perm(num, alpha, separate=False)
    perm_list = sorted(perm_list, key=len, reverse=True)

    # mu_rerun is a list of just the addresses for the multi_unit addresses
    ## mu = multi units
    mu_rerun = multi_units[['OBJECTID', 'street', 'city', 'state', 'zipcode']].values.tolist()
    mu_init = pd.concat(list(tqdm(map(geoutils.joining_permutations, mu_rerun,
                                      repeat(perm_list),
                                      repeat(ss_auth_id),
                                      repeat(ss_auth_token)))))

    alpha = list(string.ascii_uppercase[:15])
    perm_total = geoutils.create_perm(geoutils.appropriate_nums(range(1001, 10000)), alpha) \
                 + geoutils.create_perm(geoutils.appropriate_nums(range(101, 1000)), alpha) \
                 + geoutils.create_perm(geoutils.appropriate_nums(range(11, 100)), alpha) \
                 + geoutils.create_perm(range(1, 10), alpha) + [alpha]
    perm_dict = dict(zip(perm_list, perm_total))

    # Expansive search
    in_sample_key = list(mu_init.secondary.unique())
    in_sample_val = [perm_dict[perm] for perm in in_sample_key]
    total_hacked = geoutils.address_hacking(in_sample_key, in_sample_val, mu_init, ss_auth_id, ss_auth_token)
    temp = pd.json_normalize(total_hacked.output.apply(ujson.loads))
    total_hacked.drop(['output', 'zipcode'], axis=1, inplace=True)

    click.echo("Writing a combined output file")
    pd.concat(
      [pd.concat([total_hacked, temp], axis=1), single_units]
    ).to_csv(
        TOTAL_OUTPUT_FILE, compression='gzip', index=False)


def append_census_data(state,
                       total_output_file=TOTAL_OUTPUT_FILE,
                       census_joined_output=CENSUS_JOINED_OUTPUT,
                       ri_crs=RI_CRS):
  click.echo("appending census data")
  df = pd.read_csv(TOTAL_OUTPUT_FILE,
                   compression='gzip')
  df.drop(['candidate_index', 'addressee', 'delivery_line_1', 'delivery_line_2', 'input_id',
           'last_line', 'delivery_point_barcode', 'record_type', 'zip_type',
           'county_fips', 'county_name', 'carrier_route', 'elot_sequence', 'elot_sort',
           'precision', 'time_zone', 'utc_offset', 'obeys_dst', 'is_ews_match', 'dpv_footnotes',
           'cmra', 'footnotes', 'lacs_link_code', 'lacs_link_indicator', 'is_suite_link_match',
           'urbanization', 'primary_number', 'street_name', 'street_predirection',
           'street_postdirection', 'street_suffix', 'secondary_number', 'extra_secondary_number',
           'extra_secondary_designator', 'pmb_designator', 'pmb_number',
           'city_name', 'default_city_name', 'state_abbreviation', 'plus4_code', 'delivery_point',
           'delivery_point_check_digit'], axis=1, inplace=True)
  col_names = list(df.columns) + ['COUNTYFP10', 'TRACTCE10', 'BLKGRPCE10']
  geo_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
  geo_df.crs = ri_crs
  cbg_shp = geoutils.get_shp_file(state, 'blockgroup')
  cbg_shp.crs = ri_crs
  joined_df = gpd.sjoin(geo_df, cbg_shp, op='within')[col_names]
  joined_df.to_csv(census_joined_output, compression='gzip', index=False)
  joined_df[['rdi', 'latitude', 'longitude']]
