import gzip
import itertools as its
import string
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from functools import partial
from pathlib import Path
from typing import Optional

import click
import pandas as pd
from tqdm import tqdm

from . import geoutils, file_utils


@click.group()
def cli():
    """ Tools for building the Big Address Database """


@cli.command("chunk")
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "-n",
    "--num-rows",
    type=int,
    default=0,
    help="The number of rows per chunk. Default is all rows in a single chunk",
)
@click.option(
    "-o",
    "--output-directory",
    type=str,
    default=".",
    help="The directory to which we output the chunks",
)
@click.option(
    "-s",
    "--suffix",
    type=str,
    default="_{:04d}",
    help="The format string representing the chunk suffix. Default is _{:04d}",
)
def chunk_command(filename: str, num_rows: int, suffix: str, output_directory: str):
    """
    Chunk a CSV file
    """
    filename = Path(filename)
    basename = Path(filename.name)
    suffixes = filename.suffixes
    total_suffix = "".join(suffixes)

    output_directory = Path(output_directory).absolute()
    if not output_directory.exists():
        output_directory.mkdir(parents=True)

    name = output_directory / basename

    if ".csv" in suffixes:
        # Remove suffixes from the filename
        chunk_prefix = name.with_suffix("")
        for _ in range(len(suffixes) - 1):
            chunk_prefix = chunk_prefix.with_suffix("")

        filename_formatter = str(chunk_prefix) + suffix + total_suffix
        with file_utils.gz_aware_opener(filename) as infile:
            headers = next(infile)
            with closing(
                file_utils.chunk_output_manager(filename_formatter, headers, num_rows)
            ) as coro:
                next(coro)  # Prime the generator
                for row in infile:
                    coro.send(row)


@cli.command("expand")
@click.option(
    "-i",
    "--auth-id",
    type=str,
    default=None,
    envvar="SS_AUTH_ID",
    help="Smarty Streets Auth ID",
)
@click.option(
    "-t",
    "--auth-token",
    type=str,
    default=None,
    envvar="SS_AUTH_TOKEN",
    help="Smarty Streets Auth Token",
)
@click.option(
    "-c", "--config", type=str, default=None, help="Smarty Streets auth config file"
)
@click.option(
    "-s",
    "--state",
    type=str,
    default="RI",
    help="The state to append to the address list (default is RI)",
)
@click.option(
    "-n",
    "--num-addresses",
    type=int,
    default=None,
    help="How many rows from address_file to expand. Default is all.",
)
@click.argument("address_list", type=click.Path(exists=True))
def expand_command(
    address_list: str,
    state: str,
    num_addresses: Optional[int],
    auth_id: Optional[str],
    auth_token: Optional[str],
    config: Optional[str],
):
    """
    address_list is in the format of the e911 database, i.e., has columns::

        OBJECTID, PrimaryAdd, ZN, Zip

    We add a column 'State' that has the constant value `state`
    """
    if not (auth_id and auth_token):
        if not config:
            raise click.BadOptionUsage(
                "config", "You must provide one of config or auth-id/auth-token pair"
            )
        with open(config, "rt") as infile:
            auth_id, auth_token = infile.read().strip().split(",")

    click.echo("Opening e911 data")
    df = pd.read_csv(
        address_list, nrows=num_addresses
    )  # N.B. if num_addresses is None this will read the whole file
    df["State"] = state
    ss_input = df[["OBJECTID", "PrimaryAdd", "ZN", "State", "Zip"]]

    click.echo("Running initial check for multi-unit buildings")
    with ThreadPoolExecutor() as executor:
        init_df = pd.DataFrame.from_dict(
            list(
                tqdm(
                    executor.map(
                        geoutils.smarty_api,
                        (row for row in ss_input.values),
                        its.repeat(auth_id),
                        its.repeat(auth_token),
                        its.repeat(True),
                    ),
                    total=len(ss_input),
                )
            )
        )

    single_units = init_df[init_df.match == "Y"].reset_index(drop=True)
    invalid_address = init_df[init_df.match.isnull()].reset_index(drop=True)
    multi_units = init_df[(init_df.match == "S") | (init_df.match == "D")].reset_index(
        drop=True
    )
    click.echo(
        f"Of {len(df)} addresses checked, {len(single_units)} were single units, {len(multi_units)} were multi-units, and {len(invalid_address)} were invalid"
    )

    click.echo("Creating test permutations...")
    num = [1, 11, 101, 1001]
    alpha = ["A"]
    perm_list = geoutils.create_perm(num, alpha)

    click.echo("Testing initial set of potential unit numbers...")
    mu_rerun = multi_units[
        ["object_id", "street", "city", "state", "zipcode"]
    ].values.tolist()
    mu_init = pd.concat(
        list(
            tqdm(
                map(
                    geoutils.joining_permutations,
                    mu_rerun,
                    its.repeat(perm_list),
                    its.repeat(auth_id),
                    its.repeat(auth_token),
                ),
                total=len(mu_rerun),
            )
        )
    )

    click.echo("Creating full permutations...")
    # Up through 'O'
    alpha = string.ascii_uppercase[:15]
    samp_total = list(
        set(
            geoutils.create_perm(range(1, 10), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(11, 100)), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(101, 1000)), alpha)
            + geoutils.create_perm(geoutils.appropriate_nums(range(1001, 10000)), alpha)
        )
    )


if __name__ == "__main__":
    cli()
