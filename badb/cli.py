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

from . import geoutils, file_utils, main


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
# make sure that this address list exists
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

    if not main.DATA_DIR.exists():
        main.DATA_DIR.mkdir()

    click.echo("Running initial check for multi-unit buildings")
    main.preliminary_test(
        input_file_dir=address_list,
        state=state,
        num_addresses=num_addresses,
        ss_auth_id=auth_id,
        ss_auth_token=auth_token
    )
    click.echo("Retrying Sample for Errors")
    main.retry_errors(
        state=state,
        ss_auth_id=auth_id,
        ss_auth_token=auth_token
    )
    main.secondary_addresses(
        ss_auth_id=auth_id,
        ss_auth_token=auth_token
    )

    main.append_census_data(
        state=state
    )

if __name__ == "__main__":
    cli()
