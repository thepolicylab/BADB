import gzip
import io
import itertools as its
from contextlib import closing, contextmanager
from functools import partial
from pathlib import Path
from typing import ContextManager, Generator, Union


@contextmanager
def gz_aware_opener(filename: Union[str, Path], mode="rt") -> ContextManager[io.IOBase]:
    """
    Open a file with gzip.open if its name ends in .gz. Else use
    the regular open command
    """
    filename = Path(filename) if isinstance(filename, str) else filename
    if filename.suffix == ".gz":
        with gzip.open(filename, mode) as infile:
            yield infile
    else:
        with open(filename, mode) as infile:
            yield infile


def chunk_output_manager(
    filename_formatter: str, headers: str, rows_in_chunk: int = 0
) -> Generator[None, str, None]:
    """
    Setup a generator that chunks the lines of a file into
    many files, printing the same header row as the first line each time.

    Example usage::

    >>> from contextlib import closing
    >>>
    >>> with open('input.csv', 'wt') as infile:
    >>>     headers = next(infile)
    >>>     with closing(chunk_output_manager'foo_{:04d}.csv', 'left,right\n', 2)) as coro:
    >>>         next(coro)
    >>>         for line in infile:
    >>>             coro.send(line)
    """
    opener = (
        partial(gzip.open, mode="wt")
        if filename_formatter.endswith(".gz")
        else partial(open, mode="wt")
    )
    for chunk_num in its.count(0):
        # Only create a new file if there is data to write
        row = yield
        with opener(filename_formatter.format(chunk_num)) as outfile:
            outfile.write(headers)
            outfile.write(row)

            # rows_in_chunk is positive, go that many steps. Else go only the passed number of steps
            row_range = range(1, rows_in_chunk) if rows_in_chunk > 0 else its.count(1)
            for _ in row_range:
                row = yield
                outfile.write(row)
