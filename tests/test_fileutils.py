import gzip
import tempfile
from contextlib import closing
from pathlib import Path

from badb import file_utils


EXAMPLE_CSV = """\
head,other_head
1,2
3,4
5,6
7,8
9,10
"""


def test_gz_aware_opener_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        with open(tmpdir / "test.csv", "wt") as outfile:
            outfile.write(EXAMPLE_CSV)

        with file_utils.gz_aware_opener(tmpdir / "test.csv") as infile:
            assert infile.read() == EXAMPLE_CSV


def test_gz_aware_opener_gz():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        with gzip.open(tmpdir / "test.csv.gz", "wt") as outfile:
            outfile.write(EXAMPLE_CSV)

        with file_utils.gz_aware_opener(tmpdir / "test.csv.gz") as infile:
            assert infile.read() == EXAMPLE_CSV


def test_chunk_output_manager_gz():
    filename_formatter = "output_{:04d}.csv.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        filename_formatter = str(tmpdir / filename_formatter)
        lines = EXAMPLE_CSV.strip().split("\n")
        with closing(
            file_utils.chunk_output_manager(filename_formatter, lines[0] + "\n", 2)
        ) as coro:
            next(coro)
            for line in lines[1:]:
                coro.send(line + "\n")

        assert len(list(tmpdir.glob("*.csv.gz"))) == 3
        for i in range(3):
            with gzip.open(filename_formatter.format(i), "rt") as infile:
                actual = infile.read()
            expected = "head,other_head\n{},{}\n".format(4 * i + 1, 4 * i + 2)
            if i < 2:
                expected += f"{4 * i + 3},{4 * i + 4}"
            assert actual.strip() == expected.strip()


def test_chunk_output_manager_csv():
    filename_formatter = "output_{:04d}.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        filename_formatter = str(tmpdir / filename_formatter)
        lines = EXAMPLE_CSV.strip().split("\n")
        with closing(
            file_utils.chunk_output_manager(filename_formatter, lines[0] + "\n", 2)
        ) as coro:
            next(coro)
            for line in lines[1:]:
                coro.send(line + "\n")

        assert len(list(tmpdir.glob("*.csv"))) == 3
        for i in range(3):
            with open(filename_formatter.format(i), "rt") as infile:
                actual = infile.read()
            expected = "head,other_head\n{},{}\n".format(4 * i + 1, 4 * i + 2)
            if i < 2:
                expected += f"{4 * i + 3},{4 * i + 4}"
            assert actual.strip() == expected.strip()
