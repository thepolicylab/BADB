# BADB
Big Address DataBase is a series of python scripts that provide a command line interface (cli) to geocode large address databases.
The original design was intended for the Rhode Island Enhanced-911 (E-911) Data set, but can be adapted to apply to any dataset.

## What can I find here?
- `scripts` folder: Breaks down each step of the workflow into independent python scripts.
- `badb` folder: Provides a cli to run workflow set out in `scripts`.
For further detail, refer to the READ.me files contained within each folder.

## Set up:
The `BADB` package relies on `poetry`. `poetry` is a tool for dependency management and packaging in Python. \
You can learn more about `poetry` [here](https://python-poetry.org/docs/)
```
poetry check    # ensure that you have poetry installed, and that the .toml file is correct
python3 -m venv venv    
source venv/bin/activate    
poetry install    # The install command reads the pyproject.toml file from the current project, resolves the dependencies, and installs them.
poetry build    # The build command builds the source and wheels archives.
poetry run badb     		
```
The script was designed primarily for the E-911 database of Rhode Island. For further exploration, the E-911 data available [here](https://www.rigis.org/datasets/e-911-sites). \
For further information regarding the [SmartyStreets](https://www.smartystreets.com/) services and API, refer to the [documentation](https://smartystreets.com/docs/cloud/us-street-api)


### VERSION HISTORY
- Version 1: 2020/06/04
