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
(If you are struggling to download `poetry` even after following the documentation, I have some suggestions in the Appendix.)
```
poetry check    # ensure that you have poetry installed, and that the .toml file is correct  
poetry install    # The install command reads the pyproject.toml file from the current project, resolves the dependencies, and installs them.
poetry build    # The build command builds the source and wheels archives.
poetry shell    		
```
Now, you can activate the CLI (Command Line Interface) by running the `badb` command
```
badb
```

The script was designed primarily for the E-911 database of Rhode Island. For further exploration, the E-911 data available [here](https://www.rigis.org/datasets/e-911-sites). \
For further information regarding the [SmartyStreets](https://www.smartystreets.com/) services and API, refer to the [documentation](https://smartystreets.com/docs/cloud/us-street-api)
Refer to their [pricing page](https://smartystreets.com/pricing) for associated costs of generating an API key to run this package! (The scripts make exhaustive searches for accurate outputs, i.e each address may need more than 1 call on the API. Therefore, the unlimited service is highly recommended!)

## Getting started:
Going to the terminal and typing:
```
badb
```
Shows the list of available command. 
The instructions to using a particular function can be found by running the `--help` flag:
```
badb [command] --help
```

### Demonstration:
Credentials for the SmartyStreets API is stored in `path_to_config/config.csv`. 
The `expand` command to a `.csv` of addresses at `path_to_file/file.csv` can be done with the following command:
```
badb expand -c path_to_config/config.csv path_to_file/file.csv
```
For efficiency, the `expand` command only searches for addresses in a single specified US state. The command defaults to searching in Rhode Island. So, if `file.csv` is composed of addresses of a different state, specificy with the `-s` or `--state` flag:
```
badb expand --state IL -c path_to_config/config.csv path_to_file/file.csv
```

### Appendix
#### Q: `poetry` just doesnt seem to be working!
I have personally found the easiest way to install `poetry` is to use `Homebrew`. 
If you do not have `Homebrew` already installed, paste the following in a macOS Terminal or Linux shell prompt:
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```
or refer to the following [documentation](https://brew.sh/).

If you already have `Homebrew` installed, you can install poetry with:
```
brew install poetry
```


### VERSION HISTORY
- Version 1: 2020/06/04
