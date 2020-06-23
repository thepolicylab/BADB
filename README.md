# BADB
Big Address DataBase geocodes and maintains a copy of the e911 database in Rhode Island.

## Set up:
The `BADB` package relies on `poetry`. `poetry` is a tool for dependency management and packaging in Python. \
You can learn more about `poetry` [here](https://python-poetry.org/docs/)
```
poetry check # ensure that you have poetry installed, and that the .toml file is correct
python3 -m venv venv # create a virtual environment
source venv/bin/activate	# activate the virtual environment
poetry install # The install command reads the pyproject.toml file from the current project, resolves the dependencies, and installs them.
poetry build # The build command builds the source and wheels archives.
poetry run badb     		
```

This repository primarily uses the E911 data available [here](https://www.rigis.org/datasets/e-911-sites). \
It also uses the [SmartyStreets](https://www.smartystreets.com/) API, for which you will need an API key to run this code.

## In Scope
### Phase I 
* `unique_id` Internally generated identification number associated with each unique address
* `version` Last updated date. 
#### (Source: E911 Dataset)
* `street` The street address to send the letter to (250 Main Street)
* `secondary` The secondary component of the address (Ste A4)
* `city` The city component of the address (Warwick)
* `state` The state component of the address (RI)
* `zipcode` The ZIP code of the address 
#### (Source: Smarty Streets API)
* `latitude` The latitude coordinate of location (64.75233)
* `longitude` The longitude coordinate of location (-147.35297)
* `rdi` Residential Delivery Indicator (residential or commercial)
* `vacant` Indicates that a delivery point was active in the past but is currently vacant (in most cases, unoccupied over 90 days) and is not receiving deliveries. (Y)
* `active` Indicates whether the address is active, or "in-service" according to the USPS.
* `record_type` Indicates the type of record that was matched. Only given if a DPV match is made.
* `Congressional District`
#### (Source: Shape Files)
* `Census Tract`
* `Census Block Groups`

### Phase II:
#### Voting Information
* `Voting Precinct`
#### City Government
* `Ward`
#### State Legislature
* `House District`
* `Senate District`
#### Policing:
* `Police District`

Possible Sources: 
- Contacting Jess Cigna
- DataSparks seems to have some relevant information, but in [pdf form](https://datasparkri.org/maps/)
- Reach out to whoever is behind these city-level [websites](cityofnewport.com/living-in-newport/gis-mapping)

### Out of Scope:
* `Location image`
* `Hyperlinks to Google Maps API`


### VERSION HISTORY
- Version 1: 2020/06/04
