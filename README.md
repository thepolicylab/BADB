# BADB
Big Address DataBase geocodes and maintains a copy of the e911 database in Rhode Island

This repository primarily uses the E911 data available [here](https://www.rigis.org/datasets/e-911-sites). It also uses the [SmartyStreets](https://www.smartystreets.com/) API, for which you will need an API key to run this code.

## In Scope
### Phase I 
* `unique_id` Internally generated identification number associated with each unique address (01112)
* `version` Last updated date. (1)

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

F — Firm; the finest level of match available for an address.
(e.g., Julie Julia 11300 Center Ave Gilroy CA 95020-9257)\
G — General Delivery; for mail to be held at local post offices.
(e.g., General Delivery Provo UT 84601)\
H — High-rise; address contains apartment or building sub-units.
(e.g., 1600 Pennsylvania Ave SE Washington DC 20003-3228)\
P — Post Office box; address is a PO Box record type.
(e.g., PO Box 4735 Tulsa OK 74159-0735)\
R — Rural Route or Highway Contract; may have box number ranges.
(e.g., RR 2 Box 4560 Anasco PR 00610-9393)\
S — Street; address contains a valid primary number range.
(e.g., 16990 Monterey Rd Lake Elsinore CA 92530-7529)\
[blank] — No record type because address did not make a valid DPV match

### Phase II:


### VERSION HISTORY
- Version 1: 2020/06/04
