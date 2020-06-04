# BADB
Big Address DataBase geocodes and maintains a copy of the e911 database in Rhode Island

This repository primarily uses the E911 data available [here](https://www.rigis.org/datasets/e-911-sites). It also uses the [SmartyStreets](https://www.smartystreets.com/) API, for which you will need an API key to run this code.

## In Scope
### Phase I 
* `unique_id` Internally generated identification number associated with each unique address ex) 01112

(Source: E911 Dataset)
* `street` The street address to send the letter to (250 Main Street)
* `secondary` The secondary component of the address (Ste A4)
* `city` The city component of the address (Warwick)
* `state` The state component of the address (RI)
* `zipcode` The ZIP code of the address 
(Source: Smarty Streets API)
* `rdi` Residential Delivery Indicator (residential or commercial)
* `vacant`
* `active`
