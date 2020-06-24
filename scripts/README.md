## Overview:
The `scripts` folder breaks down each step of the workflow into independent python scripts.  

- **00_Get_Raw_Output.py**: Validates input addresses with the SmartyStreets API  
  - Input: E911_FILE
  - Output: `00_ss_raw.csv.gz`  
    columns: [OBJECTID, street, city, state, output]
- **01_Retry_Errors.py**: Rerun addresses that were not successful in initial run due to zipcode error (`output` column is None)
  - Input: `00_ss_raw.csv.gz`
  - Output: `01_ss_fixed.csv.gz`    
    columns: [OBJECTID, street, city, state, output]
- **10_Secondary_Addresses.py**: Expands multi-unit addresses from `00_Get_Raw_Output.py` by testing out various secondary address formats
  - Input: `01_ss_fixed.csv.gz`
  - Output: `10_ss_total.csv.gz`    
    columns: [OBJECTID, street, city, state, ... {expanded output columns} ..., secondary]
- **20_Append_Census_Data.py**: Include Census information to output from `10_Secondary_Address.py`
  - Input: `10_ss_total.csv.gz`
  - Output: `20_ss_census.csv.gz`  
    columns: [OBJECTID, street, city, state, ... {expanded output columns} ... secondary, COUNTYFP10, TRACTCE10, BLKGRPCE10]
