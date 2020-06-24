`scripts` folder: Breaks down each step of the workflow into independent python scripts.
  - `00_Get_Raw_Output.py`: Validates input addresses with the SmartyStreets API
  - `01_Retry_Errors.py`: Rerun addresses that were not successful in initial run due to zipcode errors
  - `10_Secondary_Addresses.py`: Expands multi-unit addresses from `00_Get_Raw_Output.py` by testing out various secondary address formats
  - `20_Append_Census_Data.py`: Include Census information to output from `10_Secondary_Address.py`
