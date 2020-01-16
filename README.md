## CONTENT
* Code Lists - Correspond with Datasets in DHIS2
    * `codelist_20191128.csv` - FY16-19 Code Lists compiled from Zen Desk
* MER Indicators [mer.csv]
    * `mer_20191128.csv` - FY18-20 MER Guidance
* Data Elements [??]

## SCRIPTS
* PEPFAR Organization
    * Create PEPFAR Organization
    * Wipe PEPFAR Organization
* MSP Environment
    * Initialize MSP environment (MER Source)
    * Wipe MSP environment (MER source, Code Lists, ...)
* MER Reference Indicators
    * Load MER reference indicators into OCL from CSV
* Code Lists
    * Load Code Lists into OCL from CSV
    * Verify Code Lists in OCL
* DATIM Data Elements

## ADDITIONAL NOTES
* Each PEPFAR code list corresponds to a Data Set in DHIS2. DHIS2 Data Sets were retrieved
from dev-de.datim.org on 2019-12-05 using the query below. Code Lists were manually tabulated
from ZenDisk into a spreadsheet. Code Lists and Data Sets were then matched up using the DataSet ID.
```
https://dev-de.datim.org/api/dataSets.csv?fields=id,href,shortName,name,code,description,periodType&paging=false
```

## Updating Code Lists
Each year new code lists are published. Updating code lists in MSP is a multi-step process:
1. Retrieve datasets from DHIS2 and save as CSV -- see `get_dhis_datasets.py`
2. Compile code lists from ZenDesk into CSV (https://datim.zendesk.com/hc/en-us/articles/115002334246-DATIM-Data-Import-and-Exchange-Resources)
3. Populate empty fields in spreadsheet with DataSet data from DHIS2
