"""
Settings for msp_metadata repository
"""

# OCL API Tokens
OCL_API_TOKEN = ''

# DATIM DHIS2 Account
DATIM_USERNAME = ''
DATIM_PASSWORD = ''

# VERBOSITY: Set verbosity level during processing:
#   1 - Summary information only
#   2 - Display detailed report
#   3 - Display debug info
#   4 - Display all info
VERBOSITY = 4

# Output filename: "%s"s are replaced with MSP_ORG_ID, YYYYMMDD, and filenum
OUTPUT_FILENAME = 'output/msp_%s_%s.json'
OUTPUT_OCL_FORMATTED_JSON = True  # Creates the OCL import JSON

# Set org/source ID, input/output periods
MSP_ORG_ID = 'PEPFAR-Test10b'
MSP_SOURCE_ID = 'MER'
CANONICAL_URL = 'https://datim.org'
MSP_INPUT_PERIODS = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20', 'FY21']  # List of all input periods
IHUB_NUM_RUN_SEQUENCES = 3  # Number of run sequences for processing IHUB derived DEs
IHUB_RULE_PERIOD_END_YEAR = '2021'  # Constant for processing IHUB rule periods
OUTPUT_PERIODS = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20', 'FY21']

# Metadata source files: Updated for FY21
FILENAME_DATIM_CODELISTS = 'data/codelists_RT_FY16_21_20210106.csv'
FILENAME_MER_REFERENCE_INDICATORS = [
    'data/mer_indicators_FY16_20200615.csv',
    'data/mer_indicators_FY17_20200615.csv',
    'data/mer_indicators_FY18_20200611.csv',
    'data/mer_indicators_FY19_20200611.csv',
    'data/mer_indicators_FY20_20200611.csv',
    'data/mer_indicators_FY21_20210106.csv'
]
FILENAME_DATIM_DATA_ELEMENTS = 'data/datim_dataElements_20210309.json'
FILENAME_DATIM_COCS = 'data/datim_categoryOptionCombos_20210309.json'
FILENAME_DATIM_INDICATORS = 'data/datim_indicators_20210309.json'
FILENAME_DATIM_CODELISTS_WITH_EXPORT = (
    'data/codelist_collections_with_exports_FY16_21_20210309.json')
FILENAME_IHUB = 'data/ihub_mer_derived_data_rules_20210212b.csv'
