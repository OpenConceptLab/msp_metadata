"""
Local settings for msp_metadata repository
"""

# OCL API Tokens
ocl_api_token = ''

# DATIM DHIS2 Account
datim_username = ''
datim_password = ''

# VERBOSITY: Set verbosity level during processing:
# 1 - Summary information only
# 2 - Display detailed report
# 3 - Display debug info
# 4 - Display all info
VERBOSITY = 0

# Output settings
OUTPUT_FILENAME = 'output/build_pepfar_test8_20200617_%s.json'
IMPORT_LIST_CHUNK_SIZE = 20000

# Set org/source ID, input periods, and map types
MSP_ORG_ID = 'PEPFAR-Test8'
MSP_SOURCE_ID = 'MER'
MSP_INPUT_PERIODS = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']  # List of all input periods
PDH_NUM_RUN_SEQUENCES = 3  # Number of run sequences for processing PDH derived DEs

# Set what the script outputs
OUTPUT_PERIODS = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']
OUTPUT_OCL_FORMATTED_JSON = True  # Creates the OCL import JSON
IMPORT_SCRIPT_OPTION_ORG = True
IMPORT_SCRIPT_OPTION_SOURCE = True
IMPORT_SCRIPT_OPTION_REFERENCE_INDICATORS = True
IMPORT_SCRIPT_OPTION_DATIM_DE_CONCEPTS = True
IMPORT_SCRIPT_OPTION_PDH_DDE_CONCEPTS = True
IMPORT_SCRIPT_OPTION_DATIM_COC_CONCEPTS = True
IMPORT_SCRIPT_OPTION_REF_INDICATOR_COLLECTIONS = True
IMPORT_SCRIPT_OPTION_CODELIST_COLLECTIONS = True
IMPORT_SCRIPT_OPTION_SOURCE_COLLECTIONS = {
	'DATIM': True,
	'PDH': True,
	'MER': True,
}
IMPORT_SCRIPT_OPTIONS = [
	IMPORT_SCRIPT_OPTION_ORG,
	IMPORT_SCRIPT_OPTION_SOURCE,
	IMPORT_SCRIPT_OPTION_REFERENCE_INDICATORS,
	IMPORT_SCRIPT_OPTION_DATIM_DE_CONCEPTS,
	IMPORT_SCRIPT_OPTION_PDH_DDE_CONCEPTS,
	IMPORT_SCRIPT_OPTION_DATIM_COC_CONCEPTS,
	IMPORT_SCRIPT_OPTION_CODELIST_COLLECTIONS,
	IMPORT_SCRIPT_OPTION_SOURCE_COLLECTIONS,
]
OUTPUT_CODELIST_JSON = False  # Outputs JSON for code lists to be used by MSP

# Metadata source files
FILENAME_DATIM_DATA_ELEMENTS = 'data/datim_dataElements_20200611.json'
FILENAME_DATIM_COCS = 'data/datim_categoryOptionCombos_20200611.json'
FILENAME_DATIM_INDICATORS = 'data/datim_indicators_20200611.json'
FILENAME_DATIM_CODELISTS = 'data/codelists_RT_FY16_20_20200616.csv'
FILENAME_MER_REFERENCE_INDICATORS = [
	'data/mer_indicators_FY16_20200615.csv',
	'data/mer_indicators_FY17_20200615.csv',
	'data/mer_indicators_FY18_20200611.csv',
	'data/mer_indicators_FY19_20200611.csv',
	'data/mer_indicators_FY20_20200611.csv'
]
FILENAME_PDH = 'data/pdh_20200105.csv'
