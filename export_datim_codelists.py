"""
Fetch CSV of datasets from DHIS2
"""
import requests
import csv
import re
import pprint
import settings
import msp


# Set org/source ID, input periods, and map types
org_id = 'PEPFAR-Test2'
source_id = 'MER-Test2'

# Source files
filename_datim_codelists = 'data/codelists_RT_FY16_20.csv'
filename_output = 'datasets_20200107.csv'

# DHIS2 DataSet Query
query = 'https://dev-de.datim.org/api/dataSets.csv?fields=id,href,shortName,name,code,description,periodType&paging=false'


def get_datim_dataset(dataset_url):
    """ Fetch JSON representation of a single DataSet from DATIM DHIS2 """
	dataset_url = '%s%s' % (dataset_url.replace('.html+css', '.json'), '&paging=false')
	r = requests.get(dataset_url)
	r.raise_for_status()
	return r.json()


# Process the code lists
codelists = msp.load_codelists(filename=filename_datim_codelists, org_id=org_id)
for codelist in codelists:
	dataset_url = codelist['ZenDesk: HTML Link']
	sqlview_id = codelist['ZenDesk: DATIM SqlView ID']
	dataset_id = codelist['ZenDesk: DATIM DataSet ID']
	dataset = get_datim_dataset(dataset_url)
	pprint.pprint(codelist)
	pprint.pprint(dataset)
	print ''
	exit()


# # Fetch the export from DATIM and save to file
# r = requests.get(query, auth=(settings.datim_username, settings.datim_password))
# r.raise_for_status()

# # Write to file
# with open(filename, mode='wb') as localfile:
#     localfile.write(r.content)

