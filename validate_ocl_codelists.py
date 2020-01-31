import json
import requests
import msp
import settings
import pprint
import ocldev.oclexport


def fetch_datim_codelist(url_datim):
    r = requests.get(url_datim)
    r.raise_for_status()
    return r.json()


# OCL Settings
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token
org_id = 'PEPFAR-Test2'
source_id = 'MER-Test2'
periods = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']
map_type_indicator_to_de = 'Has Data Element'
map_type_de_to_coc = 'Has Option'

# TODO: DATIM DHIS2 Settings

# Metadata source files
filename_datim_codelists = 'data/codelists_RT_FY16_20_20200107.csv'

# Load codelists definitions from local JSON file
codelists = msp.load_codelists(filename=filename_datim_codelists, org_id=org_id)
# codelists_dict = msp.build_codelists_dict(codelists=codelists)

for codelist in codelists:
    url_datim = codelist['ZenDesk: HTML Link'].replace('.html+css', '.json')
    url_ocl = '%s/orgs/%s/collections/%s/' % (api_url_root, org_id, codelist['id'])
    print codelist['external_id'], codelist['id'], url_datim, url_ocl, '\n'
    pprint.pprint(codelist)
    codelist_ocl = ocldev.oclexport.OclExportFactory.get_latest_version_id(url_ocl, oclapitoken=ocl_api_token)
    codelist_datim = fetch_datim_codelist(url_datim)
    pprint.pprint(codelist_datim)
    break

