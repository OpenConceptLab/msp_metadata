import json
import requests
import msp
import settings
import pprint
import ocldev.oclexport


# Settings
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token
org_id = 'PEPFAR-Test4'
source_id = 'MER-Test4'
periods = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']

# Metadata source files
filename_datim_codelists = 'data/codelists_RT_FY16_20_20200107.csv'

# Load codelists definitions from local JSON file
codelist_defs = msp.load_codelists(filename=filename_datim_codelists, org_id=org_id)
# codelists_dict = msp.build_codelists_dict(codelists=codelist_defs)

for codelist_def in codelist_defs:
    # Load code list from OCL and DATIM
    url_datim = '%s&paging=false' % codelist_def['ZenDesk: HTML Link'].replace('.html+css', '.json')
    url_ocl = '%s/orgs/%s/collections/%s/' % (api_url_root, org_id, codelist_def['id'])
    print '\n\n******** %s (%s)' % (codelist_def['id'], codelist_def['external_id'])
    print 'URLS:\n  ', url_ocl, '\n  ', url_datim
    print 'Code List Definition:'
    pprint.pprint(codelist_def)

    try:
        codelist_ocl = ocldev.oclexport.OclExportFactory.load_latest_export(url_ocl, oclapitoken=ocl_api_token)
        codelist_datim = msp.fetch_datim_codelist(url_datim)
        print '** OCL Code List Stats:'
        pprint.pprint(codelist_ocl.get_stats())
        print '** DATIM Code List Stats:'
        pprint.pprint(msp.get_datim_codelist_stats(codelist_datim))
        diff_result = msp.diff_codelist(codelist_ocl=codelist_ocl, codelist_datim=codelist_datim)
        print '** DIFF:'
        pprint.pprint(diff_result)
    except ocldev.oclexport.OclUnknownResourceError as e:
        print '\nCould not retrieve export for: %s' % url_ocl
