"""
Saves codelist exports retreived from DHIS2 in the OCL collection format.
The input into this is a spreadsheet in the format of 
/data/codelists_RT_FY21_22_20220131.csv. The output is a large JSON file
containing the codelists as OCL-formatted collections.
"""
import json
import settings
import msp


# Load codelists from CSV and download/add the exports from ZenDesk exports to each
print 'Loading codelists...'
codelist_collections = msp.load_codelist_collections(
    filename=settings.FILENAME_DATIM_CODELISTS, org_id=settings.MSP_ORG_ID,
    canonical_url=settings.CANONICAL_URL, verbosity=2)

# Save codelists with their exports to file
with open(settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT, 'w') as output_file:
	output_file.write(json.dumps(codelist_collections.to_list()))
print '%s collections with their exports saved to "%s"' % (
	len(codelist_collections), settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT)
