"""
Saves codelist exports retreived from DHIS2 in the OCL collection format.
"""
import json
import settings
import msp


codelist_collections = msp.load_codelist_collections(
    filename=settings.FILENAME_DATIM_CODELISTS, org_id=settings.MSP_ORG_ID)
with open(settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT, 'w') as output_file:
	output_file.write(json.dumps(codelist_collections.to_list()))
print '%s collections with their exports saved to "%s"' % (
	len(codelist_collections), settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT)
