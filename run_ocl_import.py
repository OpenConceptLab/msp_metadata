import json
import ocldev.oclfleximporter
import settings


# Settings
json_filename = 'output/msp_full_FY17_20_20200127.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token
do_local_import = False  # Instead of bulk import

# Local import settings -- only used if do_local_import == True
test_mode = False
limit = 0  # Set to 0 to import all records

# Process the import
if do_local_import:
    importer = ocldev.oclfleximporter.OclFlexImporter(
        file_path=json_filename, api_url_root=api_url_root, api_token=ocl_api_token,
        test_mode=test_mode, verbosity=2, do_update_if_exists=True, limit=limit)
    importer.process()
else:  # Do bulk import
    import_request = ocldev.oclfleximporter.OclBulkImporter.post(
        file_path=json_filename, api_url_root=api_url_root, api_token=ocl_api_token)
    import_request.raise_for_status()
    import_response = import_request.json()
    task_id = import_response['task']
    print 'Bulk Import Task ID:\n%s\n' % task_id
