"""
Imports a JSON lines file into OCL
"""
import ocldev.oclfleximporter
import ocldev.oclresourcelist
import settings


# settings
json_filename = 'output/msp_pepfar_test8_20200617.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.OCL_API_TOKEN
do_local_import = False  # Instead of bulk import

# Local import settings -- only used if do_local_import == True
test_mode = False
limit = 0  # Set to 0 to import all records
reference_batch_size = 25

# Validate
print 'Validating import file "%s"...' % json_filename
import_list = ocldev.oclresourcelist.OclJsonResourceList.load_from_file(json_filename)
import_list.validate()

# Process the import
if do_local_import:
    importer = ocldev.oclfleximporter.OclFlexImporter(
        file_path=json_filename, api_url_root=api_url_root, api_token=ocl_api_token,
        test_mode=test_mode, verbosity=2, do_update_if_exists=True, limit=limit,
        reference_batch_size=reference_batch_size)
    importer.process()
else:  # Do bulk import
    import_request = ocldev.oclfleximporter.OclBulkImporter.post(
        file_path=json_filename, api_url_root=api_url_root, api_token=ocl_api_token)
    import_request.raise_for_status()
    import_response = import_request.json()
    task_id = import_response['task']
    print '\nImport Filename: %s\nBulk Import Task ID:\n%s\n' % (json_filename, task_id)
