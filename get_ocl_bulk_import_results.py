"""
Retrieves bulk import results and saves to file
"""
import ocldev.oclfleximporter
import settings


# Settings
results_filename = 'logs/import_results.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.OCL_API_TOKEN

# Bulk Import Task ID
task_id = ''

# Fetch results and write to file
results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
    task_id=task_id, api_url_root=api_url_root, api_token=ocl_api_token)
if results and results_filename:
    with open(results_filename, 'w') as ofile:
        ofile.write(results.to_json())
        print 'Bulk import results for task "%s" successfully written to %s' % (
        	task_id, results_filename)
