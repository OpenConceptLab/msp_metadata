"""
Retrieves bulk import results and saves to file
"""
import ocldev.oclfleximporter
import settings


# Settings
results_filename = 'logs/bulk_import_results_staging_PEPFAR_Test3_20200127.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token

# Bulk Import Task ID
task_id = '24b3c7f7-fbca-4220-b625-63e1ffd194c5-datim-admin'

results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
    task_id=task_id, api_url_root=api_url_root, api_token=ocl_api_token)

# Write results to file
if results and results_filename:
    with open(results_filename, 'w') as ofile:
        ofile.write(results.to_json())
