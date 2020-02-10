"""
Retrieves bulk import results and saves to file
"""
import ocldev.oclfleximporter
import settings


# Settings
results_filename = 'logs/import_results_staging_PEPFAR_Test4_20200207.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token

# Bulk Import Task ID
task_id = '3b42cd4e-0da4-43ec-9aa1-2ef2ecc71a8c-datim-admin'

results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
    task_id=task_id, api_url_root=api_url_root, api_token=ocl_api_token)

# Write results to file
if results and results_filename:
    with open(results_filename, 'w') as ofile:
        ofile.write(results.to_json())
