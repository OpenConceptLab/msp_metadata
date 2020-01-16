"""
Retrieves bulk import results and saves to file
"""
import ocldev.oclfleximporter


# Settings
results_filename = 'bulk_import_results_v2.json'
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = settings.ocl_api_token

# Bulk Import Task ID
task_id = 'enter-your-bulk-import-task-id-here'

results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
    task_id=task_id, api_url_root=api_url_root, api_token=ocl_api_token)

# Use this to write to a file
with open(results_filename, 'w') as ofile:
    ofile.write(results.to_json())

# Use this to move results into an OclImportResults object
import_results = ocldev.oclfleximporter.OclImportResults.load_from_json(results)
