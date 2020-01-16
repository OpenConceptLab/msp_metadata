import datetime
import ocldev.oclfleximporter
import settings

json_filename = 'mer_import.json'
api_url_root = 'https://api.demo.openconceptlab.org'
api_token = settings.ocl_api_token
test_mode = False

print datetime.datetime.now()
import_request = ocldev.oclfleximporter.OclBulkImporter.post(
	file_path=json_filename, api_url_root=api_url_root, api_token=api_token)
import_request.raise_for_status()
import_response = import_request.json()
import_task_id = import_response['task']
print import_task_id
import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
	task_id=import_task_id, api_url_root=api_url_root, api_token=api_token, max_wait_seconds=300, delay_seconds=5)
print datetime.datetime.now()
print import_results.display_report()
