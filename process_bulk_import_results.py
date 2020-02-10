import ocldev.oclfleximporter
import json
import pprint


# Settings
results_filename = 'logs/import_results_staging_PEPFAR_Test4_20200207.json'

# Load results into an OclImportResults object
with open(results_filename) as ifile:
	results = json.load(ifile)
import_results = ocldev.oclfleximporter.OclImportResults.load_from_json(results)

# Do this first
print('IMPORT STATS:')
pprint.pprint(import_results.get_stats())
print import_results.get_import_results(
	results_mode=ocldev.oclfleximporter.OclImportResults.OCL_IMPORT_RESULTS_MODE_SUMMARY)
exit()

logging_keys = import_results.get_logging_keys()

# pprint.pprint(logging_keys)
root_key = '/orgs/PEPFAR-Test1/sources/MER-Test1/'
print import_results.get_import_results(
	results_mode=ocldev.oclfleximporter.OclImportResults.OCL_IMPORT_RESULTS_MODE_SUMMARY,
	root_key=root_key)
r = import_results.get(root_key)
# print 'update:', r[root_key]['update'].keys()
# print '\t200:', len(r[root_key]['update']['200'])
# print 'new:', r[root_key]['new'].keys()
# print '\t201:', len(r[root_key]['new']['201'])
# print '\t400:', len(r[root_key]['new']['400'])

# managed_from_concept_urls = [
# 	"/orgs/PEPFAR-Test1/sources/MER-Test1/concepts/EMR_SITE/",  # Resolved in gdoc and csv
# 	"/orgs/PEPFAR-Test1/sources/MER-Test1/concepts/VMMC_TOTALCIRC_NAT/",  # Resolved in gdoc and csv
# 	'/orgs/PEPFAR-Test1/sources/MER-Test1/concepts/HRH_STAFF_NAT/',  # No indicator for FY17, but appears to be in the DEs
# 	"/orgs/PEPFAR-Test1/sources/MER-Test1/concepts/CXCA_TX/",  # No indicator for FY17, but appears to be in the DEs
# 	"/orgs/PEPFAR-Test1/sources/MER-Test1/concepts/CXCA_SCRN/",  # No indicator for FY17, but appears to be in the DEs
# ]
managed_from_concept_urls = []


count = 0
for logging_key in r:
	for action_type in r[logging_key]:
		for status_code in r[logging_key][action_type]:
			if status_code not in ['400']:
				continue
			for result in r[logging_key][action_type][status_code]:
				count += 1
				if (result['obj_type'] == 'Mapping' and result['message'] == u'{"errors": "from_concept_url : Concept matching query does not exist."}'):
					obj = json.loads(result['text'])
					if obj['from_concept_url'] not in managed_from_concept_urls:
						print '\n**** Found a bad from_concept_url:'
						pprint.pprint(result)
						pprint.pprint(obj)
						print count
						exit()
				elif (result['obj_type'] == 'Mapping' and result['message'] == u'{"errors": "Parent, map_type, from_concept, to_concept must be unique."}'):
					# this is no prob at all, bob
					count += 1
				else:
					print '\n**** Found something else really bad...'
					pprint.pprint(result)
					print count
					exit()

print count



# print import_results.get_detailed_summary()
# print import_results.elapsed_seconds
# print import_results.get_import_results(
# 	results_mode=ocldev.oclfleximporter.OclImportResults.OCL_IMPORT_RESULTS_MODE_REPORT)
