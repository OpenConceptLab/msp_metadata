import json
import ocldev.oclcsvtojsonconverter
import ocldev.oclfleximporter


'''
What's next?
- Split MER up by FY and add Source Version resources
'''

# CSV Sources
csv_filenames = [
        'pepfar_org.csv',
        'mer_source.csv',
        'mer_indicators_FY18.csv',
        'mer_indicators_FY18_version.csv',
        'mer_indicators_FY19.csv',
        'mer_indicators_FY19_version.csv',
        'mer_indicators_FY20.csv',
        'mer_indicators_FY20_version.csv',
        'codelists_RT_FY18_19.csv',
    ]
csv_filenames = [
        'pepfar_test_org.csv',
        'mer_test_source.csv',
    ]

# Verbose -- set to False to only output JSON
verbose = False

# CSV Processing Method -- Process either one definition or one row at a time. PROCESS_BY_DEFINITION is default.
csv_processing_method = ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.PROCESS_BY_DEFINITION

# Process CSV files -- no need to change anything after this line
results = []
for csv_filename in csv_filenames:
    csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(
        csv_filename=csv_filename, verbose=verbose)
    results += csv_converter.process(method=csv_processing_method)

# Output as response -- note that output happens during processing if verbose is True
for result in results:
    print(json.dumps(result))
