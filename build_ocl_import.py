"""
Prepares MSP metadata for import

Questions:
2. How to handle periods for targets (COP19) vs. results (FY19)

TO DO:
* Build "Replaced by" mappings to connect a new DE version to an older version

Example of getting indicator formulas (numerators and denominators)
https://dev-de.datim.org/api/indicators?filter=name:like:VMMC&fields=id,code,name,numerator,denominator
"""
import json
import ocldev.oclcsvtojsonconverter
import msp


"""
Set verbosity level during processing:
 1 - Summary information only
 2 - Display detailed results
 3 - Display detailed results and inputs
 4 - Display summary of each resource processed
 5 - Display raw and transformed JSON for each resource processed
 6 - Not for the faint of heart -- ALL info, including for skipped resources
"""
verbosity = 0

# Set org/source ID, input periods, and map types
org_id = 'PEPFAR-Test1'
source_id = 'MER-Test1'
periods = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']
map_type_indicator_to_de = 'Has Data Element'
map_type_de_to_coc = 'Has Option'

# Set what the script outputs
output_periods = ['FY17', 'FY18', 'FY19', 'FY20']
output_ocl_formatted_json = True
include_new_org_json = True
include_new_source_json = True
include_new_source_versions = True
output_codelist_json = False

# Metadata source files
filename_datim_data_elements = 'data/datim_dataElements_20200107.json'
filename_datim_codelists = 'data/codelists_RT_FY16_20_20200107.csv'
filenames_mer_indicators = [
    'data/mer_indicators_FY17_20200122.csv',
    'data/mer_indicators_FY18_20200121.csv',
    'data/mer_indicators_FY19_20200121.csv',
    'data/mer_indicators_FY20_20200121.csv']
filename_pdh = 'data/pdh.csv'


"""
METADATA SOURCES: Load all metadata sources

Outputs:
1. indicators -- MER Guidance Reference Indicators (FY18-20)
2. datim_de -- DATIM Data Elements with their disags as sub-resources (<=FY20)
3. codelists -- Code lists (FY16-20)
4. pdh -- PDH (<=FY19)

Pre-process indicators and codelists, resulting in:
5. sorted_indicators -- Sorted and De-duped list of indicator codes
6. codelists_dict -- Dictionary of codelists with UID as the key
"""
indicators = msp.load_indicators(
    filenames=filenames_mer_indicators, org_id=org_id, source_id=source_id)
sorted_indicators = msp.get_sorted_unique_indicator_codes(indicators=indicators)
datim_de = msp.load_datim_data_elements(filename=filename_datim_data_elements)
codelists = msp.load_codelists(filename=filename_datim_codelists, org_id=org_id)
codelists_dict = msp.build_codelists_dict(codelists=codelists)
pdh = None
# pdh = msp.load_pdh(filename=filename_pdh)

# Summarize metadata loaded
if verbosity:
    print 'METADATA SOURCES:'
    print '  Reference Indicators (FY18-20):', len(indicators)
    if verbosity >= 5:
        for indicator in indicators:
            print '     ', indicator['id'], indicator['attr:Period'], indicator
        print ''
    print '  DATIM Data Elements (All):', len(datim_de['dataElements'])
    print '  Code Lists (FY16-20):', len(codelists), len(codelists_dict)
    if verbosity >= 3:
        for codelist in codelists:
            print '    [ %s ]  %s  --  %s' % (
                codelist['external_id'], codelist['id'], codelist['attr:Applicable Periods'])
        print ''
    print '  Unique Indicator Codes:', len(sorted_indicators)
    if verbosity >= 3:
        for indicator_code in sorted_indicators:
            print '    ', indicator_code
    # print 'PDH:', len(pdh)


"""
PROCESSING: Process metadata sources to produce the following outputs:
1. map_indicator_to_de -- Dictionary with indicator_code as key and list of data elements as value
2. map_de_to_coc -- Dictionary with DE URL as key and list COC URLs as value
2. de_concepts -- Dictionary with DE URL as key and transformed data element dictionary as value
3. coc_concepts -- Dictionary wiht COC URL as key and transformed COC dictionary as value
3. de_skipped_no_code -- List of raw DEs that have no 'code' attribute
4. de_skipped_no_indicator -- List of raw DEs that did not map to an indicator code
5. matched_codelists -- List of code lists that matched at least one processed DEs
6. matched_periods -- List of periods (eg FY18) that had at least one matched DE
"""

# Create a dictionary for indicator to DE maps pre-populated with indicator codes as keys
map_indicator_to_de = {}
for indicator_code in sorted_indicators:
    map_indicator_to_de[indicator_code] = []

# Process data elements
de_concepts = {}
coc_concepts = {}
map_de_to_coc = {}
de_skipped_no_code = []
de_skipped_no_indicator = []
matched_codelists = []
matched_periods = []
dirty_data_element_ids = []
if verbosity >= 4:
    print '\nPROCESSING:'
for de_raw in datim_de['dataElements']:
    # Set the concept ID or skip this data element if 'code' not defined
    if 'code' not in de_raw:
        de_skipped_no_code.append(de_raw)
        if verbosity >= 6:
            print '   SKIPPED: No Data Element Code\n    ', de_raw
        # raw_input("Press Enter to continue...")
        continue
    de_concept_id_dirty = de_raw['code']
    de_concept_id = msp.format_concept_id(unformatted_id=de_raw['code'])

    # Determine the indicator
    de_indicator_code = msp.lookup_indicator_code(
        de_code=de_concept_id, sorted_indicators=sorted_indicators)
    if de_indicator_code:
        map_indicator_to_de[de_indicator_code].append(de_concept_id)
    else:
        de_skipped_no_indicator.append(de_raw)
        if verbosity >= 6:
            print '   SKIPPED: Data element prefix does not match an indicator code\n    %s  %s' % (
                de_concept_id, de_raw)
        continue

    # Add dirty ID that were not skipped to the dirty ID tracking list
    if de_concept_id != de_concept_id_dirty:
        dirty_data_element_ids.append('Raw: %s != Clean: %s' % (de_concept_id_dirty, de_concept_id))

    # Determine result or target
    de_result_or_target = msp.get_data_element_result_or_target(de_code=de_concept_id)

    # Determine DE version number (if it has one)
    de_version = msp.get_data_element_version(de_code=de_concept_id)

    # Display summary of data element being processed
    if verbosity >= 4:
        print '  ', de_indicator_code, de_concept_id, de_version, de_result_or_target

    # Build the OCL-formatted data element
    if verbosity >= 5:
        print '     Raw-DE: ', de_raw
    de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_concept_id)
    de_concept_key = de_concept_url
    de_concept = {
        'type': 'Concept',
        'id': de_concept_id,
        'concept_class': 'Data Element',
        'datatype': 'Numeric',
        'owner': org_id,
        'owner_type': 'Organization',
        'source': source_id,
        'retired': False,
        'external_id': de_raw['id'],
        'descriptions': None,
        'extras': {
            'resultTarget': de_result_or_target,
            'indicator': de_indicator_code,
        },
        'names': [
            {
                'name': de_raw['name'],
                'name_type': 'Fully Specified',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            },
            {
                'name': de_raw['shortName'],
                'name_type': 'Short',
                'locale': 'en',
                'locale_preferred': False,
                'external_id': None,
            }
        ]
    }
    if 'description' in de_raw and de_raw['description']:
        de_concept['descriptions'] = [
            {
                'description': de_raw['description'],
                'description_type': 'Description',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            }
        ]
    if de_concept_id != de_concept_id_dirty:
        de_concept['extras']['unformatted_id'] = de_concept_id_dirty

    # Process DE's dataSets and Code Lists
    de_codelist_ids = []
    if 'dataSetElements' in de_raw and de_raw['dataSetElements']:
        de_concept['extras']['dataSets'] = []
        for dataset in de_raw['dataSetElements']:
            de_concept['extras']['dataSets'].append(dataset['dataSet'])
            if dataset['dataSet']['id'] in codelists_dict.keys():
                # print '\t', dataset['dataSet']
                # print '\t', codelists_dict[dataset['dataSet']['id']]
                de_codelist_ids.append(dataset['dataSet']['id'])
                codelists_dict[dataset['dataSet']['id']]['__dataElements'].append(de_concept_id)
        de_concept['extras']['code_list_ids'] = de_codelist_ids
        de_concept['extras']['Applicable Periods'] = msp.get_de_periods_from_codelists(
            de_codelist_ids=de_codelist_ids, codelists_dict=codelists_dict)
        matched_periods = list(set().union(
            matched_periods, de_concept['extras']['Applicable Periods']))
        matched_codelists = list(set().union(matched_codelists, de_codelist_ids))

    # Set DE custom attributes
    if 'dataElementGroups' in de_raw and de_raw['dataElementGroups']:
        de_concept['extras']['dataElementGroups'] = de_raw['dataElementGroups']
    if 'domainType' in de_raw and de_raw['domainType']:
        de_concept['extras']['domainType'] = de_raw['domainType']
    if 'valueType' in de_raw and de_raw['valueType']:
        de_concept['extras']['valueType'] = de_raw['valueType']
    if 'aggregationType' in de_raw and de_raw['aggregationType']:
        de_concept['extras']['aggregationType'] = de_raw['aggregationType']
    if de_version:
        de_concept['extras']['DATIM Data Element Version'] = de_version

    # Save the data element
    de_concepts[de_concept_key] = de_concept
    if verbosity >= 5:
        print '     OCL-DE: ', de_concept

    # Process disaggregates
    if verbosity >= 5:
        print '     COCs:'
    for coc_raw in de_raw['categoryCombo']['categoryOptionCombos']:
        coc_concept_key = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, coc_raw['id'])
        coc_concept = {
            'type': 'Concept',
            'id': coc_raw['id'],
            'concept_class': 'Category Option Combo',
            'datatype': 'None',
            'owner': org_id,
            'owner_type': 'Organization',
            'source': source_id,
            'retired': False,
            'descriptions': None,
            'external_id': coc_raw['id'],
            'names': [
                {
                    'name': coc_raw['name'],
                    'name_type': 'Fully Specified',
                    'locale': 'en',
                    'locale_preferred': True,
                    'external_id': None,
                }
            ]
        }
        coc_concepts[coc_concept_key] = coc_concept
        if de_concept_key not in map_de_to_coc:
            map_de_to_coc[de_concept_key] = []
        map_de_to_coc[de_concept_key].append(coc_concept_key)
        if verbosity >= 5:
            print '       Raw-COC: ', coc_raw
            print '       OCL-COC: ', coc_concept
    if verbosity >= 5:
        print '     All COC keys for this DE:', map_de_to_coc[de_concept_key]

# Summarize all results
if verbosity:
    print '\nSUMMARY RESULTS:'
    print '  Data elements skipped:', len(de_skipped_no_code) + len(de_skipped_no_indicator)
    print '    Data elements with no code:', len(de_skipped_no_code)
    if verbosity >= 2:
        for de_skipped in de_skipped_no_code:
            print '      ', de_skipped
    print '    Indicator codes not matched:', len(de_skipped_no_indicator)
    print '  Data elements transformed:', len(de_concepts)
    print '  Dirty data element IDs:', len(dirty_data_element_ids)
    if verbosity >= 2:
        for dirty_id in dirty_data_element_ids:
            print '    ', dirty_id
    print '  Matched Periods:', matched_periods
    if verbosity >= 2:
        print ''
    print '  Code Lists with Data Elements:', len(matched_codelists)
    if verbosity >= 2:
        for codelist in codelists:
            if codelist['external_id'] in matched_codelists:
                print '    ', codelist
    if verbosity >= 2:
        print ''
    print '  Code Lists with No Data Elements:', len(codelists) - len(matched_codelists)
    if verbosity >= 2:
        for codelist in codelists:
            if codelist['external_id'] not in matched_codelists:
                print '    ', codelist
    num_indicators_with_de_maps = 0
    num_indicator_to_de_maps = 0
    for indicator_code in map_indicator_to_de.keys():
        if map_indicator_to_de[indicator_code]:
            num_indicators_with_de_maps += 1
            num_indicator_to_de_maps += len(map_indicator_to_de[indicator_code])
    if verbosity >= 2:
        print ''
    print '  Indicators with DE Maps: %s indicator codes, %s maps' % (
        num_indicators_with_de_maps, num_indicator_to_de_maps)
    if verbosity >= 2:
        for indicator_code in map_indicator_to_de.keys():
            if map_indicator_to_de[indicator_code]:
                print '    %s (%s):' % (indicator_code, len(
                    map_indicator_to_de[indicator_code])), map_indicator_to_de[indicator_code]
    if verbosity >= 2:
        print ''
    print '  Indicators with no DE Maps: %s indicator codes' % str(
        len(map_indicator_to_de) - num_indicators_with_de_maps)
    if verbosity >= 2:
        for indicator_code in map_indicator_to_de.keys():
            if not map_indicator_to_de[indicator_code]:
                print '    %s' % indicator_code

# Prepare for import by period
filtered_indicators = {}
filtered_de = {}
filtered_disags = {}
filtered_codelists = {}
filtered_maps_indicator_to_de = {}
filtered_maps_de_to_coc = {}
filtered_codelist_references = {}
for period in periods:
    if verbosity:
        print '\n******** SUMMARY RESULTS BY PERIOD: %s ********' % period

    # Filter codelists by period and convert from CSV format to OCL-formatted JSON
    filtered_csv_codelists = msp.get_filtered_codelists(codelists=codelists, period=period)
    csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(
        input_list=filtered_csv_codelists)
    filtered_codelists[period] = csv_converter.process()

    # Filter indicators by period and convert from CSV format to OCL-formatted JSON
    filtered_csv_indicators = msp.get_filtered_indicators(indicators=indicators, period=period)
    csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(
        input_list=filtered_csv_indicators)
    filtered_indicators[period] = csv_converter.process()

    # Filter all of the other resources by period, which are already in OCL-formatted JSON
    filtered_de[period] = msp.get_filtered_data_elements(data_elements=de_concepts, period=period)
    filtered_disags[period] = msp.get_filtered_disags(
        data_elements=filtered_de[period], map_de_to_coc=map_de_to_coc, coc_concepts=coc_concepts)
    filtered_maps_indicator_to_de[period] = msp.get_indicator_to_de_maps(
        filtered_de=filtered_de[period], org_id=org_id, source_id=source_id,
        map_type_indicator_to_de=map_type_indicator_to_de)
    filtered_maps_de_to_coc[period] = msp.get_de_to_disag_maps(
        filtered_de=filtered_de[period], map_de_to_coc=map_de_to_coc,
        org_id=org_id, source_id=source_id, map_type_de_to_coc=map_type_de_to_coc)
    filtered_codelist_references[period] = msp.get_codelist_references(
        filtered_csv_codelists=filtered_csv_codelists, map_de_to_coc=map_de_to_coc,
        org_id=org_id, source_id=source_id)

    # Summarize results for this period
    if verbosity:
        print '  Indicators: %s' % len(filtered_indicators[period])
        print '  Data Elements: %s' % len(filtered_de[period])
        print '  Disags: %s' % len(filtered_disags[period])
        print '  Code Lists: %s' % len(filtered_codelists[period])
        print '  Indicator --> DE Maps: %s' % len(filtered_maps_indicator_to_de[period])
        print '  DE --> Disag Maps: %s' % len(filtered_maps_de_to_coc[period])
        count_codelist_references = 0
        for codelist_id in filtered_codelist_references[period].keys():
            count_codelist_references += len(
                filtered_codelist_references[period][codelist_id]['data']['expressions'])
        print '  Code List References: %s references in %s code lists' % (
            count_codelist_references, len(filtered_codelist_references[period]))

if verbosity:
    print ''

# Output OCL-formatted JSON by period
if output_ocl_formatted_json:
    if include_new_org_json:
        print json.dumps(msp.get_new_org_json(org_id=org_id))
    if include_new_source_json:
        print json.dumps(msp.get_new_source_json(org_id=org_id, source_id=source_id))
    for period in output_periods:
        for resource in filtered_codelists[period]:
            print json.dumps(resource)
        for resource in filtered_indicators[period]:
            print json.dumps(resource)
        for resource in filtered_de[period]:
            print json.dumps(resource)
        for resource_key in filtered_disags[period].keys():
            print json.dumps(filtered_disags[period][resource_key])
        for resource in filtered_maps_indicator_to_de[period]:
            print json.dumps(resource)
        for resource in filtered_maps_de_to_coc[period]:
            print json.dumps(resource)
        for codelist_id in filtered_codelist_references[period].keys():
            print json.dumps(filtered_codelist_references[period][codelist_id])

        # Generate MER source version
        if include_new_source_versions:
            print json.dumps(msp.get_repo_version_json(
                owner_id=org_id, repo_id=source_id, version_id=period,
                description='Auto-generated %s' % period))

# Optionally output codelist as JSON
if output_codelist_json:
    print json.dumps(msp.get_codelists_formatted_for_display(codelists=codelists))
