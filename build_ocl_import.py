"""
Prepares MSP metadata for import

Questions:
1. How to handle periods for targets (COP19) vs. results (FY19)

TO DO:
* Retire concepts (Data Elements, Indicators, and COCs) from previous FY that are no longer used
* Build "Replaced by" mappings to connect a new DE version to an older version

Resolved:
* Add "pepfarSupportType" attribute to DATIM DEs
* Add "numeratorDenominator" attribute to DATIM DEs
* Create collection versions by year for each code list
* Add mappings to the code list collections by setting "cascade" to "sourcemappings"
* Change DE "code_list_ids" to "codeLists" and copy the format of "dataSets"

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
org_id = 'PEPFAR-Test4'
source_id = 'MER-Test4'
periods = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']  # List of all input periods
map_type_indicator_to_de = 'Has Data Element'
map_type_de_to_coc = 'Has Option'

# Set what the script outputs
output_periods = ['FY17', 'FY18', 'FY19', 'FY20']
output_ocl_formatted_json = True  # Creates the OCL import JSON
include_new_org_json = True  # Creates new org
include_new_source_json = True  # Creates new source
include_new_source_versions = True  # One source version per period
include_new_codelist_versions = True  # One collection version per collection per period
output_codelist_json = False  # Outputs JSON for code lists to be used by MSP

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
LOAD METADATA SOURCES

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
pdh = None  # pdh = msp.load_pdh(filename=filename_pdh)

# Summarize metadata loaded
if verbosity:
    msp.display_metadata_summary(verbosity=verbosity, indicators=indicators, datim_de=datim_de,
                                 codelists=codelists, sorted_indicators=sorted_indicators, pdh=pdh)

"""
PROCESS METADATA: Process metadata sources to produce the following outputs:
1. de_concepts -- Dictionary with DE URL as key and transformed data element dictionary as value
2. coc_concepts -- Dictionary wiht COC URL as key and transformed COC dictionary as value
3. map_indicator_to_de -- Dictionary with indicator_code as key and list of data elements as value
4. map_de_to_coc -- Dictionary with DE URL as key and list COC URLs as value
5. de_skipped_no_code -- List of raw DEs that have no 'code' attribute
6. de_skipped_no_indicator -- List of raw DEs that did not map to an indicator code
7. matched_codelists -- List of code lists that matched at least one processed DEs
8. matched_periods -- List of periods (eg FY18) that had at least one matched DE
9. dirty_data_element_ids -- List of data element IDs with a code that needed to be reformatted
"""
de_concepts = {}
coc_concepts = {}
map_indicator_to_de = {}
map_de_to_coc = {}
de_skipped_no_code = []
de_skipped_no_indicator = []
matched_codelists = []
matched_periods = []
dirty_data_element_ids = []
if verbosity >= 4:
    print '\nPROCESSING:'
for de_raw in datim_de['dataElements']:
    de_concept = msp.build_concept_from_datim_de(
        de_raw, org_id, source_id, sorted_indicators, codelists_dict)

    # Handle exceptions in data element creation
    if not isinstance(de_concept, dict):
        if de_concept == -1:
            de_skipped_no_code.append(de_raw)
            err_msg = 'SKIPPED: No Data Element Code'
        elif de_concept == -2:
            de_skipped_no_indicator.append(de_raw)
            err_msg = 'SKIPPED: Data element prefix does not match an indicator code'
        if verbosity >= 6:
            print '   %s\n    ' % err_msg, de_raw
        continue
    if verbosity >= 5:
        print '     Raw-DE: ', de_raw

    # Add dirty ID that were not skipped to the dirty ID tracking list
    de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_concept['id'])
    de_concept_id = de_concept['id']
    de_concept_id_dirty = de_concept['extras'].get('unformatted_id', de_concept_id)
    if de_concept_id != de_concept_id_dirty:
        dirty_data_element_ids.append('Raw: %s != Clean: %s' % (de_concept_id_dirty, de_concept_id))

    # Add indicator code to the map list
    if 'indicator' in de_concept['extras']:
        de_indicator_code = de_concept['extras']['indicator']
        if de_indicator_code not in map_indicator_to_de:
            map_indicator_to_de[de_indicator_code] = []
        map_indicator_to_de[de_indicator_code].append(de_concept_id)

    # Process DE's dataSets and Code Lists
    if 'Applicable Periods' in de_concept['extras']:
        matched_periods = list(set().union(
            matched_periods, de_concept['extras']['Applicable Periods']))
    if 'codelists' in de_concept['extras']:
        de_codelist_ids = []
        for de_matched_codelist in de_concept['extras']['codelists']:
            de_codelist_ids.append(de_matched_codelist['id'])
        matched_codelists = list(set().union(matched_codelists, de_codelist_ids))

    # Save the data element
    de_concepts[de_concept_url] = de_concept
    if verbosity >= 5:
        print '     OCL-DE: ', de_concept

    # Process disaggregates
    if verbosity >= 5:
        print '     COCs:'
    for coc_raw in de_raw['categoryCombo']['categoryOptionCombos']:
        # Generate the COC
        coc_concept_key = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, coc_raw['id'])
        coc_concept = msp.build_concept_from_datim_coc(coc_raw, org_id, source_id)

        # Add COC to map arrays
        coc_concepts[coc_concept_key] = coc_concept
        if de_concept_url not in map_de_to_coc:
            map_de_to_coc[de_concept_url] = []
        map_de_to_coc[de_concept_url].append(coc_concept_key)
        if verbosity >= 5:
            print '       Raw-COC: ', coc_raw
            print '       OCL-COC: ', coc_concept
    if verbosity >= 5:
        print '     All COC keys for this DE:', map_de_to_coc[de_concept_url]

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
    for indicator_code in map_indicator_to_de:
        if indicator_code in map_indicator_to_de and map_indicator_to_de[indicator_code]:
            num_indicators_with_de_maps += 1
            num_indicator_to_de_maps += len(map_indicator_to_de[indicator_code])
    if verbosity >= 2:
        print ''
    print '  Indicators with DE Maps: %s indicator codes, %s maps' % (
        num_indicators_with_de_maps, num_indicator_to_de_maps)
    if verbosity >= 2:
        for indicator_code in map_indicator_to_de:
            if map_indicator_to_de[indicator_code]:
                print '    %s (%s):' % (indicator_code, len(
                    map_indicator_to_de[indicator_code])), map_indicator_to_de[indicator_code]
    if verbosity >= 2:
        print ''
    print '  Indicators with no DE Maps: %s indicator codes' % str(
        len(sorted_indicators) - num_indicators_with_de_maps)
    if verbosity >= 2:
        for indicator_code in sorted_indicators:
            if indicator_code not in map_indicator_to_de:
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
    import_list = []
    if include_new_org_json:
        import_list.append(msp.get_new_org_json(org_id=org_id))
    if include_new_source_json:
        import_list.append(msp.get_new_source_json(org_id=org_id, source_id=source_id))
    for period in output_periods:
        for resource in filtered_codelists[period]:
            import_list.append(resource)
        for resource in filtered_indicators[period]:
            import_list.append(resource)
        for resource in filtered_de[period]:
            import_list.append(resource)
        for resource_key in filtered_disags[period].keys():
            import_list.append(filtered_disags[period][resource_key])
        for resource in filtered_maps_indicator_to_de[period]:
            import_list.append(resource)
        for resource in filtered_maps_de_to_coc[period]:
            import_list.append(resource)
        for codelist_id in filtered_codelist_references[period].keys():
            import_list.append(filtered_codelist_references[period][codelist_id])

        # Generate MER source version
        if include_new_source_versions:
            import_list.append(msp.get_repo_version_json(
                owner_id=org_id, repo_id=source_id, version_id=period,
                description='Auto-generated %s' % period))

        # Generate collection versions for each codelist updated during the current period
        if include_new_codelist_versions:
            for resource in filtered_codelists[period]:
                import_list.append(msp.get_repo_version_json(
                    owner_id=org_id, repo_type='Collection', repo_id=resource['id'],
                    version_id=period, description='Auto-generated %s' % period))

    # Dedup the import list without changing order
    import_list_dedup = msp.dedup_list_of_dicts(import_list)

    # Output the list
    for resource in import_list_dedup:
        print json.dumps(resource)

# Optionally output all codelists as JSON
if output_codelist_json:
    print json.dumps(msp.get_codelists_formatted_for_display(codelists=codelists))
