"""
Prepares DATIM metadata for import into OCL

TO DO NOW:
* There's an unexpected overlap of 127 data elements between PDH and DATIM DEs, which means
  that some DATIM DEs are referenced in a run_sequence but not marked as sourced from DATIM

TO DO NEXT IMPORT:
* Build MOH alignment-style relationships between PDH source & derived DE+COC pairs
* Assign non-standard DDE indicator codes to standard reference indicators
* Build "Replaced by" mappings to connect a new DE version to an older version
* Add FYs to code list names (maybe)
* Improve handling of periods for targets (COP19) vs. results (FY19)
* Retire concepts (Data Elements, Indicators, and COCs) from previous FY that are no longer used

Resolved:
* Add versioned reference indicators w/ DE/DDE maps to the DATIM/PDH/MER collections
* Add "pepfarSupportType" attribute to DATIM DEs
* Add "numeratorDenominator" attribute to DATIM DEs
* Create collection versions by year for each code list
* Add mappings to the code list collections by setting "cascade" to "sourcemappings"
* Change DE "code_list_ids" to "codeLists" and copy the format of "dataSets"

Example of getting indicator formulas (numerators and denominators)
https://dev-de.datim.org/api/indicators?filter=name:like:VMMC&fields=id,code,name,numerator,denominator
"""
import json
import msp


# VERBOSITY: Set verbosity level during processing:
# 1 - Summary information only
# 2 - Display detailed results
# 3 - Display detailed results and inputs
# 4 - Display summary of each resource processed
# 5 - Display raw and transformed JSON for each resource processed
# 6 - Not for the faint of heart -- ALL info, including for skipped resources
verbosity = 0

# Set org/source ID, input periods, and map types
org_id = 'PEPFAR-Test5'
source_id = 'MER-Test5'
periods = ['FY16', 'FY17', 'FY18', 'FY19', 'FY20']  # List of all input periods
pdh_num_run_sequences = 3  # Number of run sequences for processing PDH derived DEs

# Set what the script outputs
output_periods = ['FY17', 'FY18', 'FY19', 'FY20']
output_ocl_formatted_json = True  # Creates the OCL import JSON
include_new_org_json = True  # Creates new org
include_new_source_json = True  # Creates new source
output_codelist_json = False  # Outputs JSON for code lists to be used by MSP

# Metadata source files
filename_datim_data_elements = 'data/datim_dataElements_20200213.json'
filename_datim_cocs = 'data/datim_categoryOptionCombos_20200213.json'
filename_datim_codelists = 'data/codelists_RT_FY16_20_20200107.csv'
filenames_mer_indicators = [
    'data/mer_indicators_FY17_20200122.csv',
    'data/mer_indicators_FY18_20200121.csv',
    'data/mer_indicators_FY19_20200121.csv',
    'data/mer_indicators_FY20_20200121.csv']
filename_pdh = 'data/pdh_20200105.csv'


# LOAD METADATA SOURCES
# Outputs:
# 1. indicators -- FY17-20 Reference Indicators, one OCL-formatted JSON concept per indicator
# 2. datim_de_all -- All DATIM DEs as exported from DHSI2, list of DHIS2-JSON resources
# 3. datim_cocs -- ALL DATIM COCs as exported from DHIS2, list of DHIS2-JSON resources
# 4. codelists -- FY16-20 Code lists, list of one OCL-formatted collection per codelists
# 5. pdh_raw -- All PDH records, list of one CSV row per record
# 6. sorted_indicator_codes -- Sorted and de-duped list of indicator codes (eg TX_CURR,TX_NEW)
indicators = msp.load_indicators(
    filenames=filenames_mer_indicators, org_id=org_id, source_id=source_id)
datim_de_all = msp.load_datim_data_elements(filename=filename_datim_data_elements)
datim_cocs = msp.load_datim_cocs(filename=filename_datim_cocs)
codelists = msp.load_codelists(filename=filename_datim_codelists, org_id=org_id)
pdh_raw = msp.load_pdh(filename=filename_pdh)
sorted_indicator_codes = msp.get_sorted_unique_indicator_codes(indicators=indicators)

# Summarize metadata loaded
if verbosity:
    msp.display_input_metadata_summary(
        verbosity=verbosity, indicators=indicators, datim_de_all=datim_de_all,
        datim_cocs=datim_cocs, codelists=codelists, pdh_raw=pdh_raw,
        sorted_indicator_codes=sorted_indicator_codes)


# GENERATE OCL-FORMATTED METADATA
# Process DATIM metadata sources to produce the following outputs:
# 1. coc_concepts -- Dictionary with COC URL as key and OCL-formatted COC dictionary as value
# 2. de_concepts -- Dictionary with DE URL as key and OCL-formatted DE dictionary as value
# 3. map_indicator_to_de -- Dictionary with indicator_code as key and list of DE URLs as value
# 4. map_de_to_coc -- Dictionary with DE URL as key and list of COC URLs as value
# 5. map_codelist_to_de -- Dictionary with Codelist ID as key and list of DE URLs as value
# 6. pdh_dde_concepts -- Dictionary with unique DDE URL as key and DDE concept as value
# 7. map_pdh_dde_to_coc -- Dictionary with DDE URL as key and list of COC URLs as value
# 8. map_indicator_to_pdh_dde -- Dict with indicator_code as key & list of DDE URLs as value
coc_concepts = msp.build_all_datim_coc_concepts(datim_cocs, org_id, source_id)
de_concepts = msp.build_all_datim_de_concepts(
    datim_de_all, org_id, source_id, sorted_indicator_codes, codelists)
map_indicator_to_de = msp.build_indicator_to_de_maps(de_concepts, sorted_indicator_codes)
map_de_to_coc = msp.build_de_to_coc_maps(de_concepts, coc_concepts, org_id, source_id)
map_codelist_to_de = msp.build_codelist_to_de_map(de_concepts)
pdh_dde_concepts = msp.build_all_pdh_dde_concepts(
    pdh_raw, pdh_num_run_sequences, org_id, source_id, sorted_indicator_codes)
map_pdh_dde_to_coc = msp.build_pdh_dde_to_coc_maps(
    pdh_dde_concepts, coc_concepts=coc_concepts)
map_indicator_to_pdh_dde = msp.build_indicator_to_de_maps(
    pdh_dde_concepts, sorted_indicator_codes)

# Summarize results of processing all data elements
if verbosity:
    msp.display_processing_summary(
        verbosity=verbosity,
        codelists=codelists,
        sorted_indicator_codes=sorted_indicator_codes,
        coc_concepts=coc_concepts,
        de_concepts=de_concepts,
        map_indicator_to_de=map_indicator_to_de,
        map_de_to_coc=map_de_to_coc,
        map_codelist_to_de=map_codelist_to_de,
        map_pdh_dde_to_coc=map_pdh_dde_to_coc,
        map_indicator_to_pdh_dde=map_indicator_to_pdh_dde)


# GENERATE REFERENCES
# Organize MSP metadata into collections:
# 1. codelist_references -- List of OCL-formatted reference batches to
#       all DEs, COCs, and mappings between them
# 2. datim_references -- Dictionary with period as key, list of
#       OCL-formatted references for all DATIM data elements as value
# 3. pdh_references -- Dictionary with period as key, list of
#       OCL-formatted references for all PDH data elements as value
# 4. mer_references -- Dictionary with period as key, list of
#       OCL-formatted references for all DATIM+PDH data elements as value
codelist_references = msp.build_codelist_references(
    map_codelist_to_de=map_codelist_to_de, map_de_to_coc=map_de_to_coc, org_id=org_id)
datim_references = msp.get_mapped_concept_references_by_period(
    from_concepts=de_concepts, map_dict=map_de_to_coc,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_DATIM, periods=output_periods,
    include_to_concept_refs=True, include_all_period=True)
datim_indicator_references = msp.get_mapped_concept_references_by_period(
    from_concepts=indicators, map_dict=map_indicator_to_de,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_DATIM, periods=output_periods,
    include_to_concept_refs=False, include_all_period=True,
    ignore_from_concepts_with_no_maps=True)
pdh_references = msp.get_mapped_concept_references_by_period(
    from_concepts=pdh_dde_concepts, map_dict=map_pdh_dde_to_coc,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_PDH, periods=output_periods,
    include_to_concept_refs=True, include_all_period=True)
pdh_indicator_references = msp.get_mapped_concept_references_by_period(
    from_concepts=indicators, map_dict=map_indicator_to_pdh_dde,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_PDH, periods=output_periods,
    include_to_concept_refs=False, include_all_period=True,
    ignore_from_concepts_with_no_maps=True)
mer_references = msp.build_mer_references(
    de_concepts=de_concepts, map_de_to_coc=map_de_to_coc,
    pdh_dde_concepts=pdh_dde_concepts, map_pdh_dde_to_coc=map_pdh_dde_to_coc,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_MER, periods=output_periods,
    include_to_concept_refs=True, include_all_period=False)
mer_indicator_references = msp.build_mer_indicator_references(
    indicators=indicators, map_indicator_to_de=map_indicator_to_de,
    map_indicator_to_pdh_dde=map_indicator_to_pdh_dde,
    org_id=org_id, collection_id=msp.COLLECTION_NAME_MER, periods=output_periods,
    include_to_concept_refs=False, include_all_period=False)

# Display summary of the references generated
if verbosity:
    msp.display_references_summary(
        codelist_references=codelist_references,
        datim_references=datim_references,
        datim_indicator_references=datim_indicator_references,
        pdh_references=pdh_references,
        pdh_indicator_references=pdh_indicator_references,
        mer_references=mer_references,
        mer_indicator_references=mer_indicator_references)


# OUTPUT OCL-FORMATTED JSON
# Prints OCL-formatted JSON by period to the standard output. De-duplicates the entire import
# list by line prior to outputting. Content by period is outputted in this sequence:
#  - Primary Org and Source
#     - Category Option Combos
#     - DATIM Data Elements (DEs)
#     - PDH Derived Data Elements (DDEs)
#     - Maps: DEs/DDEs to COCs
#     - Reference Indicators (grouped by period)
#     - Maps: Reference Indicators to DEs/DDEs
#     - Source Version
#  - Codelists
#     - DE/COC References
#     - Codelist Collection Versions
#  - Source collections (MER, DATIM, PDH)
#     - Collections
#     - Reference Indicators
#     - Maps: Indicators to Data Elements
#     - DE/DDE/COC References
#     - Collection Versions
if output_ocl_formatted_json:
    import_list = []
    if include_new_org_json:
        import_list.append(msp.get_new_org_json(org_id=org_id))
    if include_new_source_json:
        import_list.append(msp.get_primary_source(org_id=org_id, source_id=source_id))

    # Output OCL-formatted concepts for COCs, DEs, DDEs for the primary source
    for concept_url in coc_concepts:
        import_list.append(coc_concepts[concept_url])
    for concept_url in pdh_dde_concepts:
        import_list.append(pdh_dde_concepts[concept_url])
    for concept_url in de_concepts:
        import_list.append(de_concepts[concept_url])

    # Output OCL-formatted mappings for DATIM DEs-->COCs
    import_list += msp.build_ocl_mappings(
        map_dict=map_de_to_coc, owner_id=org_id, source_id=source_id,
        map_type=msp.MSP_MAP_TYPE_DE_TO_COC)

    # Output OCL-formatted mappings for PDH DDEs-->COCs
    import_list += msp.build_ocl_mappings(
        map_dict=map_pdh_dde_to_coc, owner_id=org_id, source_id=source_id,
        map_type=msp.MSP_MAP_TYPE_DE_TO_COC)

    # Output OCL-formatted concepts for Reference Indicators
    import_list += msp.get_concepts_filtered_by_period(
        concepts=indicators, period=output_periods)

    # Output OCL-formatted mappings for Indicators-->DATIM DEs
    import_list += msp.build_ocl_mappings(
        map_dict=map_indicator_to_de, owner_id=org_id, source_id=source_id,
        map_type=msp.MSP_MAP_TYPE_INDICATOR_TO_DE)

    # Output OCL-formatted mappings for Indicators-->PDH DDEs
    import_list += msp.build_ocl_mappings(
        map_dict=map_indicator_to_pdh_dde, owner_id=org_id, source_id=source_id,
        map_type=msp.MSP_MAP_TYPE_INDICATOR_TO_DE)

    # Generate released version for the primary source
    import_list.append(msp.get_repo_version_json(
        owner_id=org_id, repo_id=source_id, version_id='v1.0',
        description='Auto-generated release'))

    # Output codelists and their references
    import_list += codelists
    import_list += codelist_references
    for codelist in codelists:
        import_list.append(msp.get_repo_version_json(
            owner_id=org_id, repo_type='Collection', repo_id=codelist['id'],
            version_id='v1.0', description='Auto-generated release'))

    # Output Metadata Source collections (MER, DATIM, MSP)
    for collection_id in msp.COLLECTION_NAMES:
        if collection_id != msp.COLLECTION_NAME_MER:
            import_list.append(msp.get_new_repo_json(
                owner_id=org_id, repo_type='Collection', repo_id=collection_id,
                name=collection_id, full_name=collection_id))
        for period in output_periods:
            period_collection_id = '%s-%s' % (collection_id, period)
            import_list.append(msp.get_new_repo_json(
                owner_id=org_id, repo_type='Collection', repo_id=period_collection_id,
                name=period_collection_id, full_name=period_collection_id))
    for period_key in datim_references:
        import_list += datim_references[period_key]
    for period_key in pdh_references:
        import_list += pdh_references[period_key]
    for period_key in mer_references:
        import_list += mer_references[period_key]
    for collection_id in msp.COLLECTION_NAMES:
        if collection_id != msp.COLLECTION_NAME_MER:
            import_list.append(msp.get_repo_version_json(
                owner_id=org_id, repo_type='Collection', repo_id=collection_id,
                version_id='v1.0', description='Auto-generated release'))
        for period in output_periods:
            period_collection_id = '%s-%s' % (collection_id, period)
            import_list.append(msp.get_repo_version_json(
                owner_id=org_id, repo_type='Collection', repo_id=period_collection_id,
                version_id='v1.0', description='Auto-generated release'))

    # Dedup the import list without changing order
    import_list_dedup = msp.dedup_list_of_dicts(import_list)

    # Output the list
    for resource in import_list_dedup:
        print json.dumps(resource)


# CODELIST JSON
# Optionally output all codelists as JSON intended for populating filters in MSP
if output_codelist_json:
    print json.dumps(msp.get_codelists_formatted_for_display(codelists=codelists))
