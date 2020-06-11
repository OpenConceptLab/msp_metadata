"""
Prepares DATIM metadata for import into OCL

TO DO NEXT IMPORT:
* DATIM Indicator filters for indicator group (done), time period of validity (not done),
  results/targets - Need to generate content for these where possible (#737)
* DATIM indicator linkages to Data Elements used in its formula (either as mappings, as a custom
  attribute with an easily parseable JSON list, or both). Note that #717 suggests modeling the
  JSON attribute similar to how derivations are modeled -- analyze whether that approach makes
  sense (#713 / #717)
* Import updated MER Reference Indicators from google spreadsheets (#701)
* Exclude COCs from Codelists based on the ZenDesk queries that are used (#742)
* Add Reporting Frequency attribute from Reference Indicators to relevant data elements (#736)
* Some linkages appear not to be showing up in MSP or OCL -- this is possibly because they
  were not added to the correct collection or because they were not generated (#719)
* Update model so that Reference Indicators from previous periods are easily accessible via API
  and that the correct version of a Ref Indicator is added to the period-specific collections (#707)
* Add PDH-specific Reference Indicator codes and group them with the MER Reference Indicators,
  possibly by assigning non-standard DDE indicator codes to standard reference indicators (#707)
* Address overlap between DATIM & PDH data elements -- some DATIM DEs were replaced by PDH DDEs.
  In the last import, there were 127 data elements between PDH and DATIM with the same UIDs. (#707)

TICKETS: #701, #707, #713, #717, #719, #742, #736, #737

QUERIES THAT NEED TO BE DEFINED:
* Indicator custom attribute filters
* MOH Alignment Codelists: https://test.ohie.datim.org:5000/ocl-etl/moh/
* DATIM Codelists: https://test.ohie.datim.org:5000/ocl-etl/codelists/

TO DO LATER:
* Possibly retire concepts (DEs, Reference Indicators, and COCs) from previous FY that are no
  longer used applicalbe (Note, it is not clear that we will consider prior definitions as retired)
* Improve handling of periods for targets (COP19) vs. results (FY19)

Resolved and imported:
* Build "Replaced by" mappings to connect a new DE version to an older version
* Incorporate FY16 reference indicators
* Add PDH "derived_category_option_combo" field into "source_data_elements" custom attribute
* Fixed incorrect codelist collection ID issue
* Added list of source DEs/COCs as custom attribute of PDH derived DEs
* Added versioned reference indicators w/ DE/DDE maps to the DATIM/PDH/MER collections
* Added "pepfarSupportType" attribute to DATIM DEs
* Added "numeratorDenominator" attribute to DATIM DEs
* Created collection versions by year for each code list
* Added mappings to the code list collections by setting "cascade" to "sourcemappings"
* Changed DE "code_list_ids" to "codeLists" and copy the format of "dataSets"
"""
import json
import msp
import settings


# LOAD METADATA SOURCES
# 1. reference_indicators -- Array of FY16-20 reference indicator as OCL-formatted JSON concepts
# 2. datim_indicators -- Array of DATIM Indicators as exported from DHIS2
# 3. datim_de_all -- Array of all DATIM DEs as exported from DHIS2 (JSON)
# 4. datim_cocs -- Array of all DATIM COCs as exported from DHIS2 (JSON)
# 5. codelists -- Array of FY16-20 Codelists in OCL-formatted JSON
# 6. pdh_raw -- Array of PDH records in original CSV format
reference_indicators = msp.load_reference_indicators(
    filenames=settings.FILENAME_MER_REFERENCE_INDICATORS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)
datim_indicators = msp.load_datim_indicators(
    filename=settings.FILENAME_DATIM_INDICATORS)
datim_de_all = msp.load_datim_data_elements(filename=settings.FILENAME_DATIM_DATA_ELEMENTS)
datim_cocs = msp.load_datim_cocs(filename=settings.FILENAME_DATIM_COCS)
codelists = msp.load_codelists(
    filename=settings.FILENAME_DATIM_CODELISTS, org_id=settings.MSP_ORG_ID)
pdh_raw = msp.load_pdh(filename=settings.FILENAME_PDH)

# Summarize metadata loaded
if settings.VERBOSITY:
    msp.display_input_metadata_summary(
        verbosity=settings.VERBOSITY,
        input_periods=settings.MSP_INPUT_PERIODS,
        reference_indicators=reference_indicators,
        datim_indicators=datim_indicators,
        datim_de_all=datim_de_all,
        datim_cocs=datim_cocs,
        codelists=codelists,
        pdh_raw=pdh_raw)


# GENERATE OCL METADATA
# Process DATIM metadata sources to produce the following outputs:
#  1. sorted_indicator_codes -- De-duped list of indicator codes sorted by length descending
#  2. datim_indicator_concepts -- Dictionary with URL as key and OCL-formatted Indicator as value
#  3. coc_concepts -- Dictionary with COC URL as key and OCL-formatted COC dictionary as value
#  4. de_concepts -- Dictionary with DE URL as key and OCL-formatted DE dictionary as value
#  5. map_indicator_to_de -- Dictionary with indicator URL as key, list of DE URLs as value
#  6. map_de_to_coc -- Dictionary with DE URL as key and list of COC URLs as value
#  7. map_codelist_to_de -- Dictionary with Codelist ID as key and list of DE URLs as value
#  8. pdh_dde_concepts -- Dictionary with unique DDE URL as key and DDE concept as value
#  9. map_pdh_dde_to_coc -- Dictionary with DDE URL as key and list of COC URLs as value
# 10. map_indicator_to_pdh_dde -- Dict with indicator URL as key & list of DDE URLs as value
sorted_indicator_codes = msp.get_sorted_unique_indicator_codes(
    reference_indicators=reference_indicators)
coc_concepts = msp.build_all_datim_coc_concepts(
    datim_cocs, settings.MSP_ORG_ID, settings.MSP_SOURCE_ID)
de_concepts = msp.build_all_datim_de_concepts(
    datim_de_all, settings.MSP_ORG_ID, settings.MSP_SOURCE_ID, sorted_indicator_codes, codelists)
datim_indicator_concepts = msp.build_datim_indicator_concepts(
    datim_indicators=datim_indicators, org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
    de_concepts=de_concepts, coc_concepts=coc_concepts)
map_indicator_to_de = msp.build_indicator_to_de_maps(
    de_concepts, sorted_indicator_codes, settings.MSP_ORG_ID, settings.MSP_SOURCE_ID)
map_de_to_coc = msp.build_de_to_coc_maps(
    de_concepts, coc_concepts, settings.MSP_ORG_ID, settings.MSP_SOURCE_ID)
map_codelist_to_de = msp.build_codelist_to_de_map(de_concepts)
pdh_dde_concepts = msp.build_all_pdh_dde_concepts(
    pdh_raw, settings.PDH_NUM_RUN_SEQUENCES, settings.MSP_ORG_ID,
    settings.MSP_SOURCE_ID, sorted_indicator_codes)
map_pdh_dde_to_coc = msp.build_pdh_dde_to_coc_maps(
    pdh_dde_concepts, coc_concepts=coc_concepts)
map_indicator_to_pdh_dde = msp.build_indicator_to_de_maps(
    pdh_dde_concepts, sorted_indicator_codes, settings.MSP_ORG_ID, settings.MSP_SOURCE_ID)


# Summarize results of processing all data elements
if settings.VERBOSITY:
    msp.display_processing_summary(
        verbosity=settings.VERBOSITY,
        codelists=codelists,
        sorted_indicator_codes=sorted_indicator_codes,
        datim_indicator_concepts=datim_indicator_concepts,
        coc_concepts=coc_concepts,
        de_concepts=de_concepts,
        map_indicator_to_de=map_indicator_to_de,
        map_de_to_coc=map_de_to_coc,
        map_codelist_to_de=map_codelist_to_de,
        map_pdh_dde_to_coc=map_pdh_dde_to_coc,
        map_indicator_to_pdh_dde=map_indicator_to_pdh_dde,
        org_id=settings.MSP_ORG_ID,
        source_id=settings.MSP_SOURCE_ID)


# LINKAGES
# Generate mappings for linkages between data elements
# 1. de_version_linkages - Dictionary with version-less DE code as key, list of dicts describing
#        linked DEs as value (url, DE code, version number, sort order)
# 2. map_de_version_linkages - Dictionary with DE URL as key, list of replaced DE URLs as value
# 3. map_dde_source_linkages - Dictionary with DE URL as key, list of source DE URLs as value
de_version_linkages = msp.build_linkages_de_version(de_concepts=de_concepts)
de_version_linkages.update(msp.build_linkages_dde_version(pdh_dde_concepts=pdh_dde_concepts))
map_de_version_linkages = msp.build_maps_from_de_linkages(
    de_linkages=de_version_linkages, owner_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)
map_dde_source_linkages = msp.build_linkages_source_de(
    pdh_dde_concepts=pdh_dde_concepts, owner_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)

# Summarize results of processing linkages between resources
if settings.VERBOSITY:
    msp.display_linkages_summary(
        verbosity=settings.VERBOSITY,
        de_version_linkages=de_version_linkages,
        map_de_version_linkages=map_de_version_linkages,
        map_dde_source_linkages=map_dde_source_linkages)

# Convert to OCL-formatted mappings
# import_list = msp.build_ocl_mappings(
#     map_dict=map_de_version_linkages, map_type=msp.MSP_MAP_TYPE_REPLACES,
#     owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
# import_list += msp.build_ocl_mappings(
#     map_dict=map_dde_source_linkages, map_type=msp.MSP_MAP_TYPE_DERIVED_FROM,
#     owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
#
# import_list_dedup = msp.dedup_list_of_dicts(import_list)
# Output the list
# for resource in import_list_dedup:
#     print json.dumps(resource)


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
    map_codelist_to_de=map_codelist_to_de, map_de_to_coc=map_de_to_coc, org_id=settings.MSP_ORG_ID,
    codelists=codelists)
datim_references = msp.get_mapped_concept_references_by_period(
    from_concepts=de_concepts, map_dict=map_de_to_coc,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_DATIM,
    periods=settings.OUTPUT_PERIODS, include_to_concept_refs=True, include_all_period=True)
datim_indicator_references = msp.get_mapped_concept_references_by_period(
    from_concepts=reference_indicators, map_dict=map_indicator_to_de,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_DATIM,
    periods=settings.OUTPUT_PERIODS, include_to_concept_refs=False,
    include_all_period=True, ignore_from_concepts_with_no_maps=True)
pdh_references = msp.get_mapped_concept_references_by_period(
    from_concepts=pdh_dde_concepts, map_dict=map_pdh_dde_to_coc,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_PDH,
    periods=settings.OUTPUT_PERIODS,
    include_to_concept_refs=True, include_all_period=True)
pdh_indicator_references = msp.get_mapped_concept_references_by_period(
    from_concepts=reference_indicators, map_dict=map_indicator_to_pdh_dde,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_PDH,
    periods=settings.OUTPUT_PERIODS,
    include_to_concept_refs=False, include_all_period=True,
    ignore_from_concepts_with_no_maps=True)
mer_references = msp.build_mer_references(
    de_concepts=de_concepts, map_de_to_coc=map_de_to_coc,
    pdh_dde_concepts=pdh_dde_concepts, map_pdh_dde_to_coc=map_pdh_dde_to_coc,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_MER,
    periods=settings.OUTPUT_PERIODS,
    include_to_concept_refs=True, include_all_period=False)
mer_indicator_references = msp.build_mer_indicator_references(
    reference_indicators=reference_indicators, map_indicator_to_de=map_indicator_to_de,
    map_indicator_to_pdh_dde=map_indicator_to_pdh_dde,
    org_id=settings.MSP_ORG_ID, collection_id=msp.COLLECTION_NAME_MER,
    periods=settings.OUTPUT_PERIODS,
    include_to_concept_refs=False, include_all_period=False)

# Display summary of the references generated
if settings.VERBOSITY:
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
#  1. ALL REPOSITORIES
#      a. Primary Org and Source (eg /orgs/PEPFAR/sources/MER/)
#      b. Codelist collections
#      c. Metadata source collections (MER-FY**, PDH-FY**, DATIM-FY**)
#  2. FOR EACH PERIOD...
#      a. Period-based Concepts for the primary source
#          1. Reference Indicator Concepts for current period
#          2. DATIM DE Concepts for current period
#          3. PDH DDE Concepts for current period
#          4. DATIM COC Concepts for current period
#      b. Period-based Mappings for the primary source
#          1. RefInd --> DATIM DE Mappings for current period
#          2. RefInd --> PDH DDE Mappings for current period
#          3. DATIM DE --> DATIM COC Mappings for current period
#          4. PDH DDE --> DATIM COC Mappings for current period
#      c. Period-based References
#          1. Codelist References: Cascaded DEs + Non-cascaded COCs for current period
#          2. Metadata Source References (MER, PDH, DATIM):
#             Cascaded RefInds + Cascaded DEs/DDEs + Non-cascaded COCs for current period
#  3. ALL REPOSITORY VERSIONS
#      a. Primary Source Version
#      b. Codelist Collection Versions
#      c. Metadata Source Collection Versions
#  4. CLEANUP: De-duplicate import list without changing order & leaving 1st occurrence in place
if settings.OUTPUT_OCL_FORMATTED_JSON:
    import_list = []

    # 1. ALL REPOSITORIES
    # 1.a. Primary Org and Source (eg /orgs/PEPFAR/sources/MER/)
    if settings.IMPORT_SCRIPT_OPTION_ORG:
        import_list.append(msp.get_new_org_json(org_id=settings.MSP_ORG_ID))
    if settings.IMPORT_SCRIPT_OPTION_SOURCE:
        import_list.append(msp.get_primary_source(
            org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID))

    # 1.b Codelist collections
    if settings.IMPORT_SCRIPT_OPTION_CODELIST_COLLECTIONS:
        import_list += codelists

    # 1.c. Metadata source collections (MER-FY**, PDH-FY**, DATIM-FY**)
    for collection_id in msp.COLLECTION_NAMES:
        if (collection_id not in settings.IMPORT_SCRIPT_OPTION_SOURCE_COLLECTIONS or
                not settings.IMPORT_SCRIPT_OPTION_SOURCE_COLLECTIONS[collection_id]):
            continue
        if collection_id != msp.COLLECTION_NAME_MER:
            import_list.append(msp.get_new_repo_json(
                owner_id=settings.MSP_ORG_ID, repo_type='Collection', repo_id=collection_id,
                name=collection_id, full_name=collection_id))
        for period in settings.OUTPUT_PERIODS:
            period_collection_id = '%s-%s' % (collection_id, period)
            import_list.append(msp.get_new_repo_json(
                owner_id=settings.MSP_ORG_ID, repo_type='Collection', repo_id=period_collection_id,
                name=period_collection_id, full_name=period_collection_id))

    # 2. FOR EACH PERIOD...
    for period in settings.OUTPUT_PERIODS:

        # 2.a. Period-based Concepts for the primary source:
        # 2.a.1. Reference Indicator Concepts for current period
        indicator_concepts_for_period = msp.get_concepts_filtered_by_period(
                concepts=reference_indicators, period=period)
        if settings.IMPORT_SCRIPT_OPTION_REFERENCE_INDICATORS:
            import_list += indicator_concepts_for_period

        # 2.a.2. DATIM DE Concepts for current period
        datim_de_concepts_for_period = msp.get_concepts_filtered_by_period(
                concepts=de_concepts, period=period)
        if settings.IMPORT_SCRIPT_OPTION_DATIM_DE_CONCEPTS:
            import_list += datim_de_concepts_for_period

        # 2.a.3. PDH DDE Concepts for current period
        pdh_dde_concepts_for_period = msp.get_concepts_filtered_by_period(
                concepts=pdh_dde_concepts, period=period)
        if settings.IMPORT_SCRIPT_OPTION_PDH_DDE_CONCEPTS:
            import_list += pdh_dde_concepts_for_period

        # 2.a.4. DATIM COC Concepts filtered by DEs/DDEs for current period
        if settings.IMPORT_SCRIPT_OPTION_DATIM_COC_CONCEPTS:
            import_list += msp.get_filtered_cocs(
                de_concepts=datim_de_concepts_for_period, map_de_to_coc=map_de_to_coc,
                coc_concepts=coc_concepts)
            import_list += msp.get_filtered_cocs(
                de_concepts=pdh_dde_concepts_for_period, map_de_to_coc=map_pdh_dde_to_coc,
                coc_concepts=coc_concepts)

        # 2.b. Period-based Mappings for the primary source
        # 2.b.1. RefInd --> DATIM DE Mappings for current period
        import_list += msp.build_ocl_mappings(
            map_dict=map_indicator_to_de, filtered_from_concepts=indicator_concepts_for_period,
            map_type=msp.MSP_MAP_TYPE_INDICATOR_TO_DE,
            owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)

        # 2.b.2. RefInd --> PDH DDE Mappings for current period
        import_list += msp.build_ocl_mappings(
            map_dict=map_indicator_to_pdh_dde, owner_id=settings.MSP_ORG_ID,
            source_id=settings.MSP_SOURCE_ID, map_type=msp.MSP_MAP_TYPE_INDICATOR_TO_DE)

        # 2.b.3. DATIM DE --> DATIM COC Mappings for current period
        import_list += msp.build_ocl_mappings(
            map_dict=map_de_to_coc, owner_id=settings.MSP_ORG_ID,
            source_id=settings.MSP_SOURCE_ID, map_type=msp.MSP_MAP_TYPE_DE_TO_COC)

        # 2.b.4. PDH DDE --> DATIM COC Mappings for current period
        import_list += msp.build_ocl_mappings(
            map_dict=map_pdh_dde_to_coc, owner_id=settings.MSP_ORG_ID,
            source_id=settings.MSP_SOURCE_ID, map_type=msp.MSP_MAP_TYPE_DE_TO_COC)

        # 2.c. Period-based References
        # 2.c.1. Codelist References: Cascaded DEs + Non-cascaded COCs for current period
        import_list += codelist_references

        # 2.c.2. Metadata Source References (MER, PDH, DATIM):
        # Cascaded RefInds + Cascaded DEs/DDEs + Non-cascaded COCs for current period
        for period_key in datim_references:
            import_list += datim_references[period_key]
        for period_key in pdh_references:
            import_list += pdh_references[period_key]
        for period_key in mer_references:
            import_list += mer_references[period_key]

    # 3. ALL REPOSITORY VERSIONS
    # 3.a. Primary Source Version
    import_list.append(msp.get_repo_version_json(
        owner_id=settings.MSP_ORG_ID, repo_id=settings.MSP_SOURCE_ID, version_id='v1.0',
        description='Auto-generated release'))

    # 3.b. Codelist Collection Versions
    for codelist in codelists:
        import_list.append(msp.get_repo_version_json(
            owner_id=settings.MSP_ORG_ID, repo_type='Collection', repo_id=codelist['id'],
            version_id='v1.0', description='Auto-generated release'))

    # 3.c. Metadata Source Collection Versions
    for collection_id in msp.COLLECTION_NAMES:
        if collection_id != msp.COLLECTION_NAME_MER:
            import_list.append(msp.get_repo_version_json(
                owner_id=settings.MSP_ORG_ID, repo_type='Collection', repo_id=collection_id,
                version_id='v1.0', description='Auto-generated release'))
        for period in settings.OUTPUT_PERIODS:
            period_collection_id = '%s-%s' % (collection_id, period)
            import_list.append(msp.get_repo_version_json(
                owner_id=settings.MSP_ORG_ID, repo_type='Collection', repo_id=period_collection_id,
                version_id='v1.0', description='Auto-generated release'))

    # 4. CLEANUP: De-duplicate import list without changing order & leaving 1st occurrence in place
    import_list_dedup = msp.dedup_list_of_dicts(import_list)

    # Output the list
    for resource in import_list_dedup:
        print json.dumps(resource)


# CODELIST JSON
# Optionally output all codelists as JSON intended for populating filters in MSP
if settings.OUTPUT_CODELIST_JSON:
    print json.dumps(msp.get_codelists_formatted_for_display(codelists=codelists))
