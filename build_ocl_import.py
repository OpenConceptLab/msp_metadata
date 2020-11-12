"""
Prepares an OCL bulk import file for MER metadata

COMPLETED:
* Retrieve & save DATIM codelists from SqlQuery API instead of dataset membership
* Populate DE 'codelists' from codelist_references instead of DE datasets
    * 1308:build_concept_from_datim_de
    * 633:get_de_periods_from_codelist_collections

TODO:
* Remove codelist_collections['extras']['dhis2_codelist'] before saving
* Update how codelist references are generated:
    * 798:msp.build_codelist_references -- this refers to COC IDs and new custom mapping IDs,
      which the method is not yet able to validate
* Ensure that the switch to custom mapping IDs is valid throughout the script


1. Add missing reference indicator codes and mappings to the DATIM indicators
2. Group data elements by MER, SIMS, EA, Other?
3. Include # of derivation rules in report

TOP PRIORITY FOR ROUND 9 IMPORT:
Exclude COCs from Codelists based on the ZenDesk queries that are used to filter datasets (#742)
Update to latest content:
DATIM content was exported from DHIS2 on 2020-06-11 (data elements, datim indicators, COCs, dataset)
PDH Derived Data Elements extract from 2020-01-05
Codelists spreadsheet from 2020-06-16
MER Reference Indicators spreadsheet from 2020-06-11
ADDITIONAL ITEMS FOR CONSIDERATION: (for round 9 or other future imports)
Implement hierarchical reference indicator model to support PDH-specific codes and all variants, eg. _POS, _NEG, etc. (See notes in #647)
Implement Data Element Groups -- most DATIM DEs are SIMS or EA (Expenditure Analysis), but there are no filters or logical groups to work with SIMS and EA
Ensure overlap of 127 data element UIDs between DATIM & PDH is modeled correctly in OCL. These are DATIM DEs that were replaced by PDH DDEs.
Review the addition of Reporting Frequency attribute from Reference Indicators to relevant DATIM/PDH data elements and address any issues. This was included in Round 8 but needs to be reviewed.
Include Mechanism name in DATIM indicator formulas with 3 UIDs
Import Mechanisms
Review handling of fiscal year between targets and results (#667)
"""
import json
import ocldev.oclresourcelist
import ocldev.oclconstants
import settings
import msp


# LOAD METADATA SOURCES
# 1. ref_indicator_concepts -- OclJsonResourceList of FY16-20 reference indicator concepts
# 2. sorted_ref_indicator_codes -- De-duped list of indicator codes sorted by length descending
# 3. coc_concepts -- OclJsonResourceList of DATIM category option combo (COC) concepts
# 4. codelist_collections -- OclJsonResourceList of FY16-20 Codelist Collections
# 5. de_concepts -- OclJsonResourceList of DATIM Data Element (DE) concepts
# 6. datim_indicator_concepts -- OclJsonResourceList DATIM Indicator concepts
# 7. pdh_dde_concepts -- OclJsonResourceList of PDH Derived Data Element (DDE) concepts
ref_indicator_concepts = msp.load_ref_indicator_concepts(
    filenames=settings.FILENAME_MER_REFERENCE_INDICATORS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)
sorted_ref_indicator_codes = msp.get_sorted_unique_indicator_codes(
    ref_indicator_concepts=ref_indicator_concepts)
coc_concepts = msp.load_datim_coc_concepts(
    filename=settings.FILENAME_DATIM_COCS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)

# JP: Loading from file instead because no need to retrieve every time this is run
# codelist_collections = msp.load_codelist_collections(
#     filename=settings.FILENAME_DATIM_CODELISTS, org_id=settings.MSP_ORG_ID)
codelist_collections = msp.load_codelist_collections_with_exports_from_file(
    filename=settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT)

de_concepts = msp.load_datim_data_elements(
    filename=settings.FILENAME_DATIM_DATA_ELEMENTS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    codelist_collections=codelist_collections, ref_indicator_concepts=ref_indicator_concepts)
datim_indicator_concepts = msp.load_datim_indicators(
    filename=settings.FILENAME_DATIM_INDICATORS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID, de_concepts=de_concepts, coc_concepts=coc_concepts,
    sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    ref_indicator_concepts=ref_indicator_concepts)
pdh_dde_concepts = msp.load_pdh_dde_concepts(
    filename=settings.FILENAME_PDH, num_run_sequences=settings.PDH_NUM_RUN_SEQUENCES,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
    sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    ref_indicator_concepts=ref_indicator_concepts,
    pdh_rule_period_end_year=settings.PDH_RULE_PERIOD_END_YEAR)


# GENERATE MAPPINGS & LINKAGES
# 1. map_ref_indicator_to_de -- Dictionary with ref indicator URL as key, list of DE URLs as value
# 2. map_ref_indicator_to_pdh_dde -- Dict with ref indicator URL as key & list of DDE URLs as value
# 3. map_ref_indicator_to_datim_indicator - Dict with ref indicator URL as key & list of DATIM
#       indicator URLs as value
# 4. map_de_to_coc -- Dictionary with DE URL as key and list of COC URLs as value
# 5. map_pdh_dde_to_coc -- Dictionary with DDE URL as key and list of COC URLs as value
# 6. map_codelist_to_de_to_coc -- Dictionary with Codelist ID as top-level key, DE URL as
#       2nd-level key, and list of COC URLs as value
# 7. de_version_linkages - Dictionary with version-less DE root code as key, list of dicts
#       describing linked DEs as value (url, DE code, version number, sort order)
# 8. map_de_version_linkages - Dictionary with DE URL as key, list of replaced DE URLs as value
# 9. map_dde_source_linkages - Dictionary with DE URL as key, list of source DE URLs as value
map_ref_indicator_to_de = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=de_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_ref_indicator_to_pdh_dde = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=pdh_dde_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_ref_indicator_to_datim_indicator = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=datim_indicator_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_de_to_coc = msp.build_de_to_coc_maps(
    de_concepts=de_concepts, coc_concepts=coc_concepts,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_pdh_dde_to_coc = msp.build_pdh_dde_to_coc_maps(
    pdh_dde_concepts=pdh_dde_concepts, coc_concepts=coc_concepts)
map_codelist_to_de_to_coc = msp.build_codelist_to_de_map(
    codelist_collections=codelist_collections, de_concepts=de_concepts,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
de_version_linkages = msp.build_linkages_de_version(de_concepts=de_concepts)
de_version_linkages.update(msp.build_linkages_dde_version(pdh_dde_concepts=pdh_dde_concepts))
map_de_version_linkages = msp.build_maps_from_de_linkages(de_linkages=de_version_linkages)
map_dde_source_linkages = msp.build_linkages_source_de(
    pdh_dde_concepts=pdh_dde_concepts, owner_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID)


# GENERATE VALUE SET REFERENCES
# 1. ref_indicator_references -- List of ref indicator references grouped by period
# 2. codelist_references -- List of OCL-formatted reference batches to
#       all DEs, COCs, and mappings between them
ref_indicator_references = msp.build_ref_indicator_references(
    ref_indicator_concepts=ref_indicator_concepts, org_id=settings.MSP_ORG_ID)
codelist_references = msp.build_codelist_references(
    map_codelist_to_de_to_coc=map_codelist_to_de_to_coc,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
    codelist_collections=codelist_collections)


# Summarize metadata loaded
if settings.VERBOSITY:
    msp.display_input_metadata_summary(
        verbosity=settings.VERBOSITY,
        input_periods=settings.MSP_INPUT_PERIODS,
        ref_indicator_concepts=ref_indicator_concepts,
        sorted_ref_indicator_codes=sorted_ref_indicator_codes,
        coc_concepts=coc_concepts,
        codelist_collections=codelist_collections,
        de_concepts=de_concepts,
        map_codelist_to_de_to_coc=map_codelist_to_de_to_coc,
        datim_indicator_concepts=datim_indicator_concepts,
        pdh_dde_concepts=pdh_dde_concepts,
        map_ref_indicator_to_de=map_ref_indicator_to_de,
        map_ref_indicator_to_pdh_dde=map_ref_indicator_to_pdh_dde,
        map_ref_indicator_to_datim_indicator=map_ref_indicator_to_datim_indicator,
        map_de_to_coc=map_de_to_coc, map_pdh_dde_to_coc=map_pdh_dde_to_coc,
        de_version_linkages=de_version_linkages, map_de_version_linkages=map_de_version_linkages,
        map_dde_source_linkages=map_dde_source_linkages,
        ref_indicator_references=ref_indicator_references,
        codelist_references=codelist_references)


# OUTPUT OCL-FORMATTED JSON
#  1. ALL REPOSITORIES
#      a. Primary Org and Source (eg /orgs/PEPFAR/sources/MER/)
#      b. Codelist collections
#      c. Reference indicator collections for each period (eg MER_Reference_Indicators_FY18)
#  2. FOR EACH PERIOD...
#      a. Reference indicator concepts for primary source for current period
#      b. Reference indicator period references for current period
#  3. RESOURCES FOR PRIMARY SOURCE
#      a. DATIM/PDH data elements, DATIM COCs, and DATIM indicators
#      b. Mappings
#  4. CODELIST REFERENCES
#  5. LINKAGES: Version Replacement and Source/Derivation Linkages Mappings
#  6. ALL REPOSITORY VERSIONS
#      a. Primary Source Version
#      b. Codelist Collection Versions
#      c. Reference Indicator Period Collection Versions
#  7. CLEANUP: De-duplicate import list without changing order & leaving 1st occurrence in place
if settings.OUTPUT_OCL_FORMATTED_JSON:
    import_list = ocldev.oclresourcelist.OclJsonResourceList()

    # 1. ALL REPOSITORIES
    # 1.a. Primary Org and Source (eg /orgs/PEPFAR/sources/MER/)
    if settings.IMPORT_SCRIPT_OPTION_ORG:
        import_list.append(msp.get_new_org_json(org_id=settings.MSP_ORG_ID))
    if settings.IMPORT_SCRIPT_OPTION_SOURCE:
        import_list.append(msp.get_primary_source(
            org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID))

    # 1.b Codelist collections
    if settings.IMPORT_SCRIPT_OPTION_CODELIST_COLLECTIONS:
        import_list += codelist_collections

    # 1.c. Reference Indicator collections
    if settings.IMPORT_SCRIPT_OPTION_REF_INDICATOR_COLLECTIONS:
        for period in settings.OUTPUT_PERIODS:
            period_collection_id = msp.COLLECTION_NAME_MER_REFERENCE_INDICATORS % period
            import_list.append(msp.get_new_repo_json(
                owner_id=settings.MSP_ORG_ID,
                repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION,
                repo_id=period_collection_id,
                name=period_collection_id,
                full_name=period_collection_id))

    # 2. FOR EACH PERIOD...
    for period in settings.OUTPUT_PERIODS:
        # Reference indicators by period
        import_list += ref_indicator_concepts.get_resources(
            custom_attrs={msp.ATTR_PERIOD: period})

        # Ref indicator collection references by period
        if settings.IMPORT_SCRIPT_OPTION_REF_INDICATOR_COLLECTIONS:
            if period in ref_indicator_references:
                import_list.append(ref_indicator_references[period])

    # 3. RESOURCES FOR PRIMARY SOURCE
    # 3.a. DATIM/PDH data elements, DATIM COCs, and DATIM indicators
    import_list.append(de_concepts)
    import_list.append(pdh_dde_concepts)
    import_list.append(coc_concepts)
    import_list.append(datim_indicator_concepts)

    # 3.b. Mappings
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_ref_indicator_to_de, map_type=msp.MSP_MAP_TYPE_REF_INDICATOR_TO_DE,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_REFIND_DE))
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_ref_indicator_to_pdh_dde, map_type=msp.MSP_MAP_TYPE_REF_INDICATOR_TO_DE,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_REFIND_DE))
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_ref_indicator_to_datim_indicator,
        map_type=msp.MSP_MAP_TYPE_REF_INDICATOR_TO_DATIM_INDICATOR,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_REFIND_IND))
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_de_to_coc, map_type=msp.MSP_MAP_TYPE_DE_TO_COC,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_DE_COC))
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_pdh_dde_to_coc, map_type=msp.MSP_MAP_TYPE_DE_TO_COC,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_DE_COC))

    # 4. CODELIST REFERENCES
    import_list += codelist_references

    # 5. LINKAGES: Version Replacement and Source/Derivation Linkages Mappings
    import_list += msp.build_ocl_mappings(
        map_dict=map_de_version_linkages, map_type=msp.MSP_MAP_TYPE_REPLACES,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
    import_list += msp.build_ocl_mappings(
        map_dict=map_dde_source_linkages, map_type=msp.MSP_MAP_TYPE_DERIVED_FROM,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)

    # 6. REPOSITORY VERSIONS
    # 6.a. Primary Source Version
    import_list.append(msp.get_repo_version_json(
        owner_id=settings.MSP_ORG_ID,
        repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_SOURCE,
        repo_id=settings.MSP_SOURCE_ID,
        version_id='v1.0', description='Auto-generated release'))

    # 6.b. Codelist Collection Versions
    for codelist in codelist_collections:
        import_list.append(msp.get_repo_version_json(
            owner_id=settings.MSP_ORG_ID,
            repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION,
            repo_id=codelist['id'],
            version_id='v1.0', description='Auto-generated release'))

    # 6.c. Reference Indicator Period Collection Versions
    if settings.IMPORT_SCRIPT_OPTION_REF_INDICATOR_COLLECTIONS:
        for period in settings.OUTPUT_PERIODS:
            period_collection_id = msp.COLLECTION_NAME_MER_REFERENCE_INDICATORS % period
            import_list.append(msp.get_repo_version_json(
                owner_id=settings.MSP_ORG_ID,
                repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION,
                repo_id=period_collection_id,
                version_id='v1.0', description='Auto-generated release'))

    # Summarize import list
    if settings.VERBOSITY:
        print 'SUMMARY OF FINAL IMPORT LIST:'
        print '  Breakdown by resource type:'
        for (key, count) in import_list.summarize(core_attr_key='type').items():
            print '    %s: %s' % (key, count)

    # 4. CLEANUP: De-duplicate import list without changing order & leaving 1st occurrence in place
    import_list_dedup = msp.dedup_list_of_dicts(import_list._resources)

    # Output import list
    if import_list:
        chunked_import_lists = import_list.chunk(settings.IMPORT_LIST_CHUNK_SIZE)
        iteration = 0
        for chunked_import_list in chunked_import_lists:
            iteration += 1
            output_filename = settings.OUTPUT_FILENAME % str(iteration)
            with open(output_filename, 'wb') as output_file:
                for resource in chunked_import_list:
                    output_file.write(json.dumps(resource))
                    output_file.write('\n')
