"""
Prepares an OCL bulk import file for MER metadata

COMPLETED FOR ROUND 10:
* Replaced all occurences of PDH with IHUB
* Modified settings.py to include FY21
* Removed chunking of bulk import file now that oclapi2 can handle large bulk imports
* Applied MSP_ORG_ID to codelist file
* Update to latest content:
    * FY21 MER v2.5 Reference Indicators spreadsheet
    * FY21 Codelists spreadsheet
    * DATIM metadata exports retreived 2021-01-06 (DEs, indicators, COCs, dataset)
    * IHUB Derived Data Elements extract from 2021-01-08

TODO FOR ROUND 10:
* Remove codelist_collections['extras']['dhis2_codelist'] before saving
* Update how codelist references are generated:
    * 798:msp.build_codelist_references -- this refers to COC IDs and new custom mapping IDs,
      which the method is not yet able to validate
* Add missing reference indicator codes and mappings to the DATIM indicators
* Include # of derivation rules in report
* Review the addition of Reporting Frequency attribute from Reference Indicators to relevant
  DATIM/IHUB DEs and address any issues. This was included in Rnd8 but needs to be reviewed.
* Include Mechanism name in DATIM indicator formulas with 3 UIDs
"""
import datetime
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
# 7. ihub_dde_concepts -- OclJsonResourceList of IHUB Derived Data Element (DDE) concepts
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
    filename=settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT, org_id=settings.MSP_ORG_ID)

de_concepts = msp.load_datim_data_elements(
    filename=settings.FILENAME_DATIM_DATA_ELEMENTS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    codelist_collections=codelist_collections, ref_indicator_concepts=ref_indicator_concepts)
datim_indicator_concepts = msp.load_datim_indicators(
    filename=settings.FILENAME_DATIM_INDICATORS, org_id=settings.MSP_ORG_ID,
    source_id=settings.MSP_SOURCE_ID, de_concepts=de_concepts, coc_concepts=coc_concepts,
    sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    ref_indicator_concepts=ref_indicator_concepts)

ihub_dde_concepts = msp.load_ihub_dde_concepts(
    filename=settings.FILENAME_IHUB, num_run_sequences=settings.IHUB_NUM_RUN_SEQUENCES,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
    sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    ref_indicator_concepts=ref_indicator_concepts,
    ihub_rule_period_end_year=settings.IHUB_RULE_PERIOD_END_YEAR)


# GENERATE MAPPINGS & LINKAGES
# 1. map_ref_indicator_to_de -- Dictionary with ref indicator URL as key, list of DE URLs as value
# 2. map_ref_indicator_to_ihub_dde -- Dict with ref indicator URL as key & list of DDE URLs as value
# 3. map_ref_indicator_to_datim_indicator - Dict with ref indicator URL as key & list of DATIM
#       indicator URLs as value
# 4. map_de_to_coc -- Dictionary with DE URL as key and list of COC URLs as value
# 5. map_ihub_dde_to_coc -- Dictionary with DDE URL as key and list of COC URLs as value
# 6. map_codelist_to_de_to_coc -- Dictionary with Codelist ID as top-level key, DE URL as
#       2nd-level key, and list of COC URLs as value
# 7. de_version_linkages - Dictionary with version-less DE root code as key, list of dicts
#       describing linked DEs as value (url, DE code, version number, sort order)
# 8. map_de_version_linkages - Dictionary with DE URL as key, list of replaced DE URLs as value
# 9. map_dde_source_linkages - Dictionary with DE URL as key, list of source DE URLs as value
map_ref_indicator_to_de = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=de_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_ref_indicator_to_ihub_dde = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=ihub_dde_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_ref_indicator_to_datim_indicator = msp.build_ref_indicator_to_child_resource_maps(
    child_concepts=datim_indicator_concepts, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_de_to_coc = msp.build_de_to_coc_maps(
    de_concepts=de_concepts, coc_concepts=coc_concepts,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
map_ihub_dde_to_coc = msp.build_ihub_dde_to_coc_maps(
    ihub_dde_concepts=ihub_dde_concepts, coc_concepts=coc_concepts)
map_codelist_to_de_to_coc = msp.build_codelist_to_de_map(
    codelist_collections=codelist_collections, de_concepts=de_concepts,
    org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID)
de_version_linkages = msp.build_linkages_de_version(de_concepts=de_concepts)
de_version_linkages.update(msp.build_linkages_dde_version(ihub_dde_concepts=ihub_dde_concepts))
map_de_version_linkages = msp.build_maps_from_de_linkages(de_linkages=de_version_linkages)
map_dde_source_linkages = msp.build_linkages_source_de(
    ihub_dde_concepts=ihub_dde_concepts, owner_id=settings.MSP_ORG_ID,
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
        ihub_dde_concepts=ihub_dde_concepts,
        map_ref_indicator_to_de=map_ref_indicator_to_de,
        map_ref_indicator_to_ihub_dde=map_ref_indicator_to_ihub_dde,
        map_ref_indicator_to_datim_indicator=map_ref_indicator_to_datim_indicator,
        map_de_to_coc=map_de_to_coc, map_ihub_dde_to_coc=map_ihub_dde_to_coc,
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
#      a. DATIM/IHUB data elements, DATIM COCs, and DATIM indicators
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
    import_list.append(msp.get_new_org_json(org_id=settings.MSP_ORG_ID))
    import_list.append(msp.get_primary_source(
        org_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID))

    # 1.b Codelist collections
    import_list += codelist_collections

    # 1.c. Reference Indicator collections
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
        if period in ref_indicator_references:
            import_list.append(ref_indicator_references[period])

    # 3. RESOURCES FOR PRIMARY SOURCE
    # 3.a. DATIM/IHUB data elements, DATIM COCs, and DATIM indicators
    import_list.append(de_concepts)
    import_list.append(ihub_dde_concepts)
    import_list.append(coc_concepts)
    import_list.append(datim_indicator_concepts)

    # 3.b. Mappings
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_ref_indicator_to_de, map_type=msp.MSP_MAP_TYPE_REF_INDICATOR_TO_DE,
        owner_id=settings.MSP_ORG_ID, source_id=settings.MSP_SOURCE_ID,
        do_generate_mapping_id=True, id_format=msp.MSP_MAP_ID_FORMAT_REFIND_DE))
    import_list.append(msp.build_ocl_mappings(
        map_dict=map_ref_indicator_to_ihub_dde, map_type=msp.MSP_MAP_TYPE_REF_INDICATOR_TO_DE,
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
        map_dict=map_ihub_dde_to_coc, map_type=msp.MSP_MAP_TYPE_DE_TO_COC,
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
    for period in settings.OUTPUT_PERIODS:
        period_collection_id = msp.COLLECTION_NAME_MER_REFERENCE_INDICATORS % period
        import_list.append(msp.get_repo_version_json(
            owner_id=settings.MSP_ORG_ID,
            repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION,
            repo_id=period_collection_id,
            version_id='v1.0', description='Auto-generated release'))

    # 4. CLEANUP: De-duplicate import list without changing order & leaving 1st occurrence in place
    import_list_dedup = msp.dedup_list_of_dicts(import_list._resources)

    # Summarize import list (after deduplication)
    if settings.VERBOSITY:
        summarize_import_list(import_list)

    # Output import list
    if import_list:
        output_filename = settings.OUTPUT_FILENAME % (
            settings.MSP_ORG_ID, datetime.datetime.today().strftime('%Y%m%d'))
        with open(output_filename, 'wb') as output_file:
            for resource in import_list:
                output_file.write(json.dumps(resource))
                output_file.write('\n')
