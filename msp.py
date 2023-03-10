"""
Common functionality used in the MSP ETL scripts used to prepare and import metadata from
MER guidance, DATIM, iHUB, and related systems.

Update these for each update of MSP:
1. Run get_codelist_collections_formatted_for_display to provide MSP with codelist definitions

"""
import json
import csv
import datetime
import re
import requests
import ocldev.oclcsvtojsonconverter
import ocldev.oclconstants
import ocldev.oclresourcelist


# Constants for OCL mappings
MSP_MAP_TYPE_REF_INDICATOR_TO_DE = 'Has Data Element'
MSP_MAP_TYPE_REF_INDICATOR_TO_DATIM_INDICATOR = 'Has DATIM Indicator'
MSP_MAP_TYPE_DE_TO_COC = 'Has Option'
MSP_MAP_TYPE_REPLACES = 'Replaces'
MSP_MAP_TYPE_DERIVED_FROM = 'Derived From'
MSP_MAP_TYPES = [
    MSP_MAP_TYPE_REF_INDICATOR_TO_DE,
    MSP_MAP_TYPE_DE_TO_COC,
    MSP_MAP_TYPE_REPLACES,
]
MSP_MAP_ID_FORMAT_DE_COC = 'MAP_DE_COC_%s_%s'
MSP_MAP_ID_FORMAT_REFIND_DE = 'MAP_REFIND_DE_%s_%s'
MSP_MAP_ID_FORMAT_REFIND_IND = 'MAP_REFIND_IND_%s_%s'

# Constants for iHUB source spreadsheet
IHUB_COLUMN_INDICATOR = 'indicator'
IHUB_COLUMN_SOURCE_KEY = 'source_srgt_key'
IHUB_COLUMN_DISAGGREGATE = 'disaggregate'
IHUB_COLUMN_STANDARDIZED_DISAGGREGATE = 'standardized_disaggregate'
IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID = 'derived_data_element_uid'
IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME = 'derived_data_element_name'
IHUB_COLUMN_DERIVED_COC_UID = 'derived_category_option_combo'
IHUB_COLUMN_DERIVED_COC_NAME = 'derived_category_option_combo_name'
IHUB_COLUMN_SOURCE_DATA_ELEMENT_UID = 'source_data_element_uid'
IHUB_COLUMN_SOURCE_DATA_ELEMENT_NAME = 'source_data_element_name'
IHUB_COLUMN_SOURCE_DISAGGREGATE = 'source_disaggregate'
IHUB_COLUMN_SOURCE_COC_UID = 'source_category_option_combo_uid'
IHUB_COLUMN_SOURCE_COC_NAME = 'source_category_option_combo_name'
IHUB_COLUMN_RULE_BEGIN_PERIOD = 'rule_begin_period'
IHUB_COLUMN_RULE_END_PERIOD = 'rule_end_period'
IHUB_COLUMN_ADD_OR_SUBTRACT = 'add_or_subtract'
IHUB_COLUMN_RESULT_TARGET = 'result_target'
IHUB_COLUMN_RUN_SEQUENCE = 'derived_level_run_seq'
IHUB_COLUMN_RULE_ID = 'rule_id'
IHUB_COLUMNS = [
    IHUB_COLUMN_INDICATOR,
    IHUB_COLUMN_SOURCE_KEY,
    IHUB_COLUMN_DISAGGREGATE,
    IHUB_COLUMN_STANDARDIZED_DISAGGREGATE,
    IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID,
    IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME,
    IHUB_COLUMN_DERIVED_COC_UID,
    IHUB_COLUMN_DERIVED_COC_NAME,
    IHUB_COLUMN_SOURCE_DATA_ELEMENT_UID,
    IHUB_COLUMN_SOURCE_DATA_ELEMENT_NAME,
    IHUB_COLUMN_SOURCE_DISAGGREGATE,
    IHUB_COLUMN_SOURCE_COC_UID,
    IHUB_COLUMN_SOURCE_COC_NAME,
    IHUB_COLUMN_RULE_BEGIN_PERIOD,
    IHUB_COLUMN_RULE_END_PERIOD,
    IHUB_COLUMN_ADD_OR_SUBTRACT,
    IHUB_COLUMN_RESULT_TARGET,
    IHUB_COLUMN_RUN_SEQUENCE,
    IHUB_COLUMN_RULE_ID,
]
IHUB_COLUMN_SOURCE_KEY_DATIM = '1'
IHUB_COLUMN_SOURCE_KEY_IHUB = '2'

# Constants for DATIM code list columns
DATIM_CODELIST_COLUMN_DATASET = 0
DATIM_CODELIST_COLUMN_DATA_ELEMENT_NAME = 1
DATIM_CODELIST_COLUMN_DATA_ELEMENT_SHORT_NAME = 2
DATIM_CODELIST_COLUMN_DATA_ELEMENT_CODE = 3
DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID = 4
DATIM_CODELIST_COLUMN_DATA_ELEMENT_DESCRIPTION = 5
DATIM_CODELIST_COLUMN_COC_NAME = 6
DATIM_CODELIST_COLUMN_COC_CODE = 7
DATIM_CODELIST_COLUMN_COC_UID = 8
DATIM_CODELIST_COLUMNS = [
    DATIM_CODELIST_COLUMN_DATASET,
    DATIM_CODELIST_COLUMN_DATA_ELEMENT_NAME,
    DATIM_CODELIST_COLUMN_DATA_ELEMENT_SHORT_NAME,
    DATIM_CODELIST_COLUMN_DATA_ELEMENT_CODE,
    DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID,
    DATIM_CODELIST_COLUMN_DATA_ELEMENT_DESCRIPTION,
    DATIM_CODELIST_COLUMN_COC_NAME,
    DATIM_CODELIST_COLUMN_COC_CODE,
    DATIM_CODELIST_COLUMN_COC_UID,
]

# Constants for MSP collections -- %s is replaced by period (eg FY19)
COLLECTION_NAME_MER_REFERENCE_INDICATORS = 'MER_REFERENCE_INDICATORS_%s'
COLLECTION_NAME_MER_FULL = 'MER_%s'

# Support type constants
SUPPORT_TYPE_CODES = {
    'TA': 'Technical Assistance',
    'DSD': 'Direct Service Delivery',
    'CS': 'Central Support'
}

# Constants for data element custom attributes
ATTR_APPLICABLE_PERIODS = 'Applicable Periods'
ATTR_PERIOD = 'Period'
ATTR_REPORTING_FREQUENCY = 'Reporting frequency'
ATTR_RESULT_TARGET = 'resultTarget'
ATTR_CODELISTS = 'codelists'
ATTR_PEPFAR_SUPPORT_TYPE = 'pepfarSupportType'
ATTR_NUMERATOR_DENOMINATOR_TYPE = 'numeratorDenominator'
ATTR_DOMAIN_TYPE = 'domainType'
ATTR_VALUE_TYPE = 'valueType'
ATTR_AGGREGATION_TYPE = 'aggregationType'
ATTR_STRUCTURED_DATASET = 'Structured Dataset'

# Mapping between periods and terms that appear in DATIM indicator names (case-insensitive)
MAP_PERIOD_TO_INDICATOR_TERMS = {
    "FY22": ["FY22", "COP21", "COP 21"],
    "FY21": ["FY21", "2021", "WAD21", "COP20"],
    "FY20": ["FY20", "2020", "WAD20", "FY17-20", "COP19"],
    "FY19": ["FY19", "2019", "WAD19", "FY17-20", "COP18"],
    "FY18": ["FY18", "2018", "WAD18", "FY16-18", "FY17-20", "COP17"],
    "FY17": ["FY17", "2017", "WAD17", "FY16-18", "FY17-20", "COP16"],
    "FY16": ["FY16", "2016", "WAD16", "FY16-18", "COP15"],
}


def display_resource_list_summaries(resource_list, summary_dict):
    """ Outputs a summary of a resource list to stdout """
    for (custom_attr_key, summary_dict_title) in summary_dict.items():
        print('    Breakdown by %s:' % summary_dict_title)
        for (key, count) in resource_list.summarize(custom_attr_key=custom_attr_key).items():
            print('      %s: %s' % (key, count))


def display_input_metadata_summary(verbosity=1, input_periods=None, ref_indicator_concepts=None,
                                   sorted_ref_indicator_codes=None, coc_concepts=None,
                                   codelist_collections=None, de_concepts=None,
                                   map_codelist_to_de_to_coc=None, datim_indicator_concepts=None,
                                   ihub_dde_concepts=None, map_ref_indicator_to_de=None,
                                   map_ref_indicator_to_ihub_dde=None,
                                   map_ref_indicator_to_datim_indicator=None,
                                   map_de_to_coc=None, map_ihub_dde_to_coc=None,
                                   de_version_linkages=None, map_de_version_linkages=None,
                                   map_dde_source_linkages=None,
                                   ref_indicator_references=None, codelist_references=None):
    """ Displays summary of the loaded metadata """
    print('MSP Metadata Statistics %s\n' % datetime.datetime.now().strftime("%Y-%m-%d"))
    print('METADATA SOURCES:')

    # Input periods
    print('  Input Periods:', input_periods)

    # Reference Indicators
    if ref_indicator_concepts and sorted_ref_indicator_codes:
        print('  MER Reference Indicators (FY16-20):',)
        print('%s unique reference indicator codes, %s total definitions' % (
            len(ref_indicator_concepts), len(sorted_ref_indicator_codes)))
        print('    Breakdown by Indicator Code:')
        for ref_indicator_code in sorted(sorted_ref_indicator_codes):
            print('      %s: ' % ref_indicator_code)
            print('        Periods:', ', '.join(ref_indicator_concepts.get_resources(
                            core_attrs={'id': ref_indicator_code}).summarize(
                                custom_attr_key=ATTR_PERIOD).keys()))
            ref_indicator_concept = ref_indicator_concepts.get_resource(
                core_attrs={'type': 'Concept', 'id': ref_indicator_code})
            if ref_indicator_concept:
                if ref_indicator_concept['__url'] in map_ref_indicator_to_de:
                    print('        Mapped DATIM data elements: %s' % (
                        len(map_ref_indicator_to_de[ref_indicator_concept['__url']])))
                if ref_indicator_concept['__url'] in map_ref_indicator_to_ihub_dde:
                    print('        Mapped iHUB derived data elements: %s' % (
                        len(map_ref_indicator_to_ihub_dde[ref_indicator_concept['__url']])))
                if ref_indicator_concept['__url'] in map_ref_indicator_to_datim_indicator:
                    print('        Mapped DATIM indicators: %s' % (
                        len(map_ref_indicator_to_datim_indicator[ref_indicator_concept['__url']])))
        print('    Breakdown by Period:')
        for (period, count) in ref_indicator_concepts.summarize(
                custom_attr_key=ATTR_PERIOD).items():
            print('      %s: %s' % (period, count))
        print('    Summary of Reference Indicator Mappings:')
        print('      Mappings to DATIM Data Elements (DE): ',)
        print('%s reference indicators with %s unique DE mappings' % (
                    get_dict_child_counts(map_ref_indicator_to_de)))
        print('      Mappings to iHUB Derived Data Elements (DDE): ',)
        print('%s reference indicators with %s unique DDE mappings' % (
                    get_dict_child_counts(map_ref_indicator_to_ihub_dde)))
        print('      Mappings to DATIM Indicators: ',)
        print('%s reference indicators with %s unique DATIM indicator mappings\n' % (
                    get_dict_child_counts(map_ref_indicator_to_datim_indicator)))

    # Codelist collections
    if codelist_collections:
        print('  DATIM Code Lists (FY16-20):', len(codelist_collections))
        print('    Breakdown by Period: (Note some codelists span multiple periods)')
        for period in input_periods:
            period_codelist_collections = ocldev.oclresourcelist.OclJsonResourceList()
            for codelist in codelist_collections:
                if period in codelist['extras'][ATTR_APPLICABLE_PERIODS]:
                    period_codelist_collections.append(codelist)
            print('      %s Code Lists: %s' % (period, len(period_codelist_collections)))
            if verbosity >= 2:
                for result_target_type in ['Result', 'Target']:
                    filtered_codelists = period_codelist_collections.get_resources(
                        custom_attrs={ATTR_RESULT_TARGET: result_target_type})
                    if filtered_codelists:
                        print('        %s: %s' % (result_target_type, len(filtered_codelists)))
                    else:
                        print('        %s: None' % (result_target_type))
                    for codelist in filtered_codelists:
                        if codelist['external_id'] in map_codelist_to_de_to_coc:
                            print('          %s: %s data elements' % (
                                codelist['name'],
                                len(map_codelist_to_de_to_coc[codelist['external_id']])))
                        else:
                            print('          %s' % codelist['name'])

    # DATIM data element concepts
    if de_concepts:
        de_concepts_summary_dict = {
            ATTR_RESULT_TARGET: 'Result/Target',
            ATTR_DOMAIN_TYPE: 'Domain Type',
            ATTR_NUMERATOR_DENOMINATOR_TYPE: 'Numerator/Denominator',
            ATTR_PEPFAR_SUPPORT_TYPE: 'PEPFAR Support Type',
            ATTR_REPORTING_FREQUENCY: 'Reporting Frequency'
        }
        print('  DATIM Data Elements (All):', len(de_concepts))
        print('    Breakdown by period (via codelists):')
        for (period, count) in summarize_applicable_periods_from_concepts(de_concepts).items():
            print('      %s: %s' % (period, count))
        display_resource_list_summaries(de_concepts, de_concepts_summary_dict)

    # iHUB derived data element concepts
    if ihub_dde_concepts:
        ihub_dde_concepts_summary_dict = {
            ATTR_RESULT_TARGET: 'Result/Target',
            'standardized_disaggregate': 'Standardized Disaggregate',
            ATTR_NUMERATOR_DENOMINATOR_TYPE: 'Numerator/Denominator',
            ATTR_PEPFAR_SUPPORT_TYPE: 'PEPFAR Support Type',
            ATTR_REPORTING_FREQUENCY: 'Reporting Frequency'
        }
        print('  iHUB Derived Data Element (All):', len(ihub_dde_concepts))
        print('    Breakdown by period (via derivation rules):')
        for (period, count) in summarize_applicable_periods_from_concepts(
                ihub_dde_concepts).items():
            print('      %s: %s' % (period, count))
        display_resource_list_summaries(ihub_dde_concepts, ihub_dde_concepts_summary_dict)

    # Summary for DE version linkages
    print('\nRESULTS OF GENERATING LINKAGES BETWEEN DATA ELEMENTS:')
    print('  Data Element Version Links (DATIM and iHUB):')
    print('    %s DEs replaced %s DEs' % get_dict_child_counts(map_de_version_linkages))
    if verbosity >= 2:
        for de_code in de_version_linkages:
            print('      %s' % de_code)
            for de_version in de_version_linkages[de_code]:
                print('        %s: %s (%s)' % (
                    de_version['sort_order'], de_version['code'], de_version['url']))

    # Summary for DE source-derivation linkages
    print('\n  iHUB Data Element Source-Derivation Linkages:')
    print('    %s derived data elements linked to %s source data elements' % get_dict_child_counts(
        map_dde_source_linkages))
    print('    NOTE: Source-derivation linkages are defined between data elements only, not COCs')

    # COC concepts
    if coc_concepts:
        print('  DATIM COC concepts (All):', len(coc_concepts))
        if map_de_to_coc:
            print('    %s DATIM data elements with %s unique COC maps' % get_dict_child_counts(
                map_de_to_coc))
        if map_ihub_dde_to_coc:
            print('    %s iHUB DDEs with %s unique COC maps' % get_dict_child_counts(
                map_ihub_dde_to_coc))

    # DATIM indicator concepts
    if datim_indicator_concepts:
        datim_indicator_concepts_summary_dict = {
            ATTR_RESULT_TARGET: 'Result/Target',
            'annualized': 'Annualized',
            'dimensionItemType': 'dimensionItemType',
        }
        print('  DATIM Indicators (All):', len(datim_indicator_concepts))
        print('    Breakdown by period (via keywords in indicator names):')
        for (period, count) in summarize_applicable_periods_from_concepts(
                datim_indicator_concepts).items():
            print('      %s: %s' % (period, count))
        display_resource_list_summaries(
            datim_indicator_concepts, datim_indicator_concepts_summary_dict)

    # Display list of overlapping IDs between iHUB and DATIM data elements
    if ihub_dde_concepts:
        overlapping_de_concepts = {}
        for dde_concept in ihub_dde_concepts:
            de_concept = de_concepts.get_resource(core_attrs={'id': dde_concept['id']})
            if de_concept:
                overlapping_de_concepts[dde_concept['id']] = {
                    'ihub': dde_concept,
                    'datim': de_concept
                }
        if overlapping_de_concepts:
            print('  Overlapping DATIM/iHUB Data Elements: %s' % len(overlapping_de_concepts))
            for overlapping_de_concept_id in overlapping_de_concepts:
                overlapping_concept = overlapping_de_concepts[overlapping_de_concept_id]
                print('    [%s]\n      DATIM: %s -- %s\n      iHUB: %s -- %s' % (
                    overlapping_de_concept_id,
                    overlapping_concept['datim']['names'][0]['name'],
                    overlapping_concept['datim']['extras'].get(ATTR_APPLICABLE_PERIODS),
                    overlapping_concept['ihub']['names'][0]['name'],
                    overlapping_concept['ihub']['extras'].get(ATTR_APPLICABLE_PERIODS)))


def summarize_import_list(import_list):
    """ Output a summary of the final import list """
    print('\nSUMMARY OF FINAL IMPORT LIST:')
    print('  Breakdown by resource type:')
    for (key, count) in import_list.summarize(core_attr_key='type').items():
        print('    %s: %s' % (key, count))
        if key == 'Concept':
            concepts = import_list.get_resources(core_attrs={'type': 'Concept'})
            for (subresource_key, subresource_count) in concepts.summarize(
                    core_attr_key='concept_class').items():
                print('      %s: %s' % (subresource_key, subresource_count))
                if subresource_key == 'Data Element':
                    for (concept_key, value) in concepts.get_resources(core_attrs={'concept_class': 'Data Element'}).summarize(custom_attr_key='source').items():
                        print('        %s: %s' % (concept_key, value))
                elif subresource_key == 'Reference Indicator':
                    for (concept_key, value) in concepts.get_resources(core_attrs={'concept_class': 'Reference Indicator'}).summarize(custom_attr_key='Period').items():
                        print('        %s: %s' % (concept_key, value))
        elif key == 'Mapping':
            for (subresource_key, subresource_count) in import_list.get_resources(core_attrs={'type': 'Mapping'}).summarize(core_attr_key='map_type').items():
                print('      %s: %s' % (subresource_key, subresource_count))
        elif key == 'Collection':
            for collection in import_list.get_resources(core_attrs={'type': 'Collection'}):
                print('      %s' % collection['id'])


def count_reference_expressions(references):
    """
    Returns a count of the total number of expressions in the specified references.
    Used by summary display methods.
    """
    num_expressions = 0
    for reference in references:
        num_expressions += len(reference["data"]["expressions"])
    return num_expressions


def get_dict_child_counts(dict_to_be_counted):
    """
    Returns count of dict and count of all its children as a set.
    Used by summary display methods.
    """
    count_of_children = 0
    for dict_key in dict_to_be_counted:
        count_of_children += len(dict_to_be_counted[dict_key])
    return len(dict_to_be_counted), count_of_children


def get_new_org_json(org_id=''):
    """ Returns OCL-formatted JSON for the PEPFAR org """
    return {
        "website": "https://www.pepfar.gov/",
        "name": "The United States President's Emergency Plan for AIDS Relief",
        "public_access": "View",
        "company": "US Government",
        "type": "Organization",
        "id": org_id,
        "location": "Washington, DC, USA"
    }


def get_primary_source(org_id, source_id, canonical_url):
    """ Returns OCL-formatted JSON for the PEPFAR MER source """
    return get_new_repo_json(
        owner_id=org_id, repo_id=source_id, name="MER Source",
        full_name="DATIM Monitoring, Evaluation & Results Metadata",
        canonical_url="%s/CodeSystem/MER" % canonical_url)


def get_new_repo_json(owner_type='Organization', owner_id='', repo_type='Source', repo_id='',
                      name='', full_name='', repo_sub_type='Dictionary', default_locale='en',
                      public_access='View', supported_locales='en', canonical_url=''):
    """ Returns OCL-formatted JSON for a source """
    repo_json = {
        "name": name,
        "default_locale": default_locale,
        "short_code": repo_id,
        "%s_type" % repo_type.lower(): repo_sub_type,
        "full_name": full_name,
        "owner": owner_id,
        "public_access": public_access,
        "owner_type": owner_type,
        "type": repo_type,
        "id": repo_id,
        "supported_locales": supported_locales
    }
    if canonical_url:
        repo_json['canonical_url'] = canonical_url
    return repo_json


def load_datim_data_elements(filename='', org_id='', source_id='',
                             sorted_ref_indicator_codes=None, codelist_collections=None,
                             ref_indicator_concepts=None):
    """
    Load raw DHIS2-formatted DATIM data elements and return as OCL-formatted JSON resources.
    Note that COCs and datasets are included as attributes of each data element.
    """

    # Load raw DHIS2-formatted DATIM data elements
    with open(filename, 'rb') as input_file:
        raw_datim_de_all = json.load(input_file)

    # Convert to OCL-formatted JSON
    de_concepts = ocldev.oclresourcelist.OclJsonResourceList()
    for de_raw in raw_datim_de_all['dataElements']:
        de_concepts.append(build_concept_from_datim_de(
            de_raw, org_id, source_id, sorted_ref_indicator_codes, codelist_collections,
            ref_indicator_concepts))
    return de_concepts


def load_datim_coc_concepts(filename='', org_id='', source_id=''):
    """ Load and return DATIM categoryOptionCombos as OCL-formatted JSON concepts """

    # Load COCs as raw DHIS2-formatted JSON
    with open(filename, 'rb') as input_file:
        raw_datim_cocs = json.load(input_file)

    # Transform COCs to OCL-formatted JSON and return
    coc_concepts = []
    for coc_raw in raw_datim_cocs['categoryOptionCombos']:
        coc_concepts.append(build_concept_from_datim_coc(coc_raw, org_id, source_id))
    return ocldev.oclresourcelist.OclJsonResourceList(resources=coc_concepts)


def load_codelist_collections_with_exports_from_file(filename='', org_id=''):
    """
    Load codelist collections with their exports from the specified filename.
    This returns the same output as msp.load_codelist_collections and is designed to
    be used in conjunction with save_codelists_to_file.py.
    """
    with open(filename) as input_file:
        resources = ocldev.oclresourcelist.OclJsonResourceList(json.load(input_file))

    # Modify the owner
    for resource in resources:
        resource['owner'] = org_id
    return resources


def load_codelist_collections(filename='', org_id='', canonical_url='', verbosity=0):
    """
    Load and return codelist_collections as OCL-formatted JSON collections.
    This method retrieves all of the full codelist from DATIM directly, which takes
    a long time to process.
    """

    # Load the codelist definitions into a resource list
    csv_codelists = []
    with open(filename) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            # Skip rows that are not set to be imported
            if not row['resource_type']:
                continue
            if verbosity:
                print('Retrieving codelist: %s' % row['id'])
            row['owner_id'] = org_id
            dhis2_codelist_url = row.pop('ZenDesk: JSON Link')
            dhis2_codelist_url += '&paging=false'
            if verbosity:
                print('  DHIS2 URL: %s' % dhis2_codelist_url)
                print('  Canonical URL:', "%s/ValueSet/%s" % (canonical_url, row['id']))
            row['attr:dhis2_codelist_url'] = dhis2_codelist_url

            # Fetch the codelist from DHSI2
            dhis2_codelist_response = requests.get(dhis2_codelist_url)
            dhis2_codelist_response.raise_for_status()
            row['attr:dhis2_codelist'] = dhis2_codelist_response.json()
            csv_codelists.append(row)

    codelist_csv_resource_list = ocldev.oclresourcelist.OclCsvResourceList(resources=csv_codelists)
    codelist_json_resource_list = codelist_csv_resource_list.convert_to_ocl_formatted_json()

    # Fields not supported in the CSV format get added here
    for codelist in codelist_json_resource_list:
        codelist['canonical_url'] = "%s/ValueSet/%s" % (canonical_url, codelist['id'])

    return codelist_json_resource_list


def load_datim_indicators(filename='', org_id='', source_id='',
                          de_concepts=None, coc_concepts=None,
                          sorted_ref_indicator_codes=None, ref_indicator_concepts=None):
    """ Load DHIS2-formatted DATIM indicators and return as OCL-formatted concepts """

    # Load raw DHIS2-formatted DATIM indicators
    with open(filename, 'rb') as input_file:
        raw_datim_indicators = json.load(input_file)

    # Transform indicators to OCL-formatted JSON resources
    datim_indicator_concepts = ocldev.oclresourcelist.OclJsonResourceList()
    for indicator_raw in raw_datim_indicators['indicators']:
        datim_indicator_concepts.append(build_concept_from_datim_indicator(
            indicator_raw, org_id=org_id, source_id=source_id,
            de_concepts=de_concepts, coc_concepts=coc_concepts,
            sorted_ref_indicator_codes=sorted_ref_indicator_codes,
            ref_indicator_concepts=ref_indicator_concepts))
    return datim_indicator_concepts


def load_ref_indicator_concepts(org_id='', source_id='', filenames=None):
    """ Loads reference indicators from MER guidance as OCL-formatted JSON """
    if not filenames:
        return []
    ref_indicator_concepts = []
    for filename in filenames:
        with open(filename) as ifile:
            reader = csv.DictReader(ifile)
            for row in reader:
                row['owner_id'] = org_id
                row['source'] = source_id
                ref_indicator_concepts.append(row)
    ref_indicator_csv_list = ocldev.oclresourcelist.OclCsvResourceList(
        resources=ref_indicator_concepts)
    ref_indicator_json_list = ref_indicator_csv_list.convert_to_ocl_formatted_json()

    # Add throw-away attributes (only used for processing)
    for ref_indicator in ref_indicator_json_list:
        ref_indicator['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (
            org_id, source_id, ref_indicator['id'])

    return ref_indicator_json_list


def load_ihub_dde_concepts(filename='', num_run_sequences=3, org_id='',
                           source_id='', sorted_ref_indicator_codes=None,
                           ref_indicator_concepts=None,
                           ihub_rule_period_end_year=2020):
    """ Load iHUB Derived Data Element extract and return as OCL-formatted JSON concepts """

    # Load raw iHUB extract
    ihub_raw = []
    with open(filename) as input_csv_file:
        reader = csv.DictReader(input_csv_file)
        for row in reader:
            # JP: Some iHUB exports contain extra unicode characters at the beginning of the
            # file that python v2 doesn't handle well, so I'm removing them here
            if '\xef\xbb\xbfindicator' in row:
                row['indicator'] = row['\xef\xbb\xbfindicator']
            elif '\ufeffindicator' in row:
                row['indicator'] = row['\ufeffindicator']
            ihub_raw.append(row)

    # Transform to OCL-formatted concepts and return
    dde_concept_dict = build_all_ihub_dde_concepts(
        ihub_raw, num_run_sequences=num_run_sequences, org_id=org_id,
        source_id=source_id, sorted_ref_indicator_codes=sorted_ref_indicator_codes,
        ref_indicator_concepts=ref_indicator_concepts,
        ihub_rule_period_end_year=ihub_rule_period_end_year)
    return ocldev.oclresourcelist.OclJsonResourceList(list(dde_concept_dict.values()))


def get_ihub_dde_numerator_or_denominator(de_name):
    """
    Returns 'Numerator' or 'Denominator', respectively, if 'N' or 'D' is
    present in the data element modifiers (ie, in between parentheses).
    """
    if '(N,' in de_name or '(N)' in de_name:
        return 'Numerator'
    elif '(D,' in de_name or '(D)' in de_name:
        return 'Denominator'
    return ''


def get_ihub_dde_support_type(de_name):
    """
    Returns fully specified PEPFAR support type (eg 'Technical Assistance' or 'Direct Service
    Delivery') based on the presence of one of the acronyms in a iHUB derived data element name.
    """
    de_modifiers = get_data_element_name_modifiers(de_name)
    for support_type_code in SUPPORT_TYPE_CODES:
        if ', %s' % support_type_code in de_modifiers:
            return SUPPORT_TYPE_CODES[support_type_code]
    return ''


def get_data_element_support_type(de_code=''):
    """
    Returns fully specified PEPFAR support type (eg 'Technical Assistance' or 'Direct Service
    Delivery') based on the presence of one of the acronyms in a data element code.
    """
    for support_type_code in SUPPORT_TYPE_CODES:
        if '_%s_' % support_type_code in de_code:
            return SUPPORT_TYPE_CODES[support_type_code]
    return ''


def get_data_element_structured_dataset(de_code=''):
    """ Returns the structured dataset key for a data element code """
    if de_code.startswith('SIMS'):
        return 'SIMS'
    # TODO: Identify 'Other' or None structured dataset values
    return 'MER'


def get_ihub_dde_version(de_name):
    """ Returns data element version number if ' v#:' is present in the data element name. """
    version_codes = ['v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    for version_code in version_codes:
        if ' %s:' % version_code in de_name:
            return version_code
    return ''


def get_ihub_dde_name_without_version(de_name):
    """ Return name of a derived data element with version information stripped """
    de_version = get_ihub_dde_version(de_name=de_name)
    if de_version:
        return de_name.replace(' %s:' % de_version, ':')
    return de_name


def get_data_element_numerator_or_denominator(de_code=''):
    """
    Returns 'Numerator' or 'Denominator', respectively, if '_N_' or '_D_' is
    present in the data element code.
    """
    if '_N_' in de_code:
        return 'Numerator'
    elif '_D_' in de_code:
        return 'Denominator'
    return ''


def get_data_element_result_or_target(de_code=''):
    """ Returns 'Target' if the text is in the data element code, otherwise 'Result' """
    if 'target' in de_code.lower():
        return 'Target'
    return 'Result'


def lookup_reference_indicator_code(resource_name='', resource_code='',
                                    resource_applicable_periods=None,
                                    sorted_ref_indicator_codes=None, ref_indicator_concepts=None):
    """
    Returns a reference indicator code that matches the resource code or name.
    A ref indicator code is matched to the prefix of the resource name or code
    (eg. "TX_CURR_N_DSD_Age_Sex" is a match for "TX_CURR") or embedded in the name surrounded by
    whitespace (eg. "FY19 Results TX_ML Patient Died" is a match for "TX_ML"). If
    "de_applicable_periods" is provided, a matching reference indicator must also be applicable
    for at least on of the same periods. sorted_ref_indicator_codes must be a list of reference
    indicator codes sorted by string length in descending order.
    """
    for ref_indicator_code in sorted_ref_indicator_codes:
        if (resource_code[:len(ref_indicator_code)] == ref_indicator_code or
                resource_name[:len(ref_indicator_code)] == ref_indicator_code or
                ' %s ' % (ref_indicator_code) in resource_name):
            if resource_applicable_periods:
                for period in reversed(resource_applicable_periods):
                    ref_indicator_concept = ref_indicator_concepts.get_resource(
                        core_attrs={'id': ref_indicator_code}, custom_attrs={ATTR_PERIOD: period})
                    if ref_indicator_concept:
                        return ref_indicator_code
            else:
                return ref_indicator_code

    return ''


def get_sorted_unique_indicator_codes(ref_indicator_concepts=None):
    """
    Returns a list of unique sorted indicator codes given a list of
    OCL-formatted reference indicators
    """
    output = ref_indicator_concepts.summarize(core_attr_key='id').keys()
    return sorted(output, reverse=True)


def get_data_element_version(de_code=''):
    """
    Returns a data element version string (eg 'v2') if present. For example, a de_code of
    'TX_CURR_AgeSex_v3' would return 'v3'.
    """
    result = re.search('_([vV][0-9])$', de_code)
    if result:
        return result.group(1)
    return None


def get_data_element_root(de_code=''):
    """
    Returns a data element root string, ie with the version number removed. For example, a
    de_code of 'TX_CURR_AgeSex_v3' would return 'TX_CURR_AgeSex' and a de_code of
    'HTS_TST_AgeSex' would return 'HTS_TST_AgeSex'.
    """
    de_version = get_data_element_version(de_code=de_code)
    if de_version:
        return de_code[:-len(de_version) - 1]
    return de_code


def get_de_periods_from_codelist_collections(de_codelists, codelist_collections):
    """
    Get a list of the periods present in a data element's codelists.
    codelist_collections must be in the format of msp.load_codelist_collections or
    msp.load_codelist_collections_with_exports_from_file.
    de_codelists must be...
    """
    periods = {}
    for de_codelist in de_codelists:
        for codelist_def in codelist_collections:
            if de_codelist['id'] == codelist_def['external_id']:
                for period in codelist_def['extras'][ATTR_APPLICABLE_PERIODS].split(', '):
                    periods[period] = True
                break
    return list(periods.keys())


def get_concepts_filtered_by_period(concepts=None, period=None):
    """
    Returns a list of concepts filtered by ATTR_PERIOD or ATTR_APPLICABLE_PERIODS
    custom attributes. Period filter may be a single period (eg 'FY18') or
    a list of periods (eg ['FY18', 'FY19']). Works with ref_indicator_concepts, datim indicator
    concepts and data elements for both DATIM and iHUB.
    """

    # Get period filter into the right format
    if isinstance(period, str):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, str) for item in period):
        pass
    else:
        # Invalid period filter so just return an empty list
        return []

    # Setup the iterator
    if isinstance(concepts, dict):
        iterator_items = concepts.keys()
    elif isinstance(concepts, list):
        iterator_items = range(0, len(concepts))
    else:
        raise Exception('Invalid concepts. Expected dict or list')

    # Filter the concepts in same order as filter_period
    filtered_concepts = []
    for filter_period in period:
        for concept_key in iterator_items:
            concept = concepts[concept_key]
            concept_period = None
            if ATTR_APPLICABLE_PERIODS in concept['extras']:
                concept_period = concept['extras'][ATTR_APPLICABLE_PERIODS]
            elif ATTR_PERIOD in concept['extras']:
                concept_period = concept['extras'][ATTR_PERIOD]
            if concept_period is None:
                continue
            elif isinstance(concept_period, list):
                if filter_period in concept_period:
                    filtered_concepts.append(concept)
            elif isinstance(concept_period, str):
                if filter_period == concept_period:
                    filtered_concepts.append(concept)
            else:
                raise Exception('Invalid concept period. Expected list or string: %s <%s>' % (
                    concept_period, type(concept_period)))
    return filtered_concepts


def get_filtered_cocs(de_concepts=None, map_de_to_coc=None, coc_concepts=None):
    """ Returns list of COCs mapped to the list of data elements """
    cocs = {}
    for de_concept in de_concepts:
        de_concept_key = '/orgs/%s/sources/%s/concepts/%s/' % (
            de_concept['owner'], de_concept['source'], de_concept['id'])
        if de_concept_key in map_de_to_coc:
            for coc_concept_key in map_de_to_coc[de_concept_key]:
                cocs[coc_concept_key] = coc_concepts[coc_concept_key]
    return cocs


def get_filtered_codelist_collections(codelist_collections=None, period=None):
    """ Returns list of code lists filtered by either a single period or a list of periods """
    if isinstance(period, str):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, str) for item in period):
        pass
    else:
        return []
    filtered_codelist_collections = []
    for codelist in codelist_collections:
        if (period and ATTR_APPLICABLE_PERIODS in codelist['extras'] and
                any(codelist_period.strip() in period for codelist_period in codelist[
                    'extras'][ATTR_APPLICABLE_PERIODS].split(','))):
            filtered_codelist_collections.append(codelist)
        elif not period:
            filtered_codelist_collections.append(codelist)
    return filtered_codelist_collections


def generate_mapping_id(id_format='MAP_%s_%s', from_concept_code='', to_concept_code='',
                        from_concept_url='', to_concept_url=''):
    """
    Returns a custom mapping ID according to the specified id_format. id_format must
    have two %s parameters for the from and to concept codes. Examples for id_format:
        MAP_%s_%s
        MAP_DE_COC_%s_%s
    """
    if from_concept_url and to_concept_url:
        from_concept_code = from_concept_url[from_concept_url[:-1].rfind('/') + 1:-1]
        to_concept_code = to_concept_url[to_concept_url[:-1].rfind('/') + 1:-1]
    return id_format % (from_concept_code, to_concept_code)


def build_ocl_mappings(map_dict=None, filtered_from_concepts=None,
                       owner_type='Organization', owner_id='',
                       source_id='', map_type='',
                       do_generate_mapping_id=False, id_format='MAP_%s_%s'):
    """
    Returns a list of OCL-formatted mappings between from_concepts and to_concepts
    defined in map_dict. If filtered_from_concepts is provided, then maps are
    omitted if the from_concept is not in the filtered_from_concepts list. This
    method is designed to work with mappings between ref_indicator_concepts and data elements,
    and between data elements and COCs for DATIM and iHUB.
    """
    output_mappings = []
    for from_concept_url in map_dict:
        if filtered_from_concepts and from_concept_url not in filtered_from_concepts:
            continue
        for to_concept_url in map_dict[from_concept_url]:
            output_mapping = {
                "type": "Mapping", 'owner': owner_id, 'owner_type': owner_type,
                'source': source_id, 'map_type': map_type,
                'from_concept_url': from_concept_url, 'to_concept_url': to_concept_url,
            }
            if do_generate_mapping_id:
                output_mapping['id'] = generate_mapping_id(
                    id_format=id_format, from_concept_url=from_concept_url,
                    to_concept_url=to_concept_url)
            output_mappings.append(output_mapping)
    return output_mappings


def build_ref_indicator_references(ref_indicator_concepts, org_id=''):
    """
    Return a dictionary with period as key and OCL-formatted reference as value representing the
    set of reference indicators that are valid for each period. Eg:
        {"FY18": {"type": "Reference", "owner": "PEPFAR", "owner_type": "Organization",
                  "collection": "MER_REFERENCE_INDICATORS_FY18",
                  "data": {"expressions": "/orgs/PEPFAR/sources/MER/concepts/HTS_TST/", ...}}}
    """
    output_references_by_period = {}
    ref_indicator_period_counts = ref_indicator_concepts.summarize(custom_attr_key=ATTR_PERIOD)
    for period in ref_indicator_period_counts.keys():
        expressions = [
            ref_indicator_concept['__url'] for ref_indicator_concept in
            ref_indicator_concepts.get_resources(custom_attrs={ATTR_PERIOD: period})]
        output_references_by_period[period] = {
            'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_REFERENCE,
            'owner': org_id,
            'owner_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
            'collection': COLLECTION_NAME_MER_REFERENCE_INDICATORS % period,
            'data': {'expressions': expressions}
        }
    return output_references_by_period


def build_fiscal_year_references(ref_indicator_concepts, datim_indicator_concepts, de_concepts,
                                 ihub_dde_concepts, coc_concepts, map_ref_indicator_to_de,
                                 map_ref_indicator_to_ihub_dde,
                                 map_ref_indicator_to_datim_indicator,
                                 map_de_to_coc, map_ihub_dde_to_coc,
                                 org_id='', source_id=''):
    """
    Return a dictionary with period as key and OCL-formatted reference as value representing
    all resources that can be associated with that period. Includes everything but reference
    indicators (i.e. date elements, DATIM indicators, and COCs). Reference indicators are
    excluded because they are simply a copy of the MER_REFERENCE_INDICATOR_FY## collections
    and they are processed at a different time than the remaining references defined here.

    for each ref indicator in the period...
    1.  Cascade each Reference Indicator concept version to Indicator concepts using
        Has DATIM Indicator mappings where target_concept.extras.Applicable+Periods=FY20
    2.  Cascade each Reference Indicator concept version to Data Element concepts using
        Has Data Element mappings where target_concept.extras.Applicable+Periods=FY20
        2a. Cascade each DE concept to Category Option Combo concepts using Has Option mappings

    Example output:
        {"FY18": {"type": "Reference", "owner": "PEPFAR", "owner_type": "Organization",
                  "collection": "MER_FY18",
                  "data": {"expressions": "/orgs/PEPFAR/sources/MER/concepts/XHBL1mOwLWb/", ...}}}
    """
    output_references_by_period = {}
    ref_indicator_period_counts = ref_indicator_concepts.summarize(custom_attr_key=ATTR_PERIOD)
    for period in ref_indicator_period_counts.keys():
        expressions = []
        ref_indicator_concepts.get_resources(custom_attrs={ATTR_PERIOD: period})
        for ref_indicator_concept in ref_indicator_concepts:
            # datim indicators
            ref_indicator_url = ref_indicator_concept['__url']
            if ref_indicator_url in map_ref_indicator_to_datim_indicator:
                for datim_indicator_url in map_ref_indicator_to_datim_indicator[ref_indicator_url]:
                    datim_indicator_concept = datim_indicator_concepts.get_resource_by_url(
                        datim_indicator_url)
                    if not datim_indicator_concept:
                        continue
                    if ('extras' in datim_indicator_concept and
                            ATTR_APPLICABLE_PERIODS in datim_indicator_concept['extras'] and
                            period in datim_indicator_concept["extras"][ATTR_APPLICABLE_PERIODS]):
                        # add the datim indicator
                        if datim_indicator_url not in expressions:
                            expressions.append(datim_indicator_url)

                        # add the mapping
                        mapping_id = generate_mapping_id(
                            from_concept_url=ref_indicator_url, to_concept_url=datim_indicator_url,
                            id_format=MSP_MAP_ID_FORMAT_REFIND_IND)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in expressions:
                            expressions.append(mapping_url)

            # data elements
            if ref_indicator_url in map_ref_indicator_to_de:
                for de_url in map_ref_indicator_to_de[ref_indicator_url]:
                    de_concept = de_concepts.get_resource_by_url(de_url)
                    if not de_concept:
                        continue
                    if ('extras' in de_concept and
                            ATTR_APPLICABLE_PERIODS in de_concept['extras'] and
                            period in de_concept["extras"][ATTR_APPLICABLE_PERIODS]):
                        # add the data element
                        if de_url not in expressions:
                            expressions.append(de_url)

                        # add the mapping
                        mapping_id = generate_mapping_id(
                            from_concept_url=ref_indicator_url, to_concept_url=de_url,
                            id_format=MSP_MAP_ID_FORMAT_REFIND_DE)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in expressions:
                            expressions.append(mapping_url)

                        # cascade the COCs
                        if de_url in map_de_to_coc:
                            for coc_url in map_de_to_coc[de_url]:
                                # add the coc
                                if coc_url not in expressions:
                                    expressions.append(coc_url)

                                # add the mapping
                                mapping_id = generate_mapping_id(
                                    from_concept_url=de_url, to_concept_url=coc_url,
                                    id_format=MSP_MAP_ID_FORMAT_DE_COC)
                                mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                                    org_id, source_id, mapping_id)
                                if mapping_url not in expressions:
                                    expressions.append(mapping_url)

            # iHUB derived data elements
            if ref_indicator_url in map_ref_indicator_to_ihub_dde:
                for ihub_dde_url in map_ref_indicator_to_ihub_dde[ref_indicator_url]:
                    ihub_dde_concept = de_concepts.get_resource_by_url(ihub_dde_url)
                    if not ihub_dde_concept:
                        continue
                    if ('extras' in ihub_dde_concept and
                            ATTR_APPLICABLE_PERIODS in ihub_dde_concept['extras'] and
                            period in ihub_dde_concept["extras"][ATTR_APPLICABLE_PERIODS]):
                        # add the ihub derived data element
                        if ihub_dde_url not in expressions:
                            expressions.append(ihub_dde_url)

                        # add the mapping
                        mapping_id = generate_mapping_id(
                            from_concept_url=ref_indicator_url, to_concept_url=ihub_dde_url,
                            id_format=MSP_MAP_ID_FORMAT_REFIND_DE)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in expressions:
                            expressions.append(mapping_url)

                        # cascade the COCs
                        if ihub_dde_url in map_ihub_dde_to_coc:
                            for coc_url in map_ihub_dde_to_coc[ihub_dde_url]:
                                # add the coc
                                if coc_url not in expressions:
                                    expressions.append(coc_url)

                                # add the mapping
                                mapping_id = generate_mapping_id(
                                    from_concept_url=ihub_dde_url, to_concept_url=coc_url,
                                    id_format=MSP_MAP_ID_FORMAT_DE_COC)
                                mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                                    org_id, source_id, mapping_id)
                                if mapping_url not in expressions:
                                    expressions.append(mapping_url)

        output_references_by_period[period] = {
            'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_REFERENCE,
            'owner': org_id,
            'owner_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
            'collection': COLLECTION_NAME_MER_FULL % period,
            'data': {'expressions': expressions}
        }
    return output_references_by_period


def build_codelist_references(map_codelist_to_de_to_coc=None, org_id='', source_id='',
                              codelist_collections=None):
    """ Return a list of batched references for DE/COC concepts & mappings for each codelist. """
    codelist_references = []
    for codelist_external_id in map_codelist_to_de_to_coc:
        codelist = codelist_collections.get_resource(
            core_attrs={'external_id': codelist_external_id})
        if codelist:
            codelist_references += get_mapped_concept_references(
                from_concept_urls=list(map_codelist_to_de_to_coc[codelist_external_id].keys()),
                map_dict=map_codelist_to_de_to_coc[codelist_external_id],
                org_id=org_id, source_id=source_id, collection_id=codelist['id'],
                do_cascade_source_mappings=False,
                include_explicit_mapping_reference=True,
                mapping_id_format=MSP_MAP_ID_FORMAT_DE_COC)
    return codelist_references


def get_mapped_concept_references(from_concepts=None, from_concept_urls=None, map_dict=None,
                                  org_id='', source_id='', collection_id='',
                                  include_to_concept_refs=True,
                                  ignore_from_concepts_with_no_maps=True,
                                  do_cascade_source_mappings=True,
                                  include_explicit_mapping_reference=False,
                                  mapping_id_format=''):
    """
    Returns a list of references for the specified list of from-concepts and, optionally, their
    mapped to concepts. Supports mappings for ref_indicator_concepts to data elements/DATIM
    indicators, and data elements to COCs for both DATIM and iHUB. Must provide either
    from_concepts or from_concept_urls.
    """
    output_references = []
    ref_from_concept_expressions = []
    ref_to_concept_expressions = []
    ref_mapping_expressions = []

    # Iterate thru the from_concepts as a dict or list, the from_concept_urls,
    # or directly iterate the maps
    if isinstance(from_concepts, dict) and isinstance(map_dict, dict):
        for from_concept_url in from_concepts:
            ref_from_concept_expressions.append(from_concept_url)
            if from_concept_url not in map_dict and not ignore_from_concepts_with_no_maps:
                raise Exception('ERROR: from_concept_url not in map_dict: %s' % from_concept_url)
            if include_to_concept_refs:
                for to_concept_url in map_dict[from_concept_url]:
                    if to_concept_url not in ref_to_concept_expressions:
                        ref_to_concept_expressions.append(to_concept_url)
                    if include_explicit_mapping_reference:
                        mapping_id = generate_mapping_id(
                            id_format=mapping_id_format, from_concept_url=from_concept_url,
                            to_concept_url=to_concept_url)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in ref_mapping_expressions:
                            ref_mapping_expressions.append(mapping_url)
    elif isinstance(from_concepts, list) and isinstance(map_dict, dict):
        for from_concept in from_concepts:
            from_concept_url = from_concept['__url']
            ref_from_concept_expressions.append(from_concept_url)
            if from_concept_url not in map_dict and not ignore_from_concepts_with_no_maps:
                raise Exception(
                    'ERROR: from_concept_url not in map_dict: %s' % from_concept_url)
            if include_to_concept_refs:
                for to_concept_url in map_dict[from_concept_url]:
                    if to_concept_url not in ref_to_concept_expressions:
                        ref_to_concept_expressions.append(to_concept_url)
                    if include_explicit_mapping_reference:
                        mapping_id = generate_mapping_id(
                            id_format=mapping_id_format, from_concept_url=from_concept_url,
                            to_concept_url=to_concept_url)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in ref_mapping_expressions:
                            ref_mapping_expressions.append(mapping_url)
    elif isinstance(from_concept_urls, list) and isinstance(map_dict, dict):
        for from_concept_url in from_concept_urls:
            ref_from_concept_expressions.append(from_concept_url)
            if from_concept_url not in map_dict and not ignore_from_concepts_with_no_maps:
                raise Exception('ERROR: from_concept_url not in map_dict %s' % from_concept_url)
            if include_to_concept_refs:
                for to_concept_url in map_dict[from_concept_url]:
                    if to_concept_url not in ref_to_concept_expressions:
                        ref_to_concept_expressions.append(to_concept_url)
                    if include_explicit_mapping_reference:
                        mapping_id = generate_mapping_id(
                            id_format=mapping_id_format, from_concept_url=from_concept_url,
                            to_concept_url=to_concept_url)
                        mapping_url = '/orgs/%s/sources/%s/mappings/%s/' % (
                            org_id, source_id, mapping_id)
                        if mapping_url not in ref_mapping_expressions:
                            ref_mapping_expressions.append(mapping_url)
    else:
        raise Exception('Must provide map_dict and either from_concepts or from_concept_urls')

    # Build from_concept reference
    if ref_from_concept_expressions:
        # Cascade to source mappings for from_concept references
        reference = {
            'type': 'Reference', 'owner_type': 'Organization', 'owner': org_id,
            'collection': collection_id,
            'data': {'expressions': ref_from_concept_expressions}
        }
        if do_cascade_source_mappings:
            reference['__cascade'] = 'sourcemappings'
        output_references.append(reference)

    # Build to_concept reference
    if ref_to_concept_expressions:
        output_references.append({
            'type': 'Reference', 'owner': org_id, 'owner_type': 'Organization',
            'collection': collection_id,
            'data': {'expressions': ref_to_concept_expressions},
        })

    # Build mapping reference
    if ref_mapping_expressions:
        output_references.append({
            'type': 'Reference', 'owner': org_id, 'owner_type': 'Organization',
            'collection': collection_id,
            'data': {'expressions': ref_mapping_expressions},
        })

    return output_references


def get_mapped_concept_references_by_period(from_concepts=None, map_dict=None,
                                            org_id='', collection_id='', periods=None,
                                            include_to_concept_refs=False,
                                            include_all_period=False,
                                            ignore_from_concepts_with_no_maps=False):
    """
    Returns dictionary with period as key, list of OCL-formatted references for all specified
    concepts as value. If include_to_concept_refs is True, the "to concepts" that each "from
    concept" points to are also included. If include_all_period is True, a period with value
    of '*' is added that includes all passed concepts regardless of period. Compatible with
    ref_indicator_concepts mapped to both DATIM and iHUB data elements and data elements
    mapped to COCs.
    """
    output_references = {}
    for period in periods:
        period_collection_id = '%s-%s' % (collection_id, period)
        output_references[period] = get_mapped_concept_references(
            from_concepts=get_concepts_filtered_by_period(concepts=from_concepts, period=period),
            map_dict=map_dict, org_id=org_id, collection_id=period_collection_id,
            include_to_concept_refs=include_to_concept_refs,
            ignore_from_concepts_with_no_maps=ignore_from_concepts_with_no_maps)
    if include_all_period:
        output_references['*'] = get_mapped_concept_references(
            from_concepts=from_concepts, map_dict=map_dict,
            org_id=org_id, collection_id=collection_id,
            include_to_concept_refs=include_to_concept_refs,
            ignore_from_concepts_with_no_maps=ignore_from_concepts_with_no_maps)
    return output_references


def build_mer_indicator_references(ref_indicator_concepts=None,
                                   map_indicator_to_de=None, map_indicator_to_ihub_dde=None,
                                   org_id='', collection_id='', periods=None,
                                   include_to_concept_refs=False, include_all_period=False):
    """
    Returns a dictionary with period as key, list of OCL-formatted references
    for all passed ref_indicator_concepts as value. Combines maps from ref_indicator_concepts
    to both DATIM and iHUB data elements. If include_all_period is True, a period with
    value of '*' is added that spans all periods (and resources with no period).
    """
    combined_indicator_maps = map_indicator_to_de.copy()
    combined_indicator_maps.update(map_indicator_to_ihub_dde)
    combined_indicator_references = get_mapped_concept_references_by_period(
        from_concepts=ref_indicator_concepts, map_dict=combined_indicator_maps,
        org_id=org_id, collection_id=collection_id, periods=periods,
        include_to_concept_refs=include_to_concept_refs,
        include_all_period=include_all_period,
        ignore_from_concepts_with_no_maps=True)
    return combined_indicator_references


def build_mer_references(de_concepts=None, map_de_to_coc=None,
                         ihub_dde_concepts=None, map_ihub_dde_to_coc=None,
                         org_id='', collection_id='', periods=None,
                         include_to_concept_refs=True,
                         include_all_period=False):
    """
    Returns dictionary with period as key, list of OCL-formatted references
    for all specified data elements as value. Combines all DATIM and iHUB data
    elements and their maps. If include_all_period is True, a period with
    value of '*' is added that includes all passed data elements.
    """

    # First combine the data elements and maps from the two sources
    combined_de_concepts = de_concepts.copy()
    combined_de_concepts.update(ihub_dde_concepts)
    combined_de_to_coc_maps = map_de_to_coc.copy()
    combined_de_to_coc_maps.update(map_ihub_dde_to_coc)

    # NOTE: There is an overlap between data elements in iHUB and DATIM, because
    # some DATIM DEs were replaced by derived DEs. This means that some DATIM DEs are
    # referenced in an iHUB run_sequence but are not marked as sourced from DATIM.

    mer_references = get_mapped_concept_references_by_period(
        from_concepts=combined_de_concepts, map_dict=combined_de_to_coc_maps,
        org_id=org_id, collection_id=collection_id, periods=periods,
        include_to_concept_refs=include_to_concept_refs,
        include_all_period=include_all_period)
    return mer_references


def get_repo_version_json(owner_type='Organization', owner_id='', repo_type='Source',
                          repo_id='', version_id='', description='', released=True):
    """ Returns OCL-formatted JSON for a repository version """
    if not isinstance(repo_id, str):
        raise TypeError("String expected for repo_id. %s given." % type(repo_id))
    return {
        'type': '%s Version' % repo_type,
        'id': version_id,
        'owner': owner_id,
        'owner_type': owner_type,
        repo_type.lower(): repo_id,
        'description': description,
        'released': released,
    }


def get_codelist_collections_formatted_for_display(codelist_collections):
    """
    Output a python dictionary of codelist definitions formatted for display in MSP
    """
    output_codelists = []
    for codelist in codelist_collections:
        output_codelist = {
            'id': codelist['id'],
            'name': codelist['name'],
            'full_name': codelist['full_name'],
            'periods': codelist['extras'][ATTR_APPLICABLE_PERIODS].split(', '),
            'codelist_type': codelist['extras'][ATTR_RESULT_TARGET],
            'description': codelist.get('description', ''),
            'dataset_id': codelist['extras']['dataset_id']
        }
        output_codelists.append(output_codelist)


def format_identifier(unformatted_id, replace_char='-', allow_underscore=False,
                      invalid_chars='`~!@#$%^&*()_+-=[]{}\\|;:"\',/<>?'):
    """
    Format a string according to the OCL ID rules: Everything in invalid_chars goes,
    except that underscores are allowed for the concept_id
    """
    formatted_id = list(unformatted_id)
    if allow_underscore:
        # Remove underscore from the invalid characters - Concept IDs are okay with underscores
        chars_to_remove = invalid_chars.replace('_', '')
    else:
        chars_to_remove = invalid_chars
    for index in range(len(unformatted_id)):
        if unformatted_id[index] in chars_to_remove:
            formatted_id[index] = ' '
    output_str = ''.join(formatted_id)
    output_str = re.sub(r"\s+", " ", output_str.strip())
    output_str = output_str.replace(' ', replace_char)
    return output_str


def format_concept_id(unformatted_id):
    """
    Format a string according to the OCL concept ID rules: Everything in invalid_chars goes,
    except that underscores are allowed for the concept_id
    """
    return format_identifier(
        unformatted_id=unformatted_id.replace('+', ' plus '), replace_char='_')


def dedup_list_of_dicts(dup_dict):
    """
    Dedup the import list without changing order
    NOTE: More elegant solutions to de-duping all resulted in keeping only the last occurence
    of a resource, where it is required that we keep only the first occurence of a resource,
    hence the custom solution.
    APPROACH #1: This approach successfully de-duped, but took a very long time and kept only
    the last occurence
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
    APPROACH #2: This successfully de-duped and ran quickly, but still kept only last occurence
        import_list_jsons = {json.dumps(resource, sort_keys=True) for resource in import_list}
        import_list_dedup = [json.loads(resource) for resource in import_list_jsons]
    APPROACH #3 used here:
    This custom approach (implemented below) successfully de-duped, ran slightly slower than
    approach #2, though much faster than #1, and it kept the 1st occurence!
    """
    dedup_list = []
    dedup_list_jsons = []
    old_list_jsons = [json.dumps(resource, sort_keys=True) for resource in dup_dict]
    for str_resource in old_list_jsons:
        if str_resource not in dedup_list_jsons:
            dedup_list_jsons.append(str_resource)
            dedup_list.append(json.loads(str_resource))
    return dedup_list


def summarize_applicable_periods_from_concepts(resource_list):
    """
    Return list of counts for each period in the ATTR_APPLICABLE_PERIODS custom attribute for
    the specified resource list. Ex: {"FY18": 201, "FY19": 17}. Note that counts may add up to
    more than the total number of resources because each resource may have more than one
    applicable period.
    """
    period_counts = {}
    for resource in resource_list:
        if ATTR_APPLICABLE_PERIODS in resource['extras']:
            for period in resource['extras'][ATTR_APPLICABLE_PERIODS]:
                if period not in period_counts:
                    period_counts[period] = 0
                period_counts[period] += 1
        else:
            if None not in period_counts:
                period_counts[None] = 0
            period_counts[None] += 1
    return period_counts


def build_concept_from_datim_indicator(indicator_raw, org_id='', source_id='',
                                       de_concepts=None, coc_concepts=None,
                                       sorted_ref_indicator_codes=None,
                                       ref_indicator_concepts=None):
    """
    Return an OCL-formatted concept for the specified DATIM indicator.
    If de_concepts and coc_concepts arguments are provided, extra attributes are included for the
    numerator/denominator in which the UIDs have been with human-readable codes or names.
    """

    # Determine result/target for this DATIM indicator
    if 'target' in indicator_raw['name'].lower():
        result_target = 'Target'
    elif 'result' in indicator_raw['name'].lower() or indicator_raw['name'].lower()[:3] != 'ea_':
        result_target = 'Result'
    else:
        result_target = 'N/A'

    # Determine period range for this DATIM indicator
    indicator_periods = []
    for period in MAP_PERIOD_TO_INDICATOR_TERMS:
        for term in MAP_PERIOD_TO_INDICATOR_TERMS[period]:
            if term.lower() in indicator_raw['name'].lower():
                if period not in indicator_periods:
                    indicator_periods.append(period)

    # Build the DATIM indicator concept
    indicator_concept = {
        'type': 'Concept',
        'id': indicator_raw['id'],
        'owner': org_id,
        'owner_type': 'Organization',
        'source': source_id,
        'concept_class': 'Indicator',
        'datatype': indicator_raw['indicatorType']['name'],
        'names': [
            {
                'name': indicator_raw['name'],
                'name_type': 'Fully Specified',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            },
            {
                'name': indicator_raw['shortName'],
                'name_type': 'Short',
                'locale': 'en',
                'locale_preferred': False,
                'external_id': None,
            }
        ],
        'extras': {
            'annualized': indicator_raw.get('annualized', ''),
            'denominator': indicator_raw.get('denominator', ''),
            'denominatorDescription': indicator_raw.get('denominatorDescription', ''),
            'denominatorReadableFormula': replace_formula_uids_with_names(
                formula=indicator_raw.get('denominator', ''), org_id=org_id, source_id=source_id,
                de_concepts=de_concepts, coc_concepts=coc_concepts),
            'denominatorParsedFormula': parse_indicator_formula(
                formula=indicator_raw.get('denominator', ''), org_id=org_id, source_id=source_id,
                de_concepts=de_concepts, coc_concepts=coc_concepts),
            'numerator': indicator_raw.get('numerator', ''),
            'numeratorDescription': indicator_raw.get('numeratorDescription', ''),
            'numeratorReadableFormula': replace_formula_uids_with_names(
                formula=indicator_raw.get('numerator', ''), org_id=org_id, source_id=source_id,
                de_concepts=de_concepts, coc_concepts=coc_concepts),
            'numeratorParsedFormula': parse_indicator_formula(
                formula=indicator_raw.get('numerator', ''), org_id=org_id, source_id=source_id,
                de_concepts=de_concepts, coc_concepts=coc_concepts),
            'dimensionItemType': indicator_raw['dimensionItemType'],
            ATTR_RESULT_TARGET: result_target,
            ATTR_APPLICABLE_PERIODS: indicator_periods
        },
        '__url': ocldev.oclconstants.OclConstants.get_resource_url(
            owner_id=org_id, repository_id=source_id, resource_id=indicator_raw['id'],
            include_trailing_slash=True)
    }

    # Determine mapped indicator code
    ref_indicator_code = lookup_reference_indicator_code(
        resource_name=indicator_raw['name'], resource_code=indicator_raw['shortName'],
        resource_applicable_periods=indicator_periods,
        sorted_ref_indicator_codes=sorted_ref_indicator_codes,
        ref_indicator_concepts=ref_indicator_concepts)

    if ref_indicator_code:
        indicator_concept['extras']['indicator'] = ref_indicator_code
    if 'indicatorGroups' in indicator_raw and indicator_raw['indicatorGroups']:
        indicator_concept['extras']['indicatorGroups'] = indicator_raw['indicatorGroups']

    return indicator_concept


def parse_indicator_formula(formula, org_id, source_id, de_concepts, coc_concepts):
    """
    Return an array of parsed terms that appear in the specified indicator formula.
    Each term consists of both UIDs and names for a data element and COC (if present).
    Regex returns the following for each formula term:
        [0]: full matched term
        [1]: data element UID
        [2]: COC UID, if present
        [3]: Mechanism UID, if present
    """
    regex = r'(#\{(?P<deuid>(?:\S|\d){11})(?:\}|(?:.(?P<cocuid>(?:\S|\d){11}))(?:\}|(?:.(?P<mechanismuid>(?:\S|\d){11}))\})))'
    matches = re.findall(regex, formula)
    parsed_formula = []
    has_mechanism = False
    for (full_match, de_uid, coc_uid, mechanism_uid) in matches:
        de_concept_name = ''
        coc_concept_name = ''
        mechanism_name = ''

        # Get the DE name
        de_url = ocldev.oclconstants.OclConstants.get_resource_url(
            owner_id=org_id, repository_id=source_id, resource_id=de_uid,
            include_trailing_slash=True)
        de_concept = de_concepts.get_resource_by_url(de_url)
        if de_concept:
            de_concept_name = ocldev.oclresourcelist.OclResourceList.get_concept_name_by_type(
                concept=de_concept, name_type=['Code', 'Short', 'Fully Specified'])
            if not de_concept_name:
                de_concept_name = de_uid

        # Get the COC name, if present
        if coc_uid:
            coc_url = ocldev.oclconstants.OclConstants.get_resource_url(
                owner_id=org_id, repository_id=source_id, resource_id=coc_uid,
                include_trailing_slash=True)
            coc_concept = coc_concepts.get_resource_by_url(coc_url)
            if coc_concept:
                coc_concept_name = ocldev.oclresourcelist.OclResourceList.get_concept_name_by_type(
                    concept=coc_concept, name_type=['Code', 'Short', 'Fully Specified'])
            if not coc_concept_name:
                coc_concept_name = coc_uid

        # TODO: Get the Mechanism name, if present
        if mechanism_uid:
            mechanism_name = mechanism_uid

        # Add parsed formula term to the return array
        parsed_formula_term = {
            "full_term": full_match,
            "data_element_uid": de_uid,
            "data_element_name": de_concept_name}
        if coc_uid:
            parsed_formula_term["category_option_combo_uid"] = coc_uid
            parsed_formula_term["category_option_combo_name"] = coc_concept_name
        if mechanism_uid:
            has_mechanism = True
            parsed_formula_term["mechanism_uid"] = mechanism_uid
            parsed_formula_term["mechanism_name"] = mechanism_name
        parsed_formula.append(parsed_formula_term)

    return parsed_formula


def replace_formula_uids_with_names(formula, org_id, source_id, de_concepts, coc_concepts):
    """
    Return a formula string with UIDs replaced with human-readable codes or names.
    Regex returns the following for each formula term:
        [0]: full matched term
        [1]: data element UID
        [2]: COC UID, if present
        [3]: Mechanism UID, if present
    """
    # regex = r'(#\{(?P<deuid>(?:\S|\d){11})(?:\}|(?:.(?P<cocuid>(?:\S|\d){11}))\}))'
    regex = r'(#\{(?P<deuid>(?:\S|\d){11})(?:\}|(?:.(?P<cocuid>(?:\S|\d){11}))(?:\}|(?:.(?P<mechanismuid>(?:\S|\d){11}))\})))'
    matches = re.findall(regex, formula)
    new_formula = formula
    for (full_match, de_uid, coc_uid, mechanism_uid) in matches:
        de_concept_name = ''
        coc_concept_name = ''
        mechanism_name = ''

        # Get the DE name
        de_url = ocldev.oclconstants.OclConstants.get_resource_url(
            owner_id=org_id, repository_id=source_id, resource_id=de_uid,
            include_trailing_slash=True)
        de_concept = de_concepts.get_resource_by_url(de_url)
        de_concept_name = ''
        if de_concept:
            de_concept_name = ocldev.oclresourcelist.OclResourceList.get_concept_name_by_type(
                concept=de_concept, name_type=['Code', 'Short', 'Fully Specified'])
            if de_concept_name:
                de_concept_name = '[%s]' % de_concept_name
        if not de_concept_name:
            de_concept_name = de_uid

        # Get the COC name, if present
        if coc_uid:
            coc_concept_name = ''
            coc_url = ocldev.oclconstants.OclConstants.get_resource_url(
                owner_id=org_id, repository_id=source_id, resource_id=coc_uid,
                include_trailing_slash=True)
            coc_concept = coc_concepts.get_resource_by_url(coc_url)
            if coc_concept:
                coc_concept_name = ocldev.oclresourcelist.OclResourceList.get_concept_name_by_type(
                    concept=coc_concept, name_type=['Code', 'Short', 'Fully Specified'])
            if coc_concept_name:
                coc_concept_name = '[%s]' % coc_concept_name
            else:
                coc_concept_name = coc_uid

        # TODO: Get the Mechanism name, if present
        if mechanism_uid:
            mechanism_name = mechanism_uid

        # Replace the match in the formula with the human-readable names
        if mechanism_uid:
            new_formula = new_formula.replace(full_match, '{%s.%s.%s}' % (
                de_concept_name, coc_concept_name, mechanism_name))
        elif coc_uid:
            new_formula = new_formula.replace(full_match, '{%s.%s}' % (
                de_concept_name, coc_concept_name))
        else:
            new_formula = new_formula.replace(full_match, '{%s}' % de_concept_name)

    # Add whitespace around mathematical operators to improve readability
    new_formula = new_formula.replace('}+{', '} + {').replace('}-{', '} - {').replace('}*{', '} * {').replace('}/{', '} / {')
    return new_formula


def build_concept_from_datim_coc(coc_raw, org_id, source_id):
    """ Return an OCL-formatted concept for the specified DATIM category option combo """
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
        ],
        '__url': '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, coc_raw['id'])
    }
    return coc_concept


def build_concept_from_datim_de(de_raw, org_id, source_id, sorted_ref_indicator_codes,
                                codelist_collections, ref_indicator_concepts):
    """ Return an OCL-formatted concept for the specified DATIM data element """

    # Determine core data element attributes
    de_concept_id = de_raw['id']  # eg sAxSUTFc5tp
    de_code = de_raw['code'] if 'code' in de_raw else de_raw['shortName']
    de_result_or_target = get_data_element_result_or_target(de_code=de_code)
    de_numerator_or_denominator = get_data_element_numerator_or_denominator(de_code=de_code)
    de_version = get_data_element_version(de_code=de_code)  # v2, v3, ...
    de_code_root = get_data_element_root(de_code=de_code)  # HTS_TST_AgeSex
    de_support_type = get_data_element_support_type(de_code=de_code)  # DSD, TA, CS, ...
    de_structured_dataset = get_data_element_structured_dataset(de_code=de_code) # SIMS, MER, ...

    # Build the OCL formatted concept
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
        ],
        'extras': {
            'source': 'DATIM',
            'data_element_root': de_code_root,
        }
    }

    # Save DE code as a concept synonym (not all DEs have codes)
    if 'code' in de_raw:
        de_concept['names'].append({
            'name': de_raw['code'],
            'name_type': 'Code',
            'locale': 'en',
            'locale_preferred': False,
            'external_id': None,
        })

    # Save DE description as a concept description (most DEs do not have descriptions)
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

    # Generate DE 'codelists' and 'applicable_periods'
    de_codelists = get_codelists_for_data_element(
        de_concept_id, codelist_collections)
    de_applicable_periods = get_de_periods_from_codelist_collections(
        de_codelists=de_codelists, codelist_collections=codelist_collections)

    # Determine mapped reference indicator code
    de_indicator_code = lookup_reference_indicator_code(
        resource_name=de_raw['name'],
        resource_code=de_raw.get('code', ''),
        resource_applicable_periods=de_applicable_periods,
        sorted_ref_indicator_codes=sorted_ref_indicator_codes,
        ref_indicator_concepts=ref_indicator_concepts)

    # Determine DE reporting frequency (needs name, indicator, result/target, & period)
    de_reporting_frequency = get_de_reporting_frequency(
        de_name=de_raw['name'],
        de_indicator_code=de_indicator_code,
        de_result_or_target=de_result_or_target,
        de_applicable_periods=de_applicable_periods,
        ref_indicator_concepts=ref_indicator_concepts)

    # Set DE custom attributes
    if 'dataElementGroups' in de_raw and de_raw['dataElementGroups']:
        de_concept['extras']['dataElementGroups'] = de_raw['dataElementGroups']
    if ATTR_DOMAIN_TYPE in de_raw and de_raw[ATTR_DOMAIN_TYPE]:
        de_concept['extras'][ATTR_DOMAIN_TYPE] = de_raw[ATTR_DOMAIN_TYPE]
    if ATTR_VALUE_TYPE in de_raw and de_raw[ATTR_VALUE_TYPE]:
        de_concept['extras'][ATTR_VALUE_TYPE] = de_raw[ATTR_VALUE_TYPE]
    if ATTR_AGGREGATION_TYPE in de_raw and de_raw[ATTR_AGGREGATION_TYPE]:
        de_concept['extras'][ATTR_AGGREGATION_TYPE] = de_raw[ATTR_AGGREGATION_TYPE]
    if de_codelists:
        de_concept['extras'][ATTR_CODELISTS] = de_codelists
    if de_applicable_periods:
        de_concept['extras'][ATTR_APPLICABLE_PERIODS] = de_applicable_periods
    if de_version:
        de_concept['extras']['data_element_version'] = de_version
    if de_numerator_or_denominator:
        de_concept['extras'][ATTR_NUMERATOR_DENOMINATOR_TYPE] = de_numerator_or_denominator
    if de_support_type:
        de_concept['extras'][ATTR_PEPFAR_SUPPORT_TYPE] = de_support_type
    if de_result_or_target:
        de_concept['extras'][ATTR_RESULT_TARGET] = de_result_or_target
    if de_indicator_code:
        de_concept['extras']['indicator'] = de_indicator_code
    if de_reporting_frequency:
        de_concept['extras'][ATTR_REPORTING_FREQUENCY] = de_reporting_frequency
    if de_structured_dataset:
        de_concept['extras'][ATTR_STRUCTURED_DATASET] = de_structured_dataset

    # Add throw-away attributes that are used later in processing
    de_concept['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_concept_id)
    de_concept['__cocs'] = de_raw['categoryCombo']['categoryOptionCombos']

    return de_concept


def get_codelists_for_data_element(de_uid, codelist_collections):
    """
    Returns the codelists that the specified data element is a member of. Example return value:
    [{'code': 'MER_R_FACILITY_BASED_FY2019Q4',
      'id': 'KWRj80vEfHU',
      'name': 'MER Results: Facility Based FY2019Q4',
      'shortName': 'MER R: Facility Based FY2019Q4'}]
    """
    de_codelists = []
    for codelist in codelist_collections:
        for row in codelist['extras']['dhis2_codelist']['listGrid']['rows']:
            if row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID] == de_uid:
                de_codelists.append({
                    'code': codelist['id'],
                    'id': codelist['external_id'],
                    'name': codelist['full_name'],
                    'shortName': codelist['name'],
                })
                break
    return de_codelists


def get_de_reporting_frequency(de_name='', de_result_or_target='', de_indicator_code='',
                               de_applicable_periods=None, ref_indicator_concepts=None):
    """
    Get the reporting frequency for a data element based on its name, result/target, and linked
    reference indicator. Possible values are: "Daily", "Quarterly", "Semi-Annually", "Annually".
    """
    if 'sims' in de_name.lower():
        return 'Daily'
    elif de_result_or_target == 'Target':
        return 'Annually'
    elif de_indicator_code:
        if de_applicable_periods:
            for period in reversed(de_applicable_periods):
                ref_indicator_concept = ref_indicator_concepts.get_resource(
                    core_attrs={'id': de_indicator_code}, custom_attrs={ATTR_PERIOD: period})
                if (ref_indicator_concept and
                        ATTR_REPORTING_FREQUENCY in ref_indicator_concept['extras']):
                    return ref_indicator_concept['extras'][ATTR_REPORTING_FREQUENCY]
        else:
            ref_indicator_concept = ref_indicator_concepts.get_resource(
                core_attrs={'id': de_indicator_code})
            if (ref_indicator_concept and
                    ATTR_REPORTING_FREQUENCY in ref_indicator_concept['extras']):
                return ref_indicator_concept['extras'][ATTR_REPORTING_FREQUENCY]
    return ''


def build_linkages_de_version(de_concepts=None):
    """ Return a dictionary describing DEs that have multiple versions. """

    # Build dictionary of DEs, grouping different versions of a DE together
    de_all_versions = {}
    for de_concept in de_concepts:
        if 'data_element_root' not in de_concept['extras']:
            continue
        de_root_code = de_concept['extras']['data_element_root']
        if de_root_code not in de_all_versions:
            de_all_versions[de_root_code] = []
        if 'data_element_version' in de_concept['extras']:
            de_root_code += '_%s' % de_concept['extras']['data_element_version']
        de_version = de_concept['extras'].get('data_element_version', '')
        de_sort_order = 1 if not de_version else int(de_version[1:])
        de_all_versions[de_concept['extras']['data_element_root']].append({
            'url': de_concept['__url'],
            'code': de_root_code,
            'version': de_version,
            'sort_order': de_sort_order
        })

    # Only return those with more than one record
    de_filtered_versions = {}
    for de_root_code in de_all_versions:
        if len(de_all_versions[de_root_code]) > 1:
            de_filtered_versions[de_root_code] = sorted(
                de_all_versions[de_root_code], key=lambda i: i['sort_order'])
    return de_filtered_versions


def build_linkages_dde_version(ihub_dde_concepts=None):
    """ Get a dictionary of DDEs that have multiple versions """

    # Build dictionary of DEs, grouping different versions of a DE together
    de_all_versions = {}
    for de_concept in ihub_dde_concepts:
        de_name = de_concept['names'][0]['name']
        de_name_without_version = get_ihub_dde_name_without_version(de_name=de_name)
        if de_name_without_version not in de_all_versions:
            de_all_versions[de_name_without_version] = []
        de_version = de_concept['extras'].get('data_element_version', '')
        de_sort_order = 1 if not de_version else int(de_version[1:])
        de_all_versions[de_name_without_version].append({
            'url': de_concept['__url'],
            'code': de_name,
            'version': de_version,
            'sort_order': de_sort_order
        })

    # Only return those with more than one record
    de_filtered_versions = {}
    for de_root_code in de_all_versions:
        if len(de_all_versions[de_root_code]) > 1:
            de_filtered_versions[de_root_code] = sorted(
                de_all_versions[de_root_code], key=lambda i: i['sort_order'])
    return de_filtered_versions


def build_maps_from_de_linkages(de_linkages=None):
    """
    Return a dictionary representing data element version linkages, where a data element URL is
    the key and a list of one or more data element URLs that were "replaced by" as the value.
    """
    map_de_linkages = {}
    for de_linkage_code in de_linkages:
        de_replaced_by_from_concept = de_linkages[de_linkage_code][0]
        for index in range(1, len(de_linkages[de_linkage_code])):
            de_replaced_to_concept = de_replaced_by_from_concept
            de_replaced_by_from_concept = de_linkages[de_linkage_code][index]
            if de_replaced_by_from_concept['url'] not in map_de_linkages:
                map_de_linkages[de_replaced_by_from_concept['url']] = []
            if de_replaced_to_concept['url'] not in map_de_linkages[de_replaced_by_from_concept['url']]:
                map_de_linkages[de_replaced_by_from_concept['url']].append(
                    de_replaced_to_concept['url'])
    return map_de_linkages


def build_linkages_source_de(ihub_dde_concepts=None, owner_id='', source_id=''):
    """
    Return a dictionary representing linkages between iHUB derived data elements and their
    source data elements. The keys are the derived data element URLs, and values are lists of
    source data element URLs.
    :param ihub_dde_concepts:
    :param owner_id:
    :param source_id:
    :return:
    """
    dde_source_linkages = {}
    for de_concept in ihub_dde_concepts:
        if de_concept['__url'] not in dde_source_linkages:
            dde_source_linkages[de_concept['__url']] = []
        for source_linkage in de_concept['extras']['source_data_elements']:
            source_de_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                owner_id, source_id, source_linkage['source_data_element_uid'])
            if source_de_url not in dde_source_linkages[de_concept['__url']]:
                dde_source_linkages[de_concept['__url']].append(source_de_url)
    return dde_source_linkages


def build_ref_indicator_to_child_resource_maps(child_concepts=None, sorted_ref_indicator_codes=None,
                                               org_id='', source_id=''):
    """
    Return dictionary with reference indicator URL as key and list of child concept URLs as value.
    Compatible with DATIM data elements, iHUB derived data elements, DATIM indicators, or any other
    list of resources with an 'indicator' custom attribute specifying the mapped reference indicator
    code and a '__url' core attribute. Child resources that with an unrecognized or missing
    reference indicator code are omitted.
    """
    map_indicator_to_child_resource = {}
    for child_concept in child_concepts:
        if ('indicator' in child_concept['extras'] and
                child_concept['extras']['indicator'] in sorted_ref_indicator_codes):
            de_indicator_code = child_concept['extras']['indicator']
            indicator_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, de_indicator_code)
            if indicator_concept_url not in map_indicator_to_child_resource:
                map_indicator_to_child_resource[indicator_concept_url] = []
            map_indicator_to_child_resource[indicator_concept_url].append(child_concept['__url'])
    return map_indicator_to_child_resource


def build_de_to_coc_maps(de_concepts, coc_concepts, org_id, source_id):
    """ Return dictionary with DE URL as key and list of COC URLs as value """
    map_de_to_coc = {}
    for de_concept in de_concepts:
        if de_concept['__url'] not in map_de_to_coc:
            map_de_to_coc[de_concept['__url']] = []
        for coc_raw in de_concept['__cocs']:
            coc_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, coc_raw['id'])
            coc_concept = coc_concepts.get_resource_by_url(coc_concept_url)
            if not coc_concept:
                raise Exception("Houston, we've got a problem. COC not found: %s" % coc_concept_url)
            map_de_to_coc[de_concept['__url']].append(coc_concept_url)
    return map_de_to_coc


def build_codelist_to_de_map(codelist_collections, de_concepts, org_id, source_id):
    """
    Returns dictionary with Codelist ID (eg GiqB9vjbdwb) as top-level key, DE URL as 2nd-level key,
    and list of COC URLs as value.
    """
    map_codelist_to_de_to_coc = {}
    for codelist in codelist_collections:
        codelist_id = codelist['external_id']
        map_codelist_to_de_to_coc[codelist_id] = {}
        for row in codelist['extras']['dhis2_codelist']['listGrid']['rows']:
            de_uid = row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID]
            coc_uid = row[DATIM_CODELIST_COLUMN_COC_CODE]
            de_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_uid)
            coc_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, coc_uid)
            if de_url not in map_codelist_to_de_to_coc[codelist_id]:
                map_codelist_to_de_to_coc[codelist_id][de_url] = []
            if coc_url not in map_codelist_to_de_to_coc[codelist_id][de_url]:
                map_codelist_to_de_to_coc[codelist_id][de_url].append(coc_url)
    return map_codelist_to_de_to_coc


def get_ihub_rule_applicable_periods(ihub_row, ihub_rule_period_end_year):
    """
    Return a list of strings representing the period of validity for a iHUB derived data element.
    For example: ["FY18", "FY19"]

    The first target period or quarter period that the rule began.
        Uses format YYYY0000 = Target or annual data; YYYY0100 is Q1, YYYY0400 is Q4, etc
    The last period the rule is valid or 99990400 represents no end date
    """
    begin_year, begin_quarter = parse_ihub_rule_period(ihub_row[IHUB_COLUMN_RULE_BEGIN_PERIOD])
    end_year, end_quarter = parse_ihub_rule_period(ihub_row[IHUB_COLUMN_RULE_END_PERIOD])
    if end_year == '9999':
        end_year = ihub_rule_period_end_year
    applicable_periods = []
    for fiscal_year in range(int(begin_year[2:4]), int(end_year[2:4]) + 1):
        applicable_periods.append('FY%s' % str(fiscal_year))
    return applicable_periods


def parse_ihub_rule_period(ihub_rule_period):
    """
    Parses a start or stop period of validity where the format is "YYYYQQ00".
    A value of "YYYY0000" = Target or annual data and "YYYY0100" is Q1,
    "YYYY0400" is Q4, etc. "99990400" represents no end date. Returns a
    set of ("YYYY", "QQ").
    """
    if len(ihub_rule_period) != 8:
        raise Exception('Invalid iHUB rule period: %s' % str(ihub_rule_period))
    return ihub_rule_period[:4], ihub_rule_period[4:6]


def get_data_element_name_modifiers(de_name):
    """
    Extract the modifier portion of a iHUB derived data element name.
    For example, 'D, DSD' would be returned this name:
    'CXCA_SCRN (D, DSD) TARGET: Receiving ART'
    """
    result = re.search(r'^[a-zA-Z_]+? \((.*?)\)', de_name)
    if result:
        return result.group(1)
    return ''


def build_all_ihub_dde_concepts(ihub_raw, num_run_sequences=3, org_id='', source_id='',
                                sorted_ref_indicator_codes=None, ref_indicator_concepts=None,
                                ihub_rule_period_end_year=2020):
    """
    Returns dictionary with unique DDE URL as key and DDE concept as value.
    Iterates thru iHUB rows once per run sequence. The first run sequence relies
    only on DATIM data elements, whereas subsequent run sequences rely on data
    elements derived in a previous run sequence. Run sequences are defined
    explicitly in the iHUB source data.
    """
    ihub_dde_concepts = {}
    for i in range(num_run_sequences):
        current_run_sequence_str = str(i + 1)
        for ihub_row in ihub_raw:
            # Skip data elements directly from DATIM or in a different run sequence
            if (ihub_row[IHUB_COLUMN_SOURCE_KEY] == IHUB_COLUMN_SOURCE_KEY_DATIM or
                    ihub_row[IHUB_COLUMN_RUN_SEQUENCE] != current_run_sequence_str):
                continue

            # Build the iHUB derived data element (DDE) concept
            dde_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID])
            if dde_concept_url not in ihub_dde_concepts:
                dde_concept = build_concept_from_ihub_dde(
                    ihub_row, org_id, source_id, sorted_ref_indicator_codes,
                    ref_indicator_concepts, ihub_rule_period_end_year)
                ihub_dde_concepts[dde_concept_url] = dde_concept

            # Set the current source DE/COC to the DDE's custom attribute
            dde_concept['extras']['source_data_elements'].append({
                IHUB_COLUMN_DERIVED_COC_UID: ihub_row[IHUB_COLUMN_DERIVED_COC_UID],
                IHUB_COLUMN_DERIVED_COC_NAME: ihub_row[IHUB_COLUMN_DERIVED_COC_NAME],
                IHUB_COLUMN_SOURCE_DATA_ELEMENT_UID: ihub_row[IHUB_COLUMN_SOURCE_DATA_ELEMENT_UID],
                IHUB_COLUMN_SOURCE_DATA_ELEMENT_NAME: ihub_row[IHUB_COLUMN_SOURCE_DATA_ELEMENT_NAME],
                IHUB_COLUMN_SOURCE_DISAGGREGATE: ihub_row[IHUB_COLUMN_SOURCE_DISAGGREGATE],
                IHUB_COLUMN_SOURCE_COC_UID: ihub_row[IHUB_COLUMN_SOURCE_COC_UID],
                IHUB_COLUMN_SOURCE_COC_NAME: ihub_row[IHUB_COLUMN_SOURCE_COC_NAME],
                IHUB_COLUMN_ADD_OR_SUBTRACT: ihub_row[IHUB_COLUMN_ADD_OR_SUBTRACT],
            })

            # Store the COC mapping
            ihub_derived_coc_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, ihub_row[IHUB_COLUMN_DERIVED_COC_UID])
            if ihub_derived_coc_url not in ihub_dde_concepts[dde_concept_url]['__cocs']:
                ihub_dde_concepts[dde_concept_url]['__cocs'].append(ihub_derived_coc_url)

    return ihub_dde_concepts


def build_concept_from_ihub_dde(ihub_row, org_id, source_id, sorted_ref_indicator_codes,
                                ref_indicator_concepts, ihub_rule_period_end_year):
    """ Return an OCL-formatted concept for the specified iHUB derived data element """
    de_applicable_periods = get_ihub_rule_applicable_periods(ihub_row, ihub_rule_period_end_year)
    de_result_or_target = ihub_row[IHUB_COLUMN_RESULT_TARGET].lower().capitalize()
    dde_concept = {
        'type': 'Concept',
        'id': ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID],
        'concept_class': 'Data Element',
        'datatype': 'Numeric',
        'owner': org_id,
        'owner_type': 'Organization',
        'source': source_id,
        'retired': False,
        'external_id': ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID],
        'descriptions': None,
        'extras': {
            'source': 'iHUB',
            ATTR_RESULT_TARGET: de_result_or_target,
            'ihub_indicator_code': ihub_row.get(IHUB_COLUMN_INDICATOR, ''),
            'disaggregate': ihub_row[IHUB_COLUMN_DISAGGREGATE],
            'standardized_disaggregate': ihub_row[IHUB_COLUMN_STANDARDIZED_DISAGGREGATE],
            'ihub_derivation_rule_id': ihub_row[IHUB_COLUMN_RULE_ID],
            'ihub_rule_begin_period': ihub_row[IHUB_COLUMN_RULE_BEGIN_PERIOD],
            'ihub_rule_end_period': ihub_row[IHUB_COLUMN_RULE_END_PERIOD],
            ATTR_APPLICABLE_PERIODS: de_applicable_periods,
            ATTR_STRUCTURED_DATASET: 'MER',
            'source_data_elements': []
        },
        'names': [
            {
                'name': ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME],
                'name_type': 'Fully Specified',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            }
        ]
    }

    # Determine mapped reference indicator code
    dde_standard_indicator_code = lookup_reference_indicator_code(
        resource_name=ihub_row.get(IHUB_COLUMN_INDICATOR, ''),
        resource_applicable_periods=de_applicable_periods,
        sorted_ref_indicator_codes=sorted_ref_indicator_codes,
        ref_indicator_concepts=ref_indicator_concepts)

    # Determine DE reporting frequency (needs name, indicator, result/target, & period)
    de_reporting_frequency = get_de_reporting_frequency(
        de_name=ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME],
        de_indicator_code=dde_standard_indicator_code,
        de_result_or_target=de_result_or_target,
        de_applicable_periods=de_applicable_periods,
        ref_indicator_concepts=ref_indicator_concepts)

    # Set DE custom attributes
    dde_version = get_ihub_dde_version(ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    dde_numerator_or_denominator = get_ihub_dde_numerator_or_denominator(
        ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    dde_support_type = get_ihub_dde_support_type(
        ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    if dde_version:
        dde_concept['extras']['data_element_version'] = dde_version
    if dde_numerator_or_denominator:
        dde_concept['extras'][ATTR_NUMERATOR_DENOMINATOR_TYPE] = dde_numerator_or_denominator
    if dde_support_type:
        dde_concept['extras'][ATTR_PEPFAR_SUPPORT_TYPE] = dde_support_type
    if dde_standard_indicator_code:
        dde_concept['extras']['indicator'] = dde_standard_indicator_code
    if de_reporting_frequency:
        dde_concept['extras'][ATTR_REPORTING_FREQUENCY] = de_reporting_frequency

    # Set throwaway attributes
    dde_concept['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (
        org_id, source_id, ihub_row[IHUB_COLUMN_DERIVED_DATA_ELEMENT_UID])
    dde_concept['__cocs'] = []

    return dde_concept


def build_ihub_dde_to_coc_maps(ihub_dde_concepts, coc_concepts=None):
    """
    Return dictionary with DDE URL as key and list of COC URLs as value.
    If coc_concepts provided, validates that each derived COC is present
    in the coc_concepts list.
    """
    map_ihub_dde_to_coc = {}
    for ihub_dde_concept in ihub_dde_concepts:
        map_ihub_dde_to_coc[ihub_dde_concept['__url']] = []
        for coc_url in ihub_dde_concept['__cocs']:
            coc_concept = coc_concepts.get_resource_by_url(coc_url)
            if not coc_concept:
                err_msg = 'ERROR: COC for derived data element not found:' % coc_url
                raise Exception(err_msg)
            if coc_url not in map_ihub_dde_to_coc[ihub_dde_concept['__url']]:
                map_ihub_dde_to_coc[ihub_dde_concept['__url']].append(coc_url)
    return map_ihub_dde_to_coc


def get_datim_codelist_stats(codelist_datim):
    """
    Returns counts of rows, data elements and category option combos in the DATIM codelist
    """
    unique_data_element_ids = {}
    unique_coc_ids = {}
    for row in codelist_datim['listGrid']['rows']:
        unique_data_element_ids[row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID]] = True
        unique_coc_ids[row[DATIM_CODELIST_COLUMN_COC_UID]] = True
    return {
        'Total Rows': len(codelist_datim['listGrid']['rows']),
        'Unique Data Element IDs': len(unique_data_element_ids),
        'Unique COC IDs:': len(unique_coc_ids),
    }


def diff_codelist(codelist_ocl=None, codelist_datim=None):
    """ Return a diff evaluated between two codelists, one from OCL and one from DATIM """
    diff = {
        'missing_in_ocl_de': [],
        'missing_in_ocl_coc': [],
        'missing_in_ocl_mapping': [],
        'too_many_in_ocl_de': [],
        'too_many_in_ocl_coc': [],
        'too_many_in_ocl_mapping': [],
        'missing_in_datim_codelist': []
    }

    # Iterate thru the DATIM list
    for row in codelist_datim['listGrid']['rows']:
        # Find the Data Element
        de_concepts = codelist_ocl.get_concepts(core_attrs={
            'id': row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_CODE],
            'external_id': row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID],
            'concept_class': 'Data Element'
        })
        if len(de_concepts) == 0:
            diff['missing_in_ocl_de'].append(row)
        elif len(de_concepts) > 1:
            diff['too_many_in_ocl_de'].append(row)

        # Find the COC
        coc_concepts = codelist_ocl.get_concepts(core_attrs={
            'id': row[DATIM_CODELIST_COLUMN_COC_CODE],
            'external_id': row[DATIM_CODELIST_COLUMN_COC_UID],
            'concept_class': 'Category Option Combo'
        })
        if len(coc_concepts) == 90:
            diff['missing_in_ocl_coc'].append(row)
        elif len(coc_concepts) > 1:
            diff['too_many_in_ocl_coc'].append(row)

        # Find the mapping
        if de_concepts and coc_concepts:
            de_to_coc_mappings = codelist_ocl.get_mappings(
                from_concept_uri=de_concepts[0]['url'],
                to_concept_uri=coc_concepts[0]['url'],
                map_type=MSP_MAP_TYPE_DE_TO_COC)
            # print('  from_concept:', de_concepts[0]['url'])
            # print('  to_concept:', coc_concepts[0]['url'])
            # print('  result:', de_to_coc_mappings)
            if len(de_to_coc_mappings) == 0:
                diff['missing_in_ocl_mapping'].append(row)
            elif len(de_to_coc_mappings) > 1:
                diff['too_many_in_ocl_mapping'].append(row)
        else:
            diff['missing_in_ocl_mapping'].append(row)

    # Now iterate through the OCL collection
    for de_concept in codelist_ocl.get_concepts(core_attrs={'concept_class': 'Data Element'}):
        for mapping in codelist_ocl.get_mappings(
                from_concept_uri=de_concept['url'], map_type=MSP_MAP_TYPE_DE_TO_COC):
            coc_concept = codelist_ocl.get_concept_by_uri(mapping['to_concept_url'])
            found_matching_row = False
            if coc_concept:
                for row in codelist_datim['listGrid']['rows']:
                    if (row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_CODE] == de_concept['id'] and
                            row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID] == de_concept[
                                'external_id'] and
                            row[DATIM_CODELIST_COLUMN_COC_CODE] == coc_concept['id'] and
                            row[DATIM_CODELIST_COLUMN_COC_UID] == coc_concept['external_id']):
                        found_matching_row = True
                        break
            if not found_matching_row:
                diff['missing_in_datim_codelist'].append(mapping)
                # diff['missing_in_datim_codelist'].append({
                #     'from_concept': de_concept, 'to_concept': coc_concept, 'mapping': mapping})

    # Remove empty diff keys and return
    return {k: v for k, v in diff.items() if v}
