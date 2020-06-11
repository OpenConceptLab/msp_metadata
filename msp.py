"""
Common functionality used in the MSP ETL scripts used to prepare and import metadata from
MER guidance, DATIM, PDH, and related systems.
"""
import json
import pprint
import csv
import datetime
import re
import requests
import ocldev.oclcsvtojsonconverter
import ocldev.oclconstants


# Constants for OCL mappings
MSP_MAP_TYPE_INDICATOR_TO_DE = 'Has Data Element'
MSP_MAP_TYPE_DE_TO_COC = 'Has Option'
MSP_MAP_TYPE_REPLACES = 'Replaces'
MSP_MAP_TYPE_DERIVED_FROM = 'Derived From'
MSP_MAP_TYPES = [
    MSP_MAP_TYPE_INDICATOR_TO_DE,
    MSP_MAP_TYPE_DE_TO_COC,
    MSP_MAP_TYPE_REPLACES,
]

# Constants for PDH source spreadsheet
PDH_COLUMN_INDICATOR = 'indicator'
PDH_COLUMN_SOURCE_KEY = 'source_srgt_key'
PDH_COLUMN_DISAGGREGATE = 'disaggregate'
PDH_COLUMN_STANDARDIZED_DISAGGREGATE = 'standardized_disaggregate'
PDH_COLUMN_DERIVED_DATA_ELEMENT_UID = 'derived_data_element_uid'
PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME = 'derived_data_element_name'
PDH_COLUMN_DERIVED_COC_UID = 'derived_category_option_combo'
PDH_COLUMN_DERIVED_COC_NAME = 'derived_category_option_combo_name'
PDH_COLUMN_SOURCE_DATA_ELEMENT_UID = 'source_data_element_uid'
PDH_COLUMN_SOURCE_DATA_ELEMENT_NAME = 'source_data_element_name'
PDH_COLUMN_SOURCE_DISAGGREGATE = 'source_disaggregate'
PDH_COLUMN_SOURCE_COC_UID = 'source_category_option_combo_uid'
PDH_COLUMN_SOURCE_COC_NAME = 'source_category_option_combo_name'
PDH_COLUMN_RULE_BEGIN_PERIOD = 'rule_begin_period'
PDH_COLUMN_RULE_END_PERIOD = 'rule_end_period'
PDH_COLUMN_ADD_OR_SUBTRACT = 'add_or_subtract'
PDH_COLUMN_RESULT_TARGET = 'result_target'
PDH_COLUMN_RUN_SEQUENCE = 'Derived_level_run_seq'
PDH_COLUMN_RULE_ID = 'rule_id'
PDH_COLUMNS = [
    PDH_COLUMN_INDICATOR,
    PDH_COLUMN_SOURCE_KEY,
    PDH_COLUMN_DISAGGREGATE,
    PDH_COLUMN_STANDARDIZED_DISAGGREGATE,
    PDH_COLUMN_DERIVED_DATA_ELEMENT_UID,
    PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME,
    PDH_COLUMN_DERIVED_COC_UID,
    PDH_COLUMN_DERIVED_COC_NAME,
    PDH_COLUMN_SOURCE_DATA_ELEMENT_UID,
    PDH_COLUMN_SOURCE_DATA_ELEMENT_NAME,
    PDH_COLUMN_SOURCE_DISAGGREGATE,
    PDH_COLUMN_SOURCE_COC_UID,
    PDH_COLUMN_SOURCE_COC_NAME,
    PDH_COLUMN_RULE_BEGIN_PERIOD,
    PDH_COLUMN_RULE_END_PERIOD,
    PDH_COLUMN_ADD_OR_SUBTRACT,
    PDH_COLUMN_RESULT_TARGET,
    PDH_COLUMN_RUN_SEQUENCE,
    PDH_COLUMN_RULE_ID,
]
PDH_COLUMN_SOURCE_KEY_DATIM = '1'
PDH_COLUMN_SOURCE_KEY_PDH = '2'

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

# Constants for MSP collections
COLLECTION_NAME_DATIM = 'DATIM'
COLLECTION_NAME_PDH = 'PDH'
COLLECTION_NAME_MER = 'MER'
COLLECTION_NAMES = [
    COLLECTION_NAME_DATIM,
    COLLECTION_NAME_PDH,
    COLLECTION_NAME_MER,
]

# Constants for data element custom attributes
ATTR_APPLICABLE_PERIODS = 'Applicable Periods'
ATTR_PERIOD = 'Period'


def display_input_metadata_summary(verbosity=1, input_periods=None, datim_indicators=None,
                                   reference_indicators=None, datim_de_all=None,
                                   datim_cocs=None, codelists=None, pdh_raw=None):
    """ Displays summary of the loaded metadata """
    print 'MSP Metadata Statistics %s\n' % datetime.datetime.now().strftime("%Y-%m-%d")
    print 'METADATA SOURCES:'
    print '  Input Periods:', input_periods
    if reference_indicators:
        print '  MER Reference Indicators (FY16-20):', len(reference_indicators)
        if verbosity >= 2:
            for period in input_periods:
                print '    [%s]:' % period
                for ref_indicator in reference_indicators:
                    if ref_indicator['extras'][ATTR_PERIOD] == period:
                        if verbosity == 2:
                            print '      %s' % (ref_indicator['id'])
                        elif verbosity > 2:
                            print '      %s: %s' % (ref_indicator['id'], ref_indicator)
            print ''
    if datim_indicators:
        print '  DATIM Indicators (All):', len(datim_indicators)
    if datim_de_all:
        print '  DATIM Data Elements (All):', len(datim_de_all['dataElements'])
    if datim_cocs:
        print '  DATIM Category Option Combos (All):', len(datim_cocs['categoryOptionCombos'])
    if pdh_raw:
        print '  PDH Rows:', len(pdh_raw)
    if codelists:
        print '  DATIM Code Lists (FY16-20):', len(codelists)
        if verbosity >= 2:
            for codelist in codelists:
                print '    [%s]  %s (%s)' % (
                    codelist['external_id'],
                    codelist['id'],
                    codelist['extras'][ATTR_APPLICABLE_PERIODS])
            print ''


def display_processing_summary(
        verbosity=1, codelists=None, datim_indicator_concepts=None, sorted_indicator_codes=None,
        coc_concepts=None, de_concepts=None, map_indicator_to_de=None, map_de_to_coc=None,
        map_codelist_to_de=None, map_pdh_dde_to_coc=None, map_indicator_to_pdh_dde=None,
        org_id='', source_id=''):
    """ Display a summary of the result of processing DATIM metadata """
    print '\nSUMMARY OF RESULTS FROM PROCESSING METADATA:'

    # Unique Indicator Codes
    if sorted_indicator_codes:
        print '  Unique Reference Indicator Codes across FY16-20:', len(sorted_indicator_codes)
        if verbosity >= 2:
            for indicator_code in sorted_indicator_codes:
                print '    ', indicator_code

    # DEs and matched periods
    print '  Periods mapped to DATIM Data Elements via Code Lists:',
    print get_applicable_periods_from_de_concepts(de_concepts)
    print '  Unique COC concepts:', len(coc_concepts)
    print '  DATIM DE concepts: %s data elements with %s unique COC maps' % get_dict_child_counts(
        map_de_to_coc)
    print '  PDH DDE concepts:',
    print '%s derived data elements with %s unique COC maps' % get_dict_child_counts(
        map_pdh_dde_to_coc)

    # DATIM Indicators
    print '  DATIM Indicators:', len(datim_indicator_concepts)

    # Codelists
    if verbosity >= 2:
        print ''
    print '  Code Lists mapped to DATIM Data Elements: %s out of %s' % (
        str(len(map_codelist_to_de)), str(len(codelists)))
    if verbosity >= 2:
        for codelist in codelists:
            if codelist['external_id'] in map_codelist_to_de:
                if verbosity >= 3:
                    print '   ', codelist
                else:
                    print '    [%s] %s: %s data elements' % (
                        codelist['external_id'], codelist['id'],
                        len(map_codelist_to_de[codelist['external_id']]))
        if len(codelists) - len(map_codelist_to_de):
            print '\n  Code Lists with No Data Elements:', len(codelists) - len(map_codelist_to_de)
            for codelist in codelists:
                if codelist['external_id'] not in map_codelist_to_de:
                    print '    ', codelist

    # Summary for indicator-->DATIM DE maps
    if verbosity >= 2:
        print ''
    (num_indicator_codes, num_indicator_to_de_maps) = get_dict_child_counts(
        map_indicator_to_de)
    print '  Reference Indicator Codes mapped to DATIM Data Elements:',
    print '%s of %s reference indicator codes, %s Reference indicator-->DE maps' % (
        num_indicator_codes, len(sorted_indicator_codes), num_indicator_to_de_maps)
    if verbosity >= 2:
        for indicator_url in map_indicator_to_de:
            if map_indicator_to_de[indicator_url]:
                indicator_code = indicator_url.split('/')[6]
                print '    %s (%s)' % (indicator_code, len(map_indicator_to_de[indicator_url]))
    if verbosity >= 2:
        print ''
    print '  Reference Indicator Codes with no DATIM DE Maps: %s Reference indicator codes' % str(
        len(sorted_indicator_codes) - len(map_indicator_to_de))
    if verbosity >= 2:
        for indicator_code in sorted_indicator_codes:
            indicator_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, indicator_code)
            if indicator_url not in map_indicator_to_de:
                print '    %s' % indicator_code

    # Summary for indicator-->DDE maps
    if verbosity >= 2:
        print ''
    (num_indicator_codes, num_indicator_to_dde_maps) = get_dict_child_counts(
        map_indicator_to_pdh_dde)
    print '  Reference Indicator Codes mapped to PDH Derived Data Elements:',
    print '%s of %s Reference indicator codes, %s Reference indicator-->DDE maps' % (
        num_indicator_codes, len(sorted_indicator_codes), num_indicator_to_dde_maps)
    if verbosity >= 2:
        for indicator_url in map_indicator_to_pdh_dde:
            if map_indicator_to_pdh_dde[indicator_url]:
                indicator_code = indicator_url.split('/')[6]
                print '    %s (%s)' % (
                    indicator_code, len(map_indicator_to_pdh_dde[indicator_url]))
    if verbosity >= 2:
        print ''

    # Summary for reference indicators not mapped to PDH DDEs
    standard_reference_indicators_not_mapped = []
    for indicator_code in sorted_indicator_codes:
        indicator_url = '/orgs/%s/sources/%s/concepts/%s/' % (
            org_id, source_id, indicator_code)
        if indicator_url not in map_indicator_to_pdh_dde:
            standard_reference_indicators_not_mapped.append(indicator_code)
    print '  Reference Indicator Codes with no PDH DDE Maps: %s indicator codes' % str(
        len(standard_reference_indicators_not_mapped))
    if verbosity >= 2:
        for indicator_code in standard_reference_indicators_not_mapped:
            print '    %s' % indicator_code


def display_references_summary(
        codelist_references=None,
        datim_references=None, datim_indicator_references=None,
        pdh_references=None, pdh_indicator_references=None,
        mer_references=None, mer_indicator_references=None):
    """ Displays a summary of the specified references """

    # Codelist
    print '\n  Codelist References: %s batches and %s expressions\n' % (
        len(codelist_references), count_reference_expressions(codelist_references))
    print '  DATIM References:'

    # DATIM
    for period in datim_references:
        print '    %s: %s DE/COC batches and %s expressions' % (
            period, len(datim_references[period]),
            count_reference_expressions(datim_references[period]))
        print '    %s: %s Indicator batches and %s expressions' % (
            period, len(datim_indicator_references[period]),
            count_reference_expressions(datim_indicator_references[period]))

    # PDH
    print '  PDH references:'
    for period in pdh_references:
        print '    %s: %s DE/COC batches and %s expressions' % (
            period, len(pdh_references[period]),
            count_reference_expressions(pdh_references[period]))
        print '    %s: %s Indicator batches and %s expressions' % (
            period, len(pdh_indicator_references[period]),
            count_reference_expressions(pdh_indicator_references[period]))

    # MER
    print '  MER references:'
    for period in mer_references:
        print '    %s: %s DE/COC batches and %s expressions' % (
            period, len(mer_references[period]),
            count_reference_expressions(mer_references[period]))
        print '    %s: %s Indicator batches and %s expressions' % (
            period, len(mer_indicator_references[period]),
            count_reference_expressions(mer_indicator_references[period]))


def display_linkages_summary(verbosity=1, de_version_linkages=None,
                             map_de_version_linkages=None,
                             map_dde_source_linkages=None):
    """ Display summary of linkages between data elements """

    # Summary for DE version linkages
    print '\nRESULTS OF GENERATING LINKAGES BETWEEN DATA ELEMENTS:'
    print '  Data Element Version Links (DATIM and PDH):'
    print '    %s DEs replaced %s DEs' % get_dict_child_counts(map_de_version_linkages)
    if verbosity >= 2:
        for de_code in de_version_linkages:
            print '      %s' % de_code
            for de_version in de_version_linkages[de_code]:
                print '        %s: %s (%s)' % (
                    de_version['sort_order'], de_version['code'], de_version['url'])

    # Summary for DE source-derivation linkages
    print '\n  PDH Data Element Source-Derivation Linkages:'
    print '    %s derived data elements linked to %s source data elements' % get_dict_child_counts(
        map_dde_source_linkages)
    print '    NOTE: Source-derivation linkages are defined between data elements only, not COCs'


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


def get_primary_source(org_id, source_id):
    """ Returns OCL-formatted JSON for the PEPFAR MER source """
    return get_new_repo_json(
        owner_id=org_id, repo_id=source_id, name="MER Source",
        full_name="DATIM Monitoring, Evaluation & Results Metadata")


def get_new_repo_json(owner_type='Organization', owner_id='', repo_type='Source', repo_id='',
                      name='', full_name='', repo_sub_type='Dictionary', default_locale='en',
                      public_access='View', supported_locales='en'):
    """ Returns OCL-formatted JSON for a source """
    return {
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
        "supported_locales": supported_locales,
    }


def load_datim_data_elements(filename=''):
    """
    Loads DATIM data elements from raw JSON file retrieved directly from DHIS2.
    COCs and datasets are included as attributes of each data element.
    """
    with open(filename, 'rb') as input_file:
        raw_datim_de_all = json.load(input_file)
    return raw_datim_de_all


def load_datim_cocs(filename=''):
    """
    Loads DATIM categoryOptionCombos from raw JSON file retrieved directly from DHIS2.
    """
    with open(filename, 'rb') as input_file:
        raw_datim_cocs = json.load(input_file)
    return raw_datim_cocs


def fetch_datim_codelist(url_datim):
    """ Fetches a public codelist from DATIM (not from OCL). Used by validation script. """
    codelist_response = requests.get(url_datim)
    codelist_response.raise_for_status()
    return codelist_response.json()


def load_codelists(filename='', org_id=''):
    """ Loads codelists and returns as OCL-formatted JSON """
    codelists = []
    with open(filename) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            # Skip rows that are not set to be imported
            if not row['resource_type']:
                continue
            row['id'] = format_identifier(unformatted_id=row['id'])
            row['owner_id'] = org_id
            codelists.append(row)

    # Convert CSV records to OCL-formatted JSON
    csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(
        input_list=codelists)
    converted_codelists = csv_converter.process()
    return converted_codelists


def load_datim_indicators(filename=''):
    """ Loads DHIS2-formatted DATIM indicators and return without transforming """
    with open(filename, 'rb') as input_file:
        dhis2_indicators = json.load(input_file)
    return dhis2_indicators['indicators']


def load_reference_indicators(org_id='', source_id='', filenames=None):
    """ Loads reference indicators from MER guidance as OCL-formatted JSON """
    if not filenames:
        return []
    reference_indicators = []
    for filename in filenames:
        with open(filename) as ifile:
            reader = csv.DictReader(ifile)
            for row in reader:
                row['owner_id'] = org_id
                row['source'] = source_id
                reference_indicators.append(row)

    # Convert indicator CSV records to OCL-formatted JSON
    csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(
        input_list=reference_indicators)
    converted_reference_indicators = csv_converter.process()

    # Add throw-away attributes
    for indicator in converted_reference_indicators:
        indicator['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (
            org_id, source_id, indicator['id'])

    return converted_reference_indicators


def load_pdh(filename=''):
    """ Loads PDH extract and returns in raw format """
    pdh = []
    with open(filename) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            pdh.append(row)
    return pdh


def get_pdh_dde_numerator_or_denominator(de_name):
    """
    Returns 'Numerator' or 'Denominator', respectively, if 'N' or 'D' is
    present in the data element modifiers (ie, in between parentheses).
    """
    if '(N,' in de_name or '(N)' in de_name:
        return 'Numerator'
    elif '(D,' in de_name or '(D)' in de_name:
        return 'Denominator'
    return ''


def get_pdh_dde_support_type(de_name):
    """
    Returns fully specified PEPFAR support type (eg 'Technical Assistance' or 'Direct Service
    Delivery') based on the presence of one of the acronyms in a PDH derived data element name.
    """
    de_modifiers = get_data_element_name_modifiers(de_name)
    if ', TA' in de_modifiers:
        return 'Technical Assistance'
    elif ', DSD' in de_modifiers:
        return 'Direct Service Delivery'
    return ''


def get_pdh_dde_version(de_name):
    """
    Returns data element version number if ' v#:' is present in the data element name.
    """
    if ' v2:' in de_name:
        return 'v2'
    elif ' v3:' in de_name:
        return 'v3'
    elif ' v4:' in de_name:
        return 'v4'
    elif ' v5:' in de_name:
        return 'v5'
    elif ' v6:' in de_name:
        return 'v6'
    elif ' v7:' in de_name:
        return 'v7'
    elif ' v8:' in de_name:
        return 'v8'
    elif ' v9:' in de_name:
        return 'v9'
    return ''


def get_pdh_dde_name_without_version(de_name):
    de_version = get_pdh_dde_version(de_name=de_name)
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


def get_data_element_support_type(de_code=''):
    """
    Returns fully specified PEPFAR support type (eg 'Technical Assistance' or 'Direct Service
    Delivery') based on the presence of one of the acronyms in a DATIM data element code.
    """
    if '_TA_' in de_code:
        return 'Technical Assistance'
    elif '_DSD_' in de_code:
        return 'Direct Service Delivery'
    return ''


def get_data_element_result_or_target(de_code=''):
    """ Returns 'Target' if the text is in the data element code, otherwise 'Result' """
    if 'target' in de_code.lower():
        return 'Target'
    return 'Result'


def lookup_indicator_code(de_code, sorted_indicator_codes):
    """
    Returns an indicator code that matches the prefix of the data element code.
    eg. TX_CURR would be returned for TX_CURR_N_DSD_Age_Sex. sorted_indicator_codes must be a
    list of indicator codes sorted by string length in descending order.
    """
    for indicator_code in sorted_indicator_codes:
        if de_code[:len(indicator_code)] == indicator_code:
            return indicator_code
    return ''


def get_sorted_unique_indicator_codes(reference_indicators=None):
    """
    Returns a list of unique sorted indicator codes given a list of
    OCL-formatted reference indicators
    """
    output = []
    for indicator in reference_indicators:
        if indicator['id'] not in output:
            output.append(indicator['id'])
    output.sort(key=len, reverse=True)
    return output


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

def get_de_periods_from_codelists(de_codelists, codelists):
    """ Get a list of the periods present in a data element's codelists """
    periods = {}
    for de_codelist in de_codelists:
        for codelist_def in codelists:
            if de_codelist['id'] == codelist_def['external_id']:
                for period in codelist_def['extras'][ATTR_APPLICABLE_PERIODS].split(', '):
                    periods[period] = True
                break
    return periods.keys()


def get_concepts_filtered_by_period(concepts=None, period=None):
    """
    Returns a list of concepts filtered by ATTR_PERIOD or ATTR_APPLICABLE_PERIODS
    custom attributes. Period filter may be a single period (eg 'FY18') or
    a list of periods (eg ['FY18', 'FY19']). Works with reference_indicators and data
    elements for both DATIM and PDH.
    """

    # Get period filter into the right format
    if isinstance(period, basestring):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, basestring) for item in period):
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


def get_filtered_codelists(codelists=None, period=None):
    """ Returns list of code lists filtered by either a single period or a list of periods """
    if isinstance(period, basestring):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, basestring) for item in period):
        pass
    else:
        return []
    filtered_codelists = []
    for codelist in codelists:
        if (period and ATTR_APPLICABLE_PERIODS in codelist['extras'] and
                any(codelist_period.strip() in period for codelist_period in codelist[
                    'extras'][ATTR_APPLICABLE_PERIODS].split(','))):
            filtered_codelists.append(codelist)
        elif not period:
            filtered_codelists.append(codelist)
    return filtered_codelists


def build_ocl_mappings(map_dict=None, filtered_from_concepts=None,
                       owner_type='Organization', owner_id='',
                       source_id='', map_type=''):
    """
    Returns a list of OCL-formatted mappings between from_concepts and to_concepts
    defined in map_dict. If filtered_from_concepts is provided, then maps are
    omitted if the from_concept is not in the filtered_from_concepts list. This
    method is designed to work with mappings between reference_indicators and data elements,
    and between data elements and COCs for DATIM and PDH.
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
            output_mappings.append(output_mapping)
    return output_mappings


def build_codelist_references(map_codelist_to_de=None, map_de_to_coc=None,
                              org_id='', codelists=None):
    """
    Returns a dictionary where keys are codelist IDs and values are a
    list of references for data element and COC concepts to be included
    in a codelist.
    """
    codelist_references = []
    for codelist_external_id in map_codelist_to_de:
        # Get the codelist ID (the "code" from DATIM)
        codelist_id = None
        for codelist in codelists:
            if codelist_external_id == codelist['external_id']:
                codelist_id = codelist['id']
                break

        # Get the references for the codelist ID
        if codelist_id:
            codelist_references += get_mapped_concept_references(
                from_concept_urls=map_codelist_to_de[codelist_external_id], map_dict=map_de_to_coc,
                org_id=org_id, collection_id=codelist_id)
    return codelist_references


def get_mapped_concept_references(from_concepts=None, from_concept_urls=None, map_dict=None,
                                  org_id='', collection_id='', include_to_concept_refs=True,
                                  ignore_from_concepts_with_no_maps=True):
    """
    Returns a list of references for the specified list of from concepts
    and, optionally, their mapped to concepts. Supports mappings for
    reference_indicators to data elements, and data elements to COCs for both DATIM
    and PDH. Must provide either from_concepts or from_concept_urls.
    """
    output_references = []
    ref_from_concept_expressions = []
    ref_to_concept_expressions = []

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
    elif isinstance(from_concepts, list) and isinstance(map_dict, dict):
        for from_concept in from_concepts:
            ref_from_concept_expressions.append(from_concept['__url'])
            if from_concept['__url'] not in map_dict and not ignore_from_concepts_with_no_maps:
                raise Exception(
                    'ERROR: from_concept_url not in map_dict: %s' % from_concept['__url'])
            if include_to_concept_refs:
                for to_concept_url in map_dict[from_concept['__url']]:
                    if to_concept_url not in ref_to_concept_expressions:
                        ref_to_concept_expressions.append(to_concept_url)
    elif isinstance(from_concept_urls, list) and isinstance(map_dict, dict):
        for from_concept_url in from_concept_urls:
            ref_from_concept_expressions.append(from_concept_url)
            if from_concept_url not in map_dict and not ignore_from_concepts_with_no_maps:
                raise Exception('ERROR: from_concept_url not in map_dict %s' % from_concept_url)
            if include_to_concept_refs:
                for to_concept_url in map_dict[from_concept_url]:
                    if to_concept_url not in ref_to_concept_expressions:
                        ref_to_concept_expressions.append(to_concept_url)
    else:
        raise Exception('Must provide map_dict and either from_concepts or from_concept_urls')

    # Build the from_concept and to_concept OCL-formatted references and return
    if ref_from_concept_expressions:
        # Cascade to source mappings for from_concept references
        output_references.append({
            'type': 'Reference', 'owner_type': 'Organization', 'owner': org_id,
            'collection': collection_id,
            'data': {'expressions': ref_from_concept_expressions},
            '__cascade': 'sourcemappings',
        })
    if ref_to_concept_expressions:
        output_references.append({
            'type': 'Reference', 'owner': org_id, 'owner_type': 'Organization',
            'collection': collection_id,
            'data': {'expressions': ref_to_concept_expressions},
        })
    return output_references


def get_mapped_concept_references_by_period(from_concepts=None, map_dict=None,
                                            org_id='', collection_id='', periods=None,
                                            include_to_concept_refs=False,
                                            include_all_period=False,
                                            ignore_from_concepts_with_no_maps=False):
    """
    Returns dictionary with period as key, list of OCL-formatted references for
    all specified concepts as value. If include_to_concept_refs is True, the
    "to concepts" that each "from concept" points to are also included. If
    include_all_period is True, a period with value of '*' is added that
    includes all passed concepts regardless of period. Supports reference_indicators mapped
    to both DATIM and PDH data elements and data elements mapped to COCs.
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


def build_mer_indicator_references(reference_indicators=None,
                                   map_indicator_to_de=None, map_indicator_to_pdh_dde=None,
                                   org_id='', collection_id='', periods=None,
                                   include_to_concept_refs=False, include_all_period=False):
    """
    Returns a dictionary with period as key, list of OCL-formatted references
    for all passed reference_indicators as value. Combines maps from reference_indicators to both
    DATIM and PDH data elements. If include_all_period is True, a period with
    value of '*' is added that spans all periods (and resources with no period).
    """
    combined_indicator_maps = map_indicator_to_de.copy()
    combined_indicator_maps.update(map_indicator_to_pdh_dde)
    combined_indicator_references = get_mapped_concept_references_by_period(
        from_concepts=reference_indicators, map_dict=combined_indicator_maps,
        org_id=org_id, collection_id=collection_id, periods=periods,
        include_to_concept_refs=include_to_concept_refs,
        include_all_period=include_all_period,
        ignore_from_concepts_with_no_maps=True)
    return combined_indicator_references


def build_mer_references(de_concepts=None, map_de_to_coc=None,
                         pdh_dde_concepts=None, map_pdh_dde_to_coc=None,
                         org_id='', collection_id='', periods=None,
                         include_to_concept_refs=True,
                         include_all_period=False):
    """
    Returns dictionary with period as key, list of OCL-formatted references
    for all specified data elements as value. Combines all DATIM and PDH data
    elements and their maps. If include_all_period is True, a period with
    value of '*' is added that includes all passed data elements.
    """

    # First combine the data elements and maps from the two sources
    combined_de_concepts = de_concepts.copy()
    combined_de_concepts.update(pdh_dde_concepts)
    combined_de_to_coc_maps = map_de_to_coc.copy()
    combined_de_to_coc_maps.update(map_pdh_dde_to_coc)

    # TODO: There's an unexpected overlap of 127 data elements between PDH and
    # DATIM DEs, which means that some DATIM DEs are referenced in a run_sequence
    # but not marked as sourced from DATIM
    # print '%d + %d = %d' % (len(de_concepts), len(pdh_dde_concepts), len(combined_de_concepts))
    # print '%d + %d = %d' % (
    #     len(map_de_to_coc), len(map_pdh_dde_to_coc), len(combined_de_to_coc_maps))

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


def get_codelists_formatted_for_display(codelists):
    """
    Output a python dictionary of codelist definitions formatted for display in MSP
    """
    output_codelists = []
    for codelist in codelists:
        output_codelist = {
            'id': codelist['id'],
            'name': codelist['name'],
            'full_name': codelist['full_name'],
            'periods': codelist['extras'][ATTR_APPLICABLE_PERIODS].split(', '),
            'codelist_type': codelist['extras']['Code List Type'],
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


def get_applicable_periods_from_de_concepts(de_concepts):
    """ Return list of unique ATTR_APPLICABLE_PERIODS from the specified DE concept list """
    matched_periods = []
    for de_concept_key in de_concepts:
        if ATTR_APPLICABLE_PERIODS in de_concepts[de_concept_key]['extras']:
            matched_periods = list(set().union(
                matched_periods, de_concepts[de_concept_key]['extras'][ATTR_APPLICABLE_PERIODS]))
    return matched_periods


def build_datim_indicator_concepts(datim_indicators, org_id, source_id,
                                   de_concepts=None, coc_concepts=None):
    """ """
    indicator_concepts = {}
    for indicator_raw in datim_indicators:
        indicator_concept = build_concept_from_datim_indicator(
            indicator_raw, org_id, source_id, de_concepts=de_concepts, coc_concepts=coc_concepts)
        indicator_concepts[indicator_concept['__url']] = indicator_concept
    return indicator_concepts


def build_concept_from_datim_indicator(indicator_raw, org_id, source_id,
                                       de_concepts=None, coc_concepts=None):
    """
    Return an OCL-formatted concept for the specified DATIM indicator.
    If de_concepts and coc_concepts arguments are provided, extra attributes are included for the
    numerator/denominator in which the UIDs have been with human-readable codes or names.
    """
    
    # Build the base DATIM indicator concept
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
            'numerator': indicator_raw.get('numerator', ''),
            'numeratorDescription': indicator_raw.get('numeratorDescription', ''),
            'numeratorReadableFormula': replace_formula_uids_with_names(
                formula=indicator_raw.get('numerator', ''), org_id=org_id, source_id=source_id,
                de_concepts=de_concepts, coc_concepts=coc_concepts),
            'dimensionItemType': indicator_raw['dimensionItemType'],
        },
        '__url': ocldev.oclconstants.OclConstants.get_resource_url(
            owner_id=org_id, repository_id=source_id, resource_id=indicator_raw['id'],
            include_trailing_slash=True)
    }
    if 'indicatorGroups' in indicator_raw and indicator_raw['indicatorGroups']:
        indicator_concept['extras']['indicatorGroups'] = indicator_raw['indicatorGroups']

    return indicator_concept


def replace_formula_uids_with_names(formula, org_id, source_id, de_concepts, coc_concepts):
    """ Replace UIDs in the numerator/denominator with human-readable codes or names """
    regex = r'(#\{(?P<deuid>(?:\S|\d){11})(?:\}|(?:.(?P<cocuid>(?:\S|\d){11}))\}))'
    matches = re.findall(regex, formula)
    new_formula = formula
    for (full_match, de_uid, coc_uid) in matches:
        de_concept_name = ''
        coc_concept_name = ''

        # Get the DE name
        de_url = ocldev.oclconstants.OclConstants.get_resource_url(
            owner_id=org_id, repository_id=source_id, resource_id=de_uid,
            include_trailing_slash=True)
        if de_url in de_concepts:
            de_concept_name = get_concept_name_by_type(
                de_concepts[de_url], ['Code', 'Short', 'Fully Specified'])
            if de_concept_name:
                de_concept_name = '[%s]' % de_concept_name
            else:
                de_concept_name = de_uid

        # Get the COC name
        if coc_uid:
            coc_url = ocldev.oclconstants.OclConstants.get_resource_url(
                owner_id=org_id, repository_id=source_id, resource_id=coc_uid,
                include_trailing_slash=True)
            if coc_url in coc_concepts:
                coc_concept_name = get_concept_name_by_type(
                    coc_concepts[coc_url], ['Code', 'Short', 'Fully Specified'])
            if coc_concept_name:
                coc_concept_name = '[%s]' % coc_concept_name
            else:
                coc_concept_name = coc_uid

        # Replace the match in the formula with the human-readable names
        if coc_uid:
            new_formula = new_formula.replace(full_match, '{%s.%s}' % (
                de_concept_name, coc_concept_name))
        else:
            new_formula = new_formula.replace(full_match, '{%s}' % de_concept_name)

    # Add whitespace around mathematical operators to improve readability
    new_formula = new_formula.replace('}+{', '} + {').replace('}-{', '} - {').replace('}*{', '} * {').replace('}/{', '} / {')

    return new_formula


def get_concept_name_by_type(concept, name_type):
    """ Get concept name by name_type or return None """
    if 'names' not in concept:
        return None
    if isinstance(name_type, str):
        name_type = [name_type]
    if not isinstance(name_type, list):
        raise Exception("Invalid name_type '%s'. Expected string or list." % type(name_type))
    for current_name_type in name_type:
        for concept_name in concept['names']:
            if 'name_type' in concept_name and concept_name['name_type'] == current_name_type:
                return concept_name['name']
    return None


def build_all_datim_coc_concepts(datim_cocs, org_id, source_id):
    """ Return dictionary of OCL-formatted concepts for all DATIM COCs """
    coc_concepts = {}
    for coc_raw in datim_cocs['categoryOptionCombos']:
        coc_concept = build_concept_from_datim_coc(coc_raw, org_id, source_id)
        coc_concepts[coc_concept['__url']] = coc_concept
    return coc_concepts


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


def build_all_datim_de_concepts(datim_de_all, org_id, source_id, sorted_indicator_codes, codelists):
    """ Return dictionary of OCL-formatted concepts for all DATIM Data Elements """
    de_concepts = {}
    for de_raw in datim_de_all['dataElements']:
        de_concept = build_concept_from_datim_de(
            de_raw, org_id, source_id, sorted_indicator_codes, codelists)
        de_concepts[de_concept['__url']] = de_concept
    return de_concepts


def build_concept_from_datim_de(de_raw, org_id, source_id, sorted_indicator_codes, codelists):
    """ Return an OCL-formatted concept for the specified DATIM data element """

    # Determine data element attributes
    de_concept_id = de_raw['id']  # eg sAxSUTFc5tp
    de_code = de_raw['code'] if 'code' in de_raw else de_raw['shortName']
    de_result_or_target = get_data_element_result_or_target(de_code=de_code)
    de_numerator_or_denominator = get_data_element_numerator_or_denominator(de_code=de_code)
    de_version = get_data_element_version(de_code=de_code)  # v2, v3, ...
    de_code_root = get_data_element_root(de_code=de_code)  # HTS_TST_AgeSex
    de_support_type = get_data_element_support_type(de_code=de_code)  # DSD, TA, ...

    # Build the concept
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
        },
    }

    # Handle DE code (not all DEs have codes)
    if 'code' in de_raw:
        de_concept['names'].append({
            'name': de_raw['code'],
            'name_type': 'Code',
            'locale': 'en',
            'locale_preferred': False,
            'external_id': None,
        })
        de_indicator_code = lookup_indicator_code(
            de_code=de_raw['code'],
            sorted_indicator_codes=sorted_indicator_codes)
        if de_indicator_code:
            de_concept['extras']['indicator'] = de_indicator_code

    # Handle DE description
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

    # Process DE's dataSets and codelists
    de_codelists = []
    if 'dataSetElements' in de_raw and de_raw['dataSetElements']:
        de_concept['extras']['dataSets'] = []
        for dataset in de_raw['dataSetElements']:
            de_concept['extras']['dataSets'].append(dataset['dataSet'])
            for codelist in codelists:
                if codelist['external_id'] == dataset['dataSet']['id']:
                    de_codelists.append(dataset['dataSet'])
                    break
        de_concept['extras']['codelists'] = de_codelists
        de_concept['extras'][ATTR_APPLICABLE_PERIODS] = get_de_periods_from_codelists(
            de_codelists=de_codelists, codelists=codelists)

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
        de_concept['extras']['data_element_version'] = de_version
    if de_numerator_or_denominator:
        de_concept['extras']['numeratorDenominator'] = de_numerator_or_denominator
    if de_support_type:
        de_concept['extras']['pepfarSupportType'] = de_support_type
    if de_result_or_target:
        de_concept['extras']['resultTarget'] = de_result_or_target

    # Add throw-away attributes that are used later in processing
    de_concept['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_concept_id)
    de_concept['__cocs'] = de_raw['categoryCombo']['categoryOptionCombos']

    return de_concept


def build_linkages_de_version(de_concepts=None):
    """ Get a dictionary of DEs that have multiple versions """

    # Build dictionary of DEs, grouping different versions of a DE together
    de_all_versions = {}
    for concept_url in de_concepts:
        if 'data_element_root' not in de_concepts[concept_url]['extras']:
            continue
        de_code = de_concepts[concept_url]['extras']['data_element_root']
        if de_code not in de_all_versions:
            de_all_versions[de_code] = []
        if 'data_element_version' in de_concepts[concept_url]['extras']:
            de_code += '_%s' % de_concepts[concept_url]['extras']['data_element_version']
        de_version = de_concepts[concept_url]['extras'].get('data_element_version', '')
        de_sort_order = 1 if not de_version else int(de_version[1:])
        de_all_versions[de_concepts[concept_url]['extras']['data_element_root']].append({
            'url': concept_url,
            'code': de_code,
            'version': de_version,
            'sort_order': de_sort_order,
        })

    # Only return those with more than one record
    de_filtered_versions = {}
    for de_code in de_all_versions:
        if len(de_all_versions[de_code]) > 1:
            de_filtered_versions[de_code] = sorted(
                de_all_versions[de_code], key = lambda i: i['sort_order'])
    return de_filtered_versions


def build_linkages_dde_version(pdh_dde_concepts=None):
    """ Get a dictionary of DDEs that have multiple versions """

    # Build dictionary of DEs, grouping different versions of a DE together
    de_all_versions = {}
    for de_concept_url in pdh_dde_concepts:
        de_name = pdh_dde_concepts[de_concept_url]['names'][0]['name']
        de_name_without_version = get_pdh_dde_name_without_version(de_name=de_name)
        if de_name_without_version not in de_all_versions:
            de_all_versions[de_name_without_version] = []
        de_version = pdh_dde_concepts[de_concept_url]['extras'].get('data_element_version', '')
        de_sort_order = 1 if not de_version else int(de_version[1:])
        de_all_versions[de_name_without_version].append({
            'url': de_concept_url,
            'code': de_name,
            'version': de_version,
            'sort_order': de_sort_order,
        })

    # Only return those with more than one record
    de_filtered_versions = {}
    for de_code in de_all_versions:
        if len(de_all_versions[de_code]) > 1:
            de_filtered_versions[de_code] = sorted(
                de_all_versions[de_code], key = lambda i: i['sort_order'])
    return de_filtered_versions


def build_maps_from_de_linkages(de_linkages=None, owner_id='', source_id=''):
    """ """
    map_de_linkages = {}
    for de_linkage_code in de_linkages:
        # de_replaced_to_concept = de_linkages[de_linkage_code][0]
        de_replaced_by_from_concept = de_linkages[de_linkage_code][0]
        for index in range(1, len(de_linkages[de_linkage_code])):
            de_replaced_to_concept = de_replaced_by_from_concept
            de_replaced_by_from_concept = de_linkages[de_linkage_code][index]
            if de_replaced_by_from_concept['url'] not in map_de_linkages:
                map_de_linkages[de_replaced_by_from_concept['url']] = []
            if de_replaced_to_concept['url'] not in map_de_linkages[de_replaced_by_from_concept['url']]:
                map_de_linkages[de_replaced_by_from_concept['url']].append(
                    de_replaced_to_concept['url'])
            # map_de_linkages.append({
            #     'type': 'Mapping',
            #     'owner': owner_id,
            #     'owner_type': 'Organization',
            #     'source': source_id,
            #     'map_type': MSP_MAP_TYPE_REPLACES,
            #     'from_concept_url': de_replaced_by_from_concept['url'],
            #     'to_concept_url': de_replaced_to_concept['url'],
            # })
    return map_de_linkages


def build_linkages_source_de(pdh_dde_concepts=None, owner_id='', source_id=''):
    """  """
    dde_source_linkages = {}
    for dde_concept_url in pdh_dde_concepts:
        if dde_concept_url not in dde_source_linkages:
            dde_source_linkages[dde_concept_url] = []
        for source_linkage in pdh_dde_concepts[dde_concept_url]['extras']['source_data_elements']:
            source_de_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                owner_id, source_id, source_linkage['source_data_element_uid'])
            if source_de_url not in dde_source_linkages[dde_concept_url]:
                dde_source_linkages[dde_concept_url].append(source_de_url)
    return dde_source_linkages


def build_indicator_to_de_maps(de_concepts, sorted_indicator_codes, org_id, source_id):
    """
    Dictionary with indicator_code as key and list of DE URLs as value.
    Note that data elements not associated with an indicator are omitted.
    """
    map_indicator_to_de = {}
    for de_concept_url in de_concepts:
        if ('indicator' in de_concepts[de_concept_url]['extras'] and
                de_concepts[de_concept_url]['extras']['indicator'] in sorted_indicator_codes):
            de_indicator_code = de_concepts[de_concept_url]['extras']['indicator']
            indicator_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, de_indicator_code)
            if indicator_concept_url not in map_indicator_to_de:
                map_indicator_to_de[indicator_concept_url] = []
            map_indicator_to_de[indicator_concept_url].append(de_concept_url)
    return map_indicator_to_de


def build_de_to_coc_maps(de_concepts, coc_concepts, org_id, source_id):
    """ Returns dictionary with DE URL as key and list of COC URLs as value """
    map_de_to_coc = {}
    for de_concept_url in de_concepts:
        for coc_raw in de_concepts[de_concept_url]['__cocs']:
            coc_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, coc_raw['id'])
            if de_concept_url not in map_de_to_coc:
                map_de_to_coc[de_concept_url] = []
            if coc_concept_url not in coc_concepts:
                raise Exception("Houston, we've got a problem. COC not found.")
            map_de_to_coc[de_concepts[de_concept_url]['__url']].append(coc_concept_url)
    return map_de_to_coc


def build_codelist_to_de_map(de_concepts):
    """ Returns dictionary with Codelist ID as key and list of DE URLs as value """
    map_codelist_to_de = {}
    for de_concept_url in de_concepts:
        if 'codelists' in de_concepts[de_concept_url]['extras']:
            for de_codelist in de_concepts[de_concept_url]['extras']['codelists']:
                if de_codelist['id'] not in map_codelist_to_de:
                    map_codelist_to_de[de_codelist['id']] = []
                map_codelist_to_de[de_codelist['id']].append(de_concept_url)
    return map_codelist_to_de


def get_pdh_rule_applicable_periods(pdh_row):
    """
    Return a string representing the period of validity for a PDH derived data element

    The first target period or quarter period that the rule began.
        Uses format YYYY0000 = Target or annual data; YYYY0100 is Q1, YYYY0400 is Q4, etc
    The last period the rule is valid or 99990400 represents no end date
    """
    begin_year, begin_quarter = parse_pdh_rule_period(pdh_row['rule_begin_period'])
    end_year, end_quarter = parse_pdh_rule_period(pdh_row['rule_end_period'])
    if end_year == '9999':
        end_year = '2020'
    applicable_periods = []
    for fiscal_year in range(int(begin_year[2:4]), int(end_year[2:4]) + 1):
        applicable_periods.append('FY%s' % str(fiscal_year))
    # return ', '.join(applicable_periods)
    return applicable_periods


def parse_pdh_rule_period(pdh_rule_period):
    """
    Parses a start or stop period of validity where the format is "YYYYQQ00".
    A value of "YYYY0000" = Target or annual data and "YYYY0100" is Q1,
    "YYYY0400" is Q4, etc. "99990400" represents no end date. Returns a
    set of ("YYYY", "QQ").
    """
    if len(pdh_rule_period) != 8:
        raise Exception('Invalid PDH rule period: %s' % str(pdh_rule_period))
    return pdh_rule_period[:4], pdh_rule_period[4:6]


def get_data_element_name_modifiers(de_name):
    """
    Extract the modifier portion of a PDH derived data element name.
    For example, 'D, DSD' would be returned this name:
    'CXCA_SCRN (D, DSD) TARGET: Receiving ART'
    """
    result = re.search(r'^[a-zA-Z_]+? \((.*?)\)', de_name)
    if result:
        return result.group(1)
    return ''


def build_all_pdh_dde_concepts(pdh_raw, pdh_num_run_sequences, org_id, source_id,
                               sorted_indicator_codes):
    """
    Returns dictionary with unique DDE URL as key and DDE concept as value.
    Iterates thru PDH rows once per run sequence. The first run sequence relies
    only on DATIM data elements, whereas subsequent run sequences rely on data
    elements derived in a previous run sequence. Run sequences are defined
    explicitly in the PDH source data.
    """
    pdh_dde_concepts = {}
    for i in range(pdh_num_run_sequences):
        current_run_sequence_str = str(i + 1)
        for pdh_row in pdh_raw:
            # Skip data elements directly from DATIM or in a different run sequence
            if (pdh_row[PDH_COLUMN_SOURCE_KEY] == PDH_COLUMN_SOURCE_KEY_DATIM or
                    pdh_row[PDH_COLUMN_RUN_SEQUENCE] != current_run_sequence_str):
                continue

            # Build the PDH derived data element (DDE) concept
            dde_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_UID])
            if dde_concept_url not in pdh_dde_concepts:
                dde_concept = build_concept_from_pdh_dde(
                    pdh_row, org_id, source_id, sorted_indicator_codes)
                pdh_dde_concepts[dde_concept_url] = dde_concept

            # Set the current source DE/COC to the DDE's custom attribute
            dde_concept['extras']['source_data_elements'].append({
                PDH_COLUMN_DERIVED_COC_UID: pdh_row[PDH_COLUMN_DERIVED_COC_UID],
                PDH_COLUMN_DERIVED_COC_NAME: pdh_row[PDH_COLUMN_DERIVED_COC_NAME],
                PDH_COLUMN_SOURCE_DATA_ELEMENT_UID: pdh_row[PDH_COLUMN_SOURCE_DATA_ELEMENT_UID],
                PDH_COLUMN_SOURCE_DATA_ELEMENT_NAME: pdh_row[PDH_COLUMN_SOURCE_DATA_ELEMENT_NAME],
                PDH_COLUMN_SOURCE_DISAGGREGATE: pdh_row[PDH_COLUMN_SOURCE_DISAGGREGATE],
                PDH_COLUMN_SOURCE_COC_UID: pdh_row[PDH_COLUMN_SOURCE_COC_UID],
                PDH_COLUMN_SOURCE_COC_NAME: pdh_row[PDH_COLUMN_SOURCE_COC_NAME],
                PDH_COLUMN_ADD_OR_SUBTRACT: pdh_row[PDH_COLUMN_ADD_OR_SUBTRACT],
            })

            # Store the COC mapping
            pdh_derived_coc_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                org_id, source_id, pdh_row[PDH_COLUMN_DERIVED_COC_UID])
            if pdh_derived_coc_url not in pdh_dde_concepts[dde_concept_url]['__cocs']:
                pdh_dde_concepts[dde_concept_url]['__cocs'].append(pdh_derived_coc_url)

    return pdh_dde_concepts


def build_concept_from_pdh_dde(pdh_row, org_id, source_id, sorted_indicator_codes):
    """ Return an OCL-formatted concept for the specified PDH derived data element """
    dde_concept = {
        'type': 'Concept',
        'id': pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_UID],
        'concept_class': 'Data Element',
        'datatype': 'Numeric',
        'owner': org_id,
        'owner_type': 'Organization',
        'source': source_id,
        'retired': False,
        'external_id': pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_UID],
        'descriptions': None,
        'extras': {
            'source': 'PDH',
            'resultTarget': pdh_row[PDH_COLUMN_RESULT_TARGET].lower().capitalize(),
            'pdh_indicator_code': pdh_row[PDH_COLUMN_INDICATOR],
            'disaggregate': pdh_row[PDH_COLUMN_DISAGGREGATE],
            'standardized_disaggregate': pdh_row[PDH_COLUMN_STANDARDIZED_DISAGGREGATE],
            'pdh_derivation_rule_id': pdh_row[PDH_COLUMN_RULE_ID],
            'pdh_rule_begin_period': pdh_row[PDH_COLUMN_RULE_BEGIN_PERIOD],
            'pdh_rule_end_period': pdh_row[PDH_COLUMN_RULE_END_PERIOD],
            ATTR_APPLICABLE_PERIODS: get_pdh_rule_applicable_periods(pdh_row),
            'source_data_elements': []
        },
        'names': [
            {
                'name': pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME],
                'name_type': 'Fully Specified',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            }
        ]
    }

    # Set DE custom attributes
    dde_version = get_pdh_dde_version(pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    dde_numerator_or_denominator = get_pdh_dde_numerator_or_denominator(
        pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    dde_support_type = get_pdh_dde_support_type(
        pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_NAME])
    dde_standard_indicator_code = lookup_indicator_code(
        pdh_row[PDH_COLUMN_INDICATOR], sorted_indicator_codes)
    if dde_version:
        dde_concept['extras']['data_element_version'] = dde_version
    if dde_numerator_or_denominator:
        dde_concept['extras']['numeratorDenominator'] = dde_numerator_or_denominator
    if dde_support_type:
        dde_concept['extras']['pepfarSupportType'] = dde_support_type
    if dde_standard_indicator_code:
        dde_concept['extras']['indicator'] = dde_standard_indicator_code

    # Set throwaway attributes
    dde_concept['__url'] = '/orgs/%s/sources/%s/concepts/%s/' % (
        org_id, source_id, pdh_row[PDH_COLUMN_DERIVED_DATA_ELEMENT_UID])
    dde_concept['__cocs'] = []

    return dde_concept


def build_pdh_dde_to_coc_maps(pdh_dde_concepts, coc_concepts=None):
    """
    Returns dictionary with DDE URL as key and list of COC URLs as value.
    If coc_concepts provided, validates that each derived COC is present
    in the coc_concepts list.
    """
    map_pdh_dde_to_coc = {}
    for pdh_dde_concept_url in pdh_dde_concepts:
        map_pdh_dde_to_coc[pdh_dde_concept_url] = []
        for coc_url in pdh_dde_concepts[pdh_dde_concept_url]['__cocs']:
            if coc_url not in coc_concepts:
                print 'Hello Houston, we have a problem! Missing DCOC:', coc_url
            elif coc_url not in map_pdh_dde_to_coc[pdh_dde_concept_url]:
                map_pdh_dde_to_coc[pdh_dde_concept_url].append(coc_url)
    return map_pdh_dde_to_coc


def get_datim_codelist_stats(codelist_datim):
    """ Returns counts of rows, data elements and category option combos in the DATIM codelist """
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
            # print '  from_concept:', de_concepts[0]['url']
            # print '  to_concept:', coc_concepts[0]['url']
            # print '  result:', de_to_coc_mappings
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
