"""
Common functionality used in the MSP ETL scripts used to prepare and import metadata from
MER guidance, DATIM, PDH, and related systems.
"""
import json
import csv
import pprint
import re
import requests


# Constants for OCL mappings
MSP_MAP_TYPE_INDICATOR_TO_DE = 'Has Data Element'
MSP_MAP_TYPE_DE_TO_COC = 'Has Option'
MSP_MAP_TYPES = [
    MSP_MAP_TYPE_INDICATOR_TO_DE,
    MSP_MAP_TYPE_DE_TO_COC,
]

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


def display_input_metadata_summary(verbosity=1, indicators=None, datim_de=None,
                                   codelists=None, sorted_indicators=None, pdh_raw=None):
    """ Displays summary of the loaded metadata """
    print 'LOAD METADATA SOURCES:'
    if indicators:
        print '  Reference Indicators (FY18-20):', len(indicators)
        if verbosity >= 5:
            for indicator in indicators:
                print '     ', indicator['id'], indicator['attr:Period'], indicator
            print ''
    if datim_de:
        print '  DATIM Data Elements (All):', len(datim_de['dataElements'])
    if codelists:
        print '  Code Lists (FY16-20):', len(codelists)
        if verbosity >= 3:
            for codelist in codelists:
                print '    [ %s ]  %s  --  %s' % (
                    codelist['external_id'], codelist['id'],
                    codelist['attr:Applicable Periods'])
            print ''
    if sorted_indicators:
        print '  Unique Indicator Codes:', len(sorted_indicators)
        if verbosity >= 3:
            for indicator_code in sorted_indicators:
                print '    ', indicator_code
    if pdh_raw:
        print '  PDH Rows:', len(pdh_raw)


def display_processing_summary(verbosity=1, de_concepts=None, de_skipped_no_code=None,
                               de_skipped_no_indicator=None, dirty_data_element_ids=None,
                               codelists=None, matched_periods=None,
                               matched_codelists=None, map_indicator_to_de=None,
                               sorted_indicators=None):
    """ Display a summary of the result of processing DATIM metadata """
    print '\nSUMMARY OF RESULTS FROM PROCESSING METADATA:'
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

    # Summary for indicator-->data element maps
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


def get_new_source_json(org_id='', source_id=''):
    """ Returns OCL-formatted JSON for the PEPFAR MER source """
    return {
        "name": "MER Indicators",
        "default_locale": "en",
        "short_code": source_id,
        "source_type": "Dictionary",
        "full_name": "DATIM Monitoring, Evaluation & Results Indicators",
        "owner": org_id,
        "public_access": "View",
        "owner_type": "Organization",
        "type": "Source",
        "id": source_id,
        "supported_locales": "en"
    }


def load_datim_data_elements(filename=''):
    """
    Loads DATIM data elements from JSON file in the raw JSON format retrieved directly from DHIS2.
    Disags and datasets are included as attributes of each data element.
    """
    raw_datim_de = None
    with open(filename, 'rb') as ifile:
        raw_datim_de = json.load(ifile)
    return raw_datim_de


def fetch_datim_codelist(url_datim):
    """ Fetches a public codelist from DATIM (not from OCL) """
    r = requests.get(url_datim)
    r.raise_for_status()
    return r.json()


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
            row['__dataElements'] = []
            row['owner_id'] = org_id
            codelists.append(row)
    return codelists


def load_indicators(org_id='', source_id='', filenames=None):
    """ Loads reference indicators from MER guidance as OCL-formatted JSON """
    if not filenames:
        return []
    indicators = []
    for filename in filenames:
        with open(filename) as ifile:
            reader = csv.DictReader(ifile)
            for row in reader:
                row['owner_id'] = org_id
                row['source'] = source_id
                indicators.append(row)
    return indicators


def load_pdh(filename=''):
    """ Loads PDH extract and returns in raw format """
    pdh = []
    with open(filename) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            pdh.append(row)
    return pdh


def get_pdh_de_numerator_or_denominator(de_name):
    """
    Returns 'Numerator' or 'Denominator', respectively, if 'N' or 'D' is
    present in the data element modifiers (ie, in between parentheses).
    """
    if '(N,' in de_name or '(N)' in de_name:
        return 'Numerator'
    elif '(D,' in de_name or '(D)' in de_name:
        return 'Denominator'
    return ''


def get_pdh_de_support_type(de_name):
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


def get_pdh_de_version(de_name):
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
    return ''


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


def lookup_indicator_code(de_code, sorted_indicators):
    """
    Returns an indicator code that matches the prefix of the data element code
    eg. TX_CURR would be returned for TX_CURR_N_DSD_Age_Sex
    """
    for indicator_code in sorted_indicators:
        if de_code[:len(indicator_code)] == indicator_code:
            return indicator_code
    return ''


def get_sorted_unique_indicator_codes(indicators=None):
    """ Returns a list of unique sorted indicator codes given a list of OCL-formatted indicators """
    output = []
    for indicator in indicators:
        if indicator['id'] not in output:
            output.append(indicator['id'])
    output.sort(key=len, reverse=True)
    return output


def get_data_element_version(de_code=''):
    """ Returns a data element version string (eg 'v2' or 'v5') if present """
    result = re.search('_([vV][0-9])$', de_code)
    if result:
        return result.group(1)
    return None


def build_codelists_dict(codelists=None):
    """
    Build dictionary of codelists where external_id is the key. Error if duplicate external_id.
    """
    codelists_dict = {}
    for codelist in codelists:
        if codelist['external_id'] not in codelists_dict:
            codelists_dict[codelist['external_id']] = codelist
        else:
            print ('ERROR: Duplicate code lists IDs should already be removed at this point. '
                   'Here are the 2 conflicting code lists:')
            pprint.pprint(codelists_dict[codelist['external_id']])
            pprint.pprint(codelist)
            exit(1)
    return codelists_dict


def filter_de_code_lists(de, codelists_dict, display_debug_info=False):
    """ Not currently used """
    de_codelists = []
    for dataset in de['dataSetElements']:
        if dataset['dataSet']['id'] in codelists_dict.keys():
            if display_debug_info:
                print '\t', dataset['dataSet']
                print '\t', codelists_dict[dataset['dataSet']['id']]
            de_codelists.append(dataset)
    return de_codelists


def get_de_periods_from_codelists(de_codelists, codelists_dict):
    """ Get a list of the periods present in the codelists """
    periods = {}
    for codelist in de_codelists:
        for period in codelists_dict[codelist['id']]['attr:Applicable Periods'].split(', '):
            periods[period] = True
    return periods.keys()


def get_filtered_indicators(indicators=None, period=None):
    """ Returns a list of indicators filtered by either a single period or a list of periods """
    if isinstance(period, basestring):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, basestring) for item in period):
        pass
    else:
        return []
    filtered_indicators = []
    for indicator in indicators:
        if not period or (
                period and 'attr:Period' in indicator and indicator['attr:Period'] in period):
            filtered_indicators.append(indicator)
    return filtered_indicators


def get_filtered_data_elements(data_elements=None, period=None):
    """ Returns a list of data elements filtered by either a single period or a list of periods """
    if isinstance(period, basestring):
        period = [period]
    elif isinstance(period, list) and all(isinstance(item, basestring) for item in period):
        pass
    else:
        return []
    filtered_de = []
    for de_key in data_elements.keys():
        de = data_elements[de_key]
        if (period and 'Applicable Periods' in de['extras'] and any(de_period in period for de_period in de['extras']['Applicable Periods'])) or not period:
            filtered_de.append(de)
    return filtered_de


def get_filtered_disags(data_elements=None, map_de_to_coc=None, coc_concepts=None):
    """ Returns list of disags relevant to the specified list of data elements """
    disags = {}
    for de in data_elements:
        de_concept_key = '/orgs/%s/sources/%s/concepts/%s/' % (de['owner'], de['source'], de['id'])
        # print '[%s]: ' % de_concept_key, de
        if de_concept_key in map_de_to_coc:
            # print '    ', map_de_to_coc[de_concept_key]
            for coc_concept_key in map_de_to_coc[de_concept_key]:
                disags[coc_concept_key] = coc_concepts[coc_concept_key]
    return disags


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
        # print '  ', codelist['attr:Applicable Periods'], codelist
        if (period and 'attr:Applicable Periods' in codelist and any(codelist_period.strip() in period for codelist_period in codelist['attr:Applicable Periods'].split(','))) or not period:
            filtered_codelists.append(codelist)
    return filtered_codelists


def get_indicator_to_de_maps(filtered_de=None, org_id='', source_id='',
                             map_type_indicator_to_de=''):
    """ Get a list mappings for indicator to data element relationships as OCL-formatted JSON """
    filtered_maps = []
    for de in filtered_de:
        indicator_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
            org_id, source_id, de['extras']['indicator'])
        de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de['id'])
        indicator_to_de_map = {
            "type": "Mapping",
            'owner': org_id,
            'owner_type': 'Organization',
            'source': source_id,
            'from_concept_url': indicator_concept_url,
            'to_concept_url': de_concept_url,
            'map_type': map_type_indicator_to_de,
        }
        filtered_maps.append(indicator_to_de_map)
    return filtered_maps


def get_de_to_disag_maps(filtered_de=None, map_de_to_coc=None,
                         org_id='', source_id='', map_type_de_to_coc=''):
    """ Get a list mappings for data element to disag relationships as OCL-formatted JSON """
    filtered_maps = []
    for de in filtered_de:
        de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de['id'])
        for coc_concept_url in map_de_to_coc[de_concept_url]:
            de_to_coc_map = {
                "type": "Mapping",
                'owner': org_id,
                'owner_type': 'Organization',
                'source': source_id,
                'from_concept_url': de_concept_url,
                'to_concept_url': coc_concept_url,
                'map_type': map_type_de_to_coc,
            }
            filtered_maps.append(de_to_coc_map)
    return filtered_maps


def get_codelist_references(filtered_csv_codelists=None, map_de_to_coc=None,
                            org_id='', source_id=''):
    """
    Get a dictionary where keys are codelist IDs and values are a list of references
    for data element and disag concepts to be included in a codelist.
    """
    codelist_references = {}
    for codelist in filtered_csv_codelists:
        ref_expressions = []
        for de_code in codelist['__dataElements']:
            de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (org_id, source_id, de_code)
            ref_expressions.append(de_concept_url)
            if de_concept_url not in map_de_to_coc:
                print 'ERROR: de_concept_url not in map_de_to_coc'
                exit(1)
            for coc_concept_url in map_de_to_coc[de_concept_url]:
                ref_expressions.append(coc_concept_url)
        if ref_expressions:
            codelist_references[codelist['id']] = {
                "type": "Reference",
                "owner": org_id,
                'owner_type': 'Organization',
                'collection': codelist['id'],
                "data": {"expressions": ref_expressions},
                "__cascade": "sourcemappings",
            }
        # pprint.pprint(codelist)
        # pprint.pprint(codelist_references)
    return codelist_references


def get_repo_version_json(owner_type='Organization', owner_id='', repo_type='Source', repo_id='',
                          version_id='', description='', released=True):
    """ Returns OCL-formatted JSON for a repository version """
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
    """ Output a python dictionary of codelist definitions formatted for display in MSP """
    output_codelists = []
    for codelist in codelists:
        output_codelist = {
            'id': codelist['id'],
            'name': codelist['name'],
            'full_name': codelist['full_name'],
            'periods': codelist['attr:Applicable Periods'].split(', '),
            'codelist_type': codelist['attr:Code List Type'],
            'description': codelist['description'],
            'dataset_id': codelist['ZenDesk: DATIM DataSet ID']
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
        ]
    }
    return coc_concept


def build_concept_from_datim_de(de_raw, org_id, source_id, sorted_indicators, codelists_dict):
    """ Return an OCL-formatted concept for the specified DATIM data element """

    # Set the concept ID and skip if data element 'code' not defined
    if 'code' not in de_raw:
        return -1

    # Clean the data element code
    de_concept_id_dirty = de_raw['code']
    de_concept_id = format_concept_id(unformatted_id=de_raw['code'])

    # Determine the indicator
    de_indicator_code = lookup_indicator_code(
        de_code=de_concept_id, sorted_indicators=sorted_indicators)
    if not de_indicator_code:
        return -2

    # Determine data element attributes
    de_result_or_target = get_data_element_result_or_target(de_code=de_concept_id)
    de_numerator_or_denominator = get_data_element_numerator_or_denominator(de_code=de_concept_id)
    de_version = get_data_element_version(de_code=de_concept_id)
    de_support_type = get_data_element_support_type(de_code=de_concept_id)

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
            'resultTarget': de_result_or_target,
            'indicator': de_indicator_code,
        },
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

    # Process DE's dataSets and Code Lists
    de_codelists = []
    if 'dataSetElements' in de_raw and de_raw['dataSetElements']:
        de_concept['extras']['dataSets'] = []
        for dataset in de_raw['dataSetElements']:
            de_concept['extras']['dataSets'].append(dataset['dataSet'])
            if dataset['dataSet']['id'] in codelists_dict.keys():
                de_codelists.append(dataset['dataSet'])
                codelists_dict[dataset['dataSet']['id']]['__dataElements'].append(de_concept_id)
        de_concept['extras']['codelists'] = de_codelists
        de_concept['extras']['Applicable Periods'] = get_de_periods_from_codelists(
            de_codelists=de_codelists, codelists_dict=codelists_dict)

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
    if de_concept_id != de_concept_id_dirty:
        de_concept['extras']['unformatted_id'] = de_concept_id_dirty

    return de_concept


def get_pdh_rule_begin_end_string(pdh_row):
    """
    Return a string representing the period of validity for a PDH derived data element
    """
    return '%s - %s' % (pdh_row['rule_begin_period'], pdh_row['rule_end_period'])


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


def build_concept_from_pdh_de(pdh_row, org_id, source_id):
    """ Return an OCL-formatted concept for the specified PDH derived data element """
    de_concept = {
        'type': 'Concept',
        'id': pdh_row['derived_data_element_uid'],
        'concept_class': 'Derived Data Element',
        'datatype': 'Numeric',
        'owner': org_id,
        'owner_type': 'Organization',
        'source': source_id,
        'retired': False,
        'external_id': pdh_row['derived_data_element_uid'],
        'descriptions': None,
        'extras': {
            'source': 'PDH',
            'resultTarget': pdh_row['result_target'].lower().capitalize(),
            'indicator': pdh_row['indicator'],
            'disaggregate': pdh_row['disaggregate'],
            'standardized_disaggregate': pdh_row['standardized_disaggregate'],
            'Applicable Periods': get_pdh_rule_begin_end_string(pdh_row),
            'pdh_derivation_rule_id': pdh_row['rule_id'],
        },
        'names': [
            {
                'name': pdh_row['derived_data_element_name'],
                'name_type': 'Fully Specified',
                'locale': 'en',
                'locale_preferred': True,
                'external_id': None,
            }
        ]
    }

    # Set DE custom attributes
    de_version = get_pdh_de_version(pdh_row['derived_data_element_name'])
    de_numerator_or_denominator = get_pdh_de_numerator_or_denominator(pdh_row['derived_data_element_name'])
    de_support_type = get_pdh_de_support_type(pdh_row['derived_data_element_name'])
    if de_version:
        de_concept['extras']['data_element_version'] = de_version
    if de_numerator_or_denominator:
        de_concept['extras']['numeratorDenominator'] = de_numerator_or_denominator
    if de_support_type:
        de_concept['extras']['pepfarSupportType'] = de_support_type

    return de_concept


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
        for mapping in codelist_ocl.get_mappings(from_concept_uri=de_concept['url'], map_type=MSP_MAP_TYPE_DE_TO_COC):
            coc_concept = codelist_ocl.get_concept_by_uri(mapping['to_concept_url'])
            found_matching_row = False
            if coc_concept:
                for row in codelist_datim['listGrid']['rows']:
                    if (row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_CODE] == de_concept['id'] and
                            row[DATIM_CODELIST_COLUMN_DATA_ELEMENT_UID] == de_concept['external_id'] and
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
