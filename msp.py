"""
Common functionality used in the MSP ETL scripts used to prepare and import metadata from
MER guidance, DATIM, PDH, and related systems.
"""
import json
import csv
import pprint
import re


def display_metadata_summary(verbosity=0, indicators=None, datim_de=None,
                             codelists=None, sorted_indicators=None, pdh=None):
    """ Displays summary of the loaded metadata """
    print 'METADATA SOURCES:'
    print '  Reference Indicators (FY18-20):', len(indicators)
    if verbosity >= 5:
        for indicator in indicators:
            print '     ', indicator['id'], indicator['attr:Period'], indicator
        print ''
    print '  DATIM Data Elements (All):', len(datim_de['dataElements'])
    print '  Code Lists (FY16-20):', len(codelists)
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


def get_de_periods_from_codelists(de_codelist_ids, codelists_dict):
    """ Get a list of the periods present in the codelists """
    periods = {}
    for codelist_id in de_codelist_ids:
        for period in codelists_dict[codelist_id]['attr:Applicable Periods'].split(', '):
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
                "data": {"expressions": ref_expressions}
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
    NOTE: More elegant solutions to de-duping all resulted in keeping only the last occurence of a resource,
    where it is required that we keep only the first occurence of a resource, hence the custom solution.
    APPROACH #1: This approach successfully de-duped, but took a very long time and kept only the last occurence
    import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
    APPROACH #2: This approach successfully de-duped and ran quickly, but still kept only the last occurence
    import_list_jsons = {json.dumps(resource, sort_keys=True) for resource in import_list}
    import_list_dedup = [json.loads(resource) for resource in import_list_jsons]
    APPROACH #3 used here:
    Custom approach successfully de-duped, ran slightly slower than approach #2, & kept 1st occurence!
    """
    dedup_list = []
    dedup_list_jsons = []
    old_list_jsons = [json.dumps(resource, sort_keys=True) for resource in dup_dict]
    for str_resource in old_list_jsons:
        if str_resource not in dedup_list_jsons:
            dedup_list_jsons.append(str_resource)
            dedup_list.append(json.loads(str_resource))
    return dedup_list
