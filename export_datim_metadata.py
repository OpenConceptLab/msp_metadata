"""
Script to request data element export from DHIS2
"""
import datetime
import requests
import settings


# DATIM DHIS2 export queries
# NOTE: comment out to omit; key must match the standard resource name from DHIS2
DATIM_EXPORTS = {
    'indicators': {
        'fileType': 'json',
        'url': ('https://dev-de.datim.org/api/indicators.json?fields=*,dataSets[id,name],'
                'indicatorType[id,name],indicatorGroups[id,name]&paging=false')
    },
    'dataElements': {
        'fileType': 'json',
        'url': ('https://dev-de.datim.org/api/dataElements.json?fields=id,code,name,shortName,'
                'aggregationType,domainType,description,valueType,categoryCombo[id,code,name,'
                'categoryOptionCombos[id,code,name]],dataElementGroups[id,name],attributeValues,'
                'dataSetElements[dataSet[id,name,shortName,code]]&paging=false'),
    },
    'categoryOptionCombos': {
        'fileType': 'json',
        'url': ('https://dev-de.datim.org/api/categoryOptionCombos.json?fields=id,code,name,'
                'shortName,categoryCombo[id,name,dataDimensionType],categoryOptions[id,code,name]'
                '&paging=false'),
    },
    'dataSets': {
        'fileType': 'csv',
        'url': ('https://dev-de.datim.org/api/dataSets.csv?fields=id,href,shortName,name,code,'
                'description,periodType&paging=false'),
    }
}

# Fetch the export from DATIM
for export_key in DATIM_EXPORTS:
    export_filename = 'data/datim_%s_%s.%s' % (
        export_key, datetime.datetime.today().strftime('%Y%m%d'),
        DATIM_EXPORTS[export_key]['fileType'])
    print('\n****', export_key, '\n', DATIM_EXPORTS[export_key]['url'])
    r = requests.get(
        DATIM_EXPORTS[export_key]['url'],
        auth=(settings.DATIM_USERNAME, settings.DATIM_PASSWORD))
    r.raise_for_status()
    if '.json?' in DATIM_EXPORTS[export_key]['url']:
        results = r.json()
        print('%s resources successfully retrieved from DATIM:' % str(len(results[export_key])))
    with open(export_filename, mode='wb') as localfile:
        localfile.write(r.content)
    print('Content saved to %s' % export_filename)
