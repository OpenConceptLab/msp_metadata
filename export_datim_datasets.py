"""
Fetch CSV of datasets from DHIS2
"""
import requests
import settings


query = 'https://dev-de.datim.org/api/dataSets.csv?fields=id,href,shortName,name,code,description,periodType&paging=false'
filename = 'datasets_20200107.csv'

# Fetch the export from DATIM and save to file
r = requests.get(query, auth=(settings.datim_username, settings.datim_password))
r.raise_for_status()

# Write to file
with open(filename, mode='wb') as localfile:
    localfile.write(r.content)
