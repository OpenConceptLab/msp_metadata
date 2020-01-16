"""
Script to request data element export from DHIS2
"""
import requests
import json
import csv
import settings
import msp


# Source files
output_filename = 'datim_dataElements_20200107.json'

# Custom request for all data elements in DATIM (no filter)
url_all_custom = 'https://dev-de.datim.org/api/dataElements.json?fields=id,code,name,shortName,aggregationType,domainType,description,valueType,categoryCombo[id,code,name,categoryOptionCombos[id,code,name]],dataElementGroups[id,name],attributeValues,dataSetElements[dataSet[id,name,shortName]]'

# Request used in datimsync.py without dataset IDs
url_all_sync = '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,categoryCombo[id,code,name,lastUpdated,created,categoryOptionCombos[id,code,name,lastUpdated,created]],dataSetElements[*,dataSet[id,name,shortName]]'

# Full request used in datimsync.py filtered by FY18 and FY19 dataset IDs
url_fy18_19_datasets = '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,categoryCombo[id,code,name,lastUpdated,created,categoryOptionCombos[id,code,name,lastUpdated,created]],dataSetElements[*,dataSet[id,name,shortName]]&filter=dataSetElements.dataSet.id:in:[zUoy5hk8r0q,PyD4x9oFwxJ,KWRj80vEfHU,fi9yMqWLWVy,IZ71Y2mEBJF,ndp6aR3e1X3,pnlFw2gDGHD,gc4KOv8kGlI,FsYxodZiXyH,iJ4d5HdGiqG,GiqB9vjbdwb,EbZrNIkuPtc,nIHNMxuPUOR,C2G7IyPPrvD,sBv1dj90IX6,HiJieecLXxN,dNGGlQyiq9b,tTK9BhvS5t3,PH3bllbLw8W,N4X89PgW01w,WbszaIdCi92,uN01TT331OP,tz1bQ3ZwUKJ,BxIx51zpAjh,IZ71Y2mEBJF,mByGopCrDvL,XZfmcxHV4ie,jcS5GPoHDE0,USbxEt75liS,a4FRJ2P4cLf,l796jk9SW7q,BWBS39fydnX,eyI0UOWJnDk,X8sn5HE5inC,TdLjizPNezI,I8v9shsCZDS,lXQGzSqmreb,Ncq22MRC6gd]'

# Options
option_paging = '&paging=false'
option_filter = '&filter=name:like:TX_CURR'

# Set the request URL
url = url_all_custom + option_paging

# Fetch the export from DATIM
r = requests.get(url, auth=(settings.datim_username, settings.datim_password))
r.raise_for_status()
results = r.json()
print 'Data Elements retrieved from DATIM:', len(results['dataElements'])

# Write to file
with open(output_filename, mode='wb') as localfile:
    localfile.write(r.content)
