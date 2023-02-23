"""
Script to convert all codelists saved to an export file (using save_codelists_to_file.py)
to a single CSV designed to be more human-readable. The CSV file contains these columns:
dataset, dataelement, shortname, code, dataelementuid, dataelementdesc, categoryoptioncombo,
categoryoptioncombocode, categoryoptioncombouid
"""
import json
import unicodecsv as csv
import settings
import msp


OUTPUT_FILENAME = 'all_datim_codelists_20220413.csv'


# Load codelists
codelists = msp.load_codelist_collections_with_exports_from_file(
    filename=settings.FILENAME_DATIM_CODELISTS_WITH_EXPORT,
    org_id=settings.MSP_ORG_ID)

# Build the headers array
csv_headers = []
for header in codelists[0]['extras']['dhis2_codelist']['listGrid']['headers']:
    csv_headers.append(header['column'])

# print json.dumps(csv_headers, indent=4)

with open(OUTPUT_FILENAME, 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')
    writer.writerow(csv_headers)
    for codelist in codelists:
        for codelist_row in codelist['extras']['dhis2_codelist']['listGrid']['rows']:
            # print json.dumps(codelist_row)
            writer.writerow(codelist_row)

print "Codelists saved to: %s" % OUTPUT_FILENAME
