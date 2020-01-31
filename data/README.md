Input files for MSP metadata:

* **MER Indicators** - one source CSV created per year thru manual data entry:
    * fiscal-year = eg "FY18"
    * yyyymmdd = date bulk imported started, eg `20200124`
```
mer_indicators_[fiscal_year]_[yyyymmdd].csv
```
* **Codelists** - source CSV created thru manual data entry:
    * fiscal-year-start-stop - eg `FY17_20`
    * yyyymmdd = date bulk imported started, eg `20200124`
```
codelists_RT_[fiscal-year-start-stop]_[yymmdd].json
```
* **DATIM Data Elements and Disags** - raw JSON retrieved from DATIM DHIS2 dev-de API
    * yyyymmdd = date bulk imported started, eg `20200124`
```
datim_dataElements_[yymmdd].json
```
* **DATIM Data Sets** - raw JSON retreived from DATIM DHIS2 dev-de API
    * yyyymmdd = date bulk imported started, eg `20200124`
```
datim_datasets_[yymmdd].json
```
* **PDH** - raw PDH extract
    * yyyymmdd = date bulk imported started, eg `20200124`
```
pdh_raw_[yyyymmdd].xlsx
```
