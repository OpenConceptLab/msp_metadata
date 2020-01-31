Output files:

* Codelists - JSON file of all results and target codelists (no mechanisms or tiered site support):
	* fiscal-year-start-stop - eg `FY17_20`
```
codelists_RT_[fiscal-year-start-stop].json
```
* MSP full metadata import - Includes MER guidance, Codelists, and DATIM, plus the OCL org, sources, and collections
	* fiscal-year-start-stop - eg `FY17_20`
	* yymmdd = date bulk imported started, eg `20200124`
```
msp_full_[fiscal-year-start-stop]_[yymmdd].json
```

Running list of corrections made to metadata:
* Fixed unsupported data element ID (included a plus?)
* Harmonized naming of "EMR-SITE" indicator

Running list of code fixes:
* 1/27/2020 - Removed duplicates, reducing import file size by 40%
* 1/30/2020 - Added collection versions created at the end of each period
