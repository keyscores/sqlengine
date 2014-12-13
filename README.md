[![Circle CI](https://circleci.com/gh/keyscores/sqlengine.svg?style=svg&circle-token=c2162c43e6e515771ece132b08806d37aff998ba)](https://circleci.com/gh/keyscores/sqlengine)

#SQLEngine for Keyscores

## Requirements

GAE Dev Server
>dev_appserver.py sqlengine

https://cloud.google.com/appengine/docs/python/tools/devserver

## features
- merge n tables
- auto detect col types (date, dim or fact)
- auto import facts from tables
-  import csv files from local files, URLs, blobs or CsvPy
- API to register tables(csv files) and formulas
- API to precompute results for a company
- API to compute measures

##API
- **register_rawfiles:** Adds tables for a given company.
- **load_precomputenormalized:** Adds a merged version of all tables for a given company.
- **user_analytics:** Computes measures for a given company.

## modules
**filehandler:** Stores company and location (local file, URL, ..) for each registered table. If a registered table has new facts (facts that have not been seen so far) they are stored using the filehandler.

**ks_graphDB:** Finds relations between n tables.

**merge:** Imports tables for a given company using the filehandler. Merges n tables.

**precompute:** Normalizes the merged table for further use if cross company reports or switching between different versions of the merged table for a given company is needed.

**analytics** Is used to compute measures on a merged table.

**ks_dbsettings:** Eases use of different database servers (local-dev, circle-local or circle-cloud)

**csvPy:** Stores csv files for static testing.

## testing
- python gaetestrunner.py ~/google_appengine/ dev-local [test on local server]    
- python gaetestrunner.py ~/google_appengine/ circle-cloud [test on google sql]     
- python gaetestrunner.py -h [get help]    
- python testBinaryOpsAPI.py [copy file ks_dbname.txt with dev-local into test folder; fast pure python testing]  
