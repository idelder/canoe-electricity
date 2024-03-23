This script aggregates the modular electricity sector model for CANOE. It pulls data primarily from CODERS but also from the NREL ATB (optionally) and some other sources like the IESO public database.

Aggregation can be configured through files in input_files/ (see Configuration) to include/exclude technologies, provinces, types of data/model structures.

It will download a large number of files on the first run (as of now, 141 MB) but will cache these files locally and use the local cache in subsequent runs. Parameters can be set to force downloading to get latest data (see Configuration).



=======
 Usage
=======

1. Create the conda environment.
	a. Install miniconda
	b. Open a miniconda prompt
	c. Install the conda environment
		i. Set current directory to electricity_sector
			> cd C:/.../electricity_sector/
		ii. Create the environment
			> conda env create environment.yml
2. Add your CODERS API key
	a. Acquire a CODERS API key at https://cme-emh.ca/en/coders/
	b. Create a new text file in input_files/ called coders_api_key.txt and save your API key there.
3. Run the aggregation
	a. Set current directory to parent of /electricity_sector
		> cd C:/.../foo/  (where /electricity_sector/ is contained)
	b. Activate the environment
		> conda activate canoe-backend
	c. Run the scripts
		> python electricity_sector/



============
 Components
============


 Directories
=============
- data_cache/
	Where downloaded data is locally cached and pulled from on subsequent runs, unless force_download param is set to True
- documentation/
	TODO: eventual directory of documents describing the operation of this sector backend.
- input_files/
	- Configuration files
	- CANOE database schema
	- CANOE excel spreadsheet template
	- params.yml (see Configuration)
	- coders_api_key.txt (NOTE: this file is ignored by git and must be created manually)


 Scripts
=========
- __main__.py
	For running from command line like >python electricity_sector/
- electricity_sector.py
	Handles execution order of other scripts.
- currency_conversion.py
	Converts data currencies to a unified final currency.
- coders_api.py
	Handles fetching of data from CODERS and local caching.
- setup.py
	Builds the config object which contains aggregation configuration and some common data.
- utils.py
	Repository of frequently used utility scripts. Includes get_data() method which robustly fetches and locally caches data from online sources.
- testing_dummy.py
	A blank script for prototyping code snippets.
- others...
	Aggregation scripts for particular data types.



===============
 Configuration TODO keep filling out
===============

Configuration is performed by editing the following files in electricity_sector/input_files

- atb_master_tables.csv
	This simply references where to find tables (so far only technology-specific variables) in the calculation workbook of the NREL ATB. It may need to be updated with new versions of the ATB. Columns:
		- atb_master_sheet
			The name of the excel worksheet to be extracted from (referenced by [storage/generator]_technologies.csv)
		- table
			Label of which table is being specific. So far only tsv for technology-specific variables.
		- first_row
			Excel-indexed row number of the first table row (first of multiple header rows for tsv).
		- last_row
			Excel-indexed row number of the bottom row of the table.
		- columns
			Excel-indexed column letters spanning the table.
- 