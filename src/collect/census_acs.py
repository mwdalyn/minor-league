'''
Docstring for collect.census_api
ac5 geographic options: https://api.census.gov/data/2023/acs/acs5/geography.html
NOTE: Census Geocoder API available for intaking LL instead of City, State. FIPS seems more compatible 
'''
# Imports
import os, requests, time, re
import sqlite3
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import warnings

# Retrieve API key
# dotenv_path = os.path.join(os.path.dirname(__file__), ".", ".env") 
dotenv_path = os.path.join(os.getcwd(), ".env") 
load_dotenv(dotenv_path=dotenv_path)
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Constants
SLEEP_TIME = 1
DB_PATH = os.path.abspath(os.path.join('.','database','milb.sqlite'))
BENCHMARK_YEAR = 9999 # 9999 = Current benchmark
ACS5_YEAR = 2023 # Can be for 5Y ACS or 1Y ACS, need to determine which
CBP_YEAR = 2023
ACS1_YEAR_DICT = {
	2022: [2022],
	2021: [2021],
	2020: [2020],
	2019: [2019],
	2018: [2018],
	2017: [2017],
	2016: [2016],
	2015: [2015],
	2014: [2014],
	2013: [2013],
	2012: [2012],
	2011: [2011],
	2010: [2010]
}
ACS5_YEAR_DICT = {
	2022: [2018, 2019, 2020, 2021, 2022],
	2021: [2017, 2018, 2019, 2020, 2021],
	2020: [2016, 2017, 2018, 2019, 2020],
	2019: [2015, 2016, 2017, 2018, 2019],
	2018: [2014, 2015, 2016, 2017, 2018],
	2017: [2013, 2014, 2015, 2016, 2017],
	2016: [2012, 2013, 2014, 2015, 2016],
	2015: [2011, 2012, 2013, 2014, 2015],
	2014: [2010, 2011, 2012, 2013, 2014],
	2013: [2009, 2010, 2011, 2012, 2013],
	2012: [2008, 2009, 2010, 2011, 2012],
	2011: [2007, 2008, 2009, 2010, 2011],
	2010: [2006, 2007, 2008, 2009, 2010]
}
ACS5_VARIABLES = {
	# Population & Demographics
	"B01003_001E": "Total population",
	"B01002_001E": "Median age of the total population (no average age equivalent exists in ACS)",
	"B01001_002E": "Total male population",
	"B01001_026E": "Total female population",
	# Working-age population (used to derive working-age totals)
	"B01001_007E": "Male population ages 16–17 (used to derive working-age population)",
	"B01001_008E": "Male population ages 18–24 (used to derive working-age population)",
	"B01001_009E": "Male population ages 25–34 (used to derive working-age population)",
	"B01001_010E": "Male population ages 35–44 (used to derive working-age population)",
	"B01001_011E": "Male population ages 45–54 (used to derive working-age population)",
	"B01001_012E": "Male population ages 55–64 (used to derive working-age population)",
	"B01001_013E": "Male population ages 65+ (excluded if limiting to working-age population)",
	"B01001_031E": "Female population ages 16–17 (used to derive working-age population)",
	"B01001_032E": "Female population ages 18–24 (used to derive working-age population)",
	"B01001_033E": "Female population ages 25–34 (used to derive working-age population)",
	"B01001_034E": "Female population ages 35–44 (used to derive working-age population)",
	"B01001_035E": "Female population ages 45–54 (used to derive working-age population)",
	"B01001_036E": "Female population ages 55–64 (used to derive working-age population)",
	"B01001_037E": "Female population ages 65+ (excluded if limiting to working-age population)",
	# Household size
	"B11016_001E": "Median household size",
	# "DP02_0010E": "Average household size (mean household size)", # This is a profile datapoint; separate
	# Housing
	# "DP04_0003E": "Housing unit vacancy rate (percentage of vacant units)", # This is a profile datapoint; separate
	# Income
	"B19013_001E": "Median household income (available at city, county, state, etc.)",
	"B19025_001E": "Aggregate household income (used to derive average household income)",
	"B11001_001E": "Total number of households (used to derive average household income)",
	# Employment / Labor force
	"B23025_002E": "Civilian population in the labor force (used to derive employment rate)",
	"B23025_004E": "Civilian employed population (used to derive employment rate)",
	# Industry / Employment by sector
	"C24050_002E": "Employment in agriculture, forestry, fishing, and hunting",
	"C24050_003E": "Employment in mining, quarrying, and oil and gas extraction",
	"C24050_004E": "Employment in construction",
	"C24050_005E": "Employment in manufacturing",
	"C24050_006E": "Employment in wholesale trade",
	"C24050_007E": "Employment in retail trade",
	"C24050_008E": "Employment in transportation and warehousing, and utilities",
	"C24050_009E": "Employment in educational services (education sector employment)",
	"C24050_010E": "Employment in health care and social assistance",
	"C24050_011E": "Employment in arts, entertainment, recreation, accommodation, and food services",
	"C24050_012E": "Employment in public administration",
	"C24050_013E": "Employment in other services (except public administration)"
}
STATE_ABBR = {
	"Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
	"California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
	"Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
	"Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
	"Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
	"Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
	"Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
	"New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
	"North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
	"Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
	"South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
	"Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
	"Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC"
}

# Methods
def abbreviate_state(state):
	state = state.strip().title()
	if state in STATE_ABBR:
		return STATE_ABBR[state]
	warnings.warn(f"Unknown state: {state}", RuntimeWarning)
	return None

def get_state_fips(state, acs5_year=2023):
	'''Provide either state name title or abbrev, get FIPS code for state. For use in ACS/Census querying.'''
	if state is None:
		warnings.warn("State input is None; cannot resolve state FIPS.", RuntimeWarning)
		return None
	r = requests.get("https://api.census.gov/data/{}/acs/acs5?get=NAME&for=state:*".format(str(acs5_year)))
	r.raise_for_status()
	state_fips = {name: fips for name, fips in r.json()[1:]}  # {state_name: FIPS}
	if len(state)==2:
		state_name = {abbr: name for name, abbr in STATE_ABBR.items()}[state]
	else:
		state_name = state
	return state_fips[state_name]

def get_city_fips(city, state, acs5_year=2023):
	state_fips = get_state_fips(state)  # example: California FIPS
	if state is None or state_fips is None:
		warnings.warn("State input is None; cannot resolve state FIPS.", RuntimeWarning)
		return None
	url = "https://api.census.gov/data/{}/acs/acs5".format(str(acs5_year))
	params = {
		"get": "NAME",  
		"for": "place:*",
		"in": f"state:{state_fips}",
		"key": CENSUS_API_KEY
	}
	r2 = requests.get(url, params=params)
	r2.raise_for_status()
	data = r2.json()
	# Get all sub-lists where the first item starts with search_str
	matches = [item for item in data if item[0].startswith(city)]
	# If one item returned, assume match; if multiple, clarify " city" suffix
	if len(matches)==1:
		city_fips = matches[0][2] # Return ACS place sublist then select city_fips element [2]
		# TODO: For ('Jupiter','Florida') raises IndexError: list index out of range
	elif len(matches)>1:
		city_fips = [item for item in data if item[0].startswith(city+" city")][0][2] # Return ACS place sublist then select city_fips element [2]
	else:
		city_fips = None # TODO: Don't like "None" in this instance, is there a better option for fail state?
	return city_fips

def check_fips(state_fips, place_fips, api_key, acs5_year=2023):
	url = f"https://api.census.gov/data/{str(acs5_year)}/acs/acs5?get=NAME&for=place:{place_fips}&in=state:{state_fips}&key={api_key}"
	response = requests.get(url)
	if response.status_code != 200:
		return f"Error: {response.status_code} - {response.text}"
	data = response.json()
	if len(data) < 2:
		return "FIPS code not found"
	return None  # Returns None if check succeeds

def check_vars_batch(url, variables, city_fips, state_fips, api_key, batch_size=10):
	"""
	Recursively check which variables exist for a city/state using batching.
	Returns a list of available variables.
	"""
	available = []
	# Base case: only one variable
	if len(variables) == 1:
		var = variables[0]
		params = {"get": f"NAME,{var}", "for": f"place:{city_fips}", "in": f"state:{state_fips}", "key": api_key}
		try:
			r = requests.get(url, params=params)
			r.raise_for_status()
			return [var]  # exists
		except requests.exceptions.HTTPError:
			return []  # missing
	else:
		# Split variables into batches
		for i in range(0, len(variables), batch_size):
			batch = variables[i:i+batch_size]
			params = {"get": "NAME," + ",".join(batch), "for": f"place:{city_fips}", "in": f"state:{state_fips}", "key": api_key}
			try:
				r = requests.get(url, params=params)
				r.raise_for_status()
				available.extend(batch)  # all returned → available
			except requests.exceptions.HTTPError:
				if len(batch) == 1:
					continue  # single var failed → not available
				else:
					# Split batch recursively
					available.extend(check_vars_batch(url, batch, city_fips, state_fips, api_key, batch_size=1))
	
	return available

def check_available_vars_per_city_batched(year, city_state_list, variables, key):
	"""
	Returns a dict mapping (city_fips, state_fips) -> list of available ACS variables.
	Uses batched recursive checks for efficiency and robustness.
	"""
	url = f"https://api.census.gov/data/{year}/acs/acs5"
	results = {}
	for city_fips, state_fips in city_state_list:
		available = check_vars_batch(url, variables, city_fips, state_fips, key, batch_size=10)
		results[(city_fips, state_fips)] = available
	
	return results


##% Query and set up ACS table
## Grab cities from database
conn = sqlite3.connect(DB_PATH)
query = "SELECT City, State FROM minor_league_teams;"
cities_list, cities_df_list, failed_cities = [], [], [] # TODO: Create outlet for failed_cities with try/except
for row in conn.execute(query):
	print(row)
	cities_list.append(row)
	city, state = row
	state_fips = get_state_fips(abbreviate_state(state))
	city_fips = get_city_fips(city, abbreviate_state(state))
	if not city or not state or not state_fips or not city_fips or check_fips(state_fips, city_fips, CENSUS_API_KEY):
		print("Something wrong with FIPS results for {}, {}: {},{}".format(city, state, city_fips, state_fips))
		continue # if not, all OK
	# Check variables that are available
	vars = list(ACS5_VARIABLES.keys())
	url = "https://api.census.gov/data/{}/acs/acs5".format(str(ACS5_YEAR))
	available_vars = check_vars_batch(url, vars, city_fips, state_fips, CENSUS_API_KEY, batch_size=10)
	time.sleep(SLEEP_TIME)
	# Query Census ACS data (simple, for now) # TODO: Build in more selection for Benchmark geo and Vintage of ACS to get a longitudinal view
	url = "https://api.census.gov/data/{}/acs/acs5".format(str(ACS5_YEAR))
	params = {
		"get": "NAME,{}".format(",".join(available_vars)),  # median household income
		"for": f"place:{city_fips}",
		"in": f"state:{state_fips}",
		"key": CENSUS_API_KEY,
	}
	r2 = requests.get(url, params=params)
	r2.raise_for_status()
	data = r2.json()
	# Set up city DataFrame
	city_df = pd.DataFrame([data[1] + [city, state]], columns=[data[0] + ["city_name","state_name"]])
	# Add missing columns with NaN
	for col in ["NAME"] + vars + ["city_name", "state_name"]:
		if col not in city_df.columns:
			city_df[col] = np.nan # NOTE: Be aware, is np.nan the best choice?
	city_df = city_df[["NAME"] + vars + ["city_name", "state_name"]]
	cities_df_list.append(city_df)

# User data headers from json to set column names, create df from collected responses
# cities_df = pd.DataFrame(cities_df, columns=[data[0]+["city_name","state_name"]])
cities_df = pd.concat(cities_df_list,axis=0)
# Reformat columns (non-Census cols)
cities_df.columns = [
    col.lower().replace(" ", "_")
    if isinstance(col, str) and not re.search(r"\d", col)
    else col
    for col in cities_df.columns
]
# Use ACS variables dict to rename columns into logical form
# TODO: Instead create a key table or similar, so we can log descriptions
# Upsert into database
required_columns = cities_df.columns.tolist()
# TODO: Develop upsert


##% Query and set up CBP (econ) table
## Grab cities from database
acs5_variables = list(ACS5_VARIABLES.keys())
conn = sqlite3.connect(DB_PATH)
query = "SELECT City, State FROM minor_league_teams;"
cities_list, cities_df, failed_cities = [], [], [] # TODO: Create outlet for failed_cities with try/except
for row in conn.execute(query):
	print(row)
	cities_list.append(row)
	city, state = row
	state_fips = get_state_fips(abbreviate_state(state))
	city_fips = get_city_fips(city, abbreviate_state(state))
	print("Sleep")
	time.sleep(SLEEP_TIME)
	print("Wake")
	# Query data
	url = "https://api.census.gov/data/{}/cbp".format(str(CBP_YEAR))
	# TODO
