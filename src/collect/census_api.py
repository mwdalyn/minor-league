'''
Docstring for collect.census_api
ac5 geographic options: https://api.census.gov/data/2023/acs/acs5/geography.html

'''
import os, requests, time
from dotenv import load_dotenv
import pandas as pd
from us import states

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env") # Two levels up
load_dotenv(dotenv_path=dotenv_path)
census_api_key = os.getenv("CENSUS_API_KEY")

SLEEP_TIME = 0.5
ACS_YEAR = 2023
ACS_VARIABLES = [
	"B01003_001E",  # Total population
	"B19013_001E",  # Median household income
	"B25001_001E",  # Housing units
	"B23025_002E",  # Labor force
	"B15003_022E"   # Bachelor's degree
]
CITY_STATE_LIST = [
	("Akron", "OH"),
	("Cleveland", "OH"),
	("Columbus", "OH"),
	("Dayton", "OH"),
	("Toledo", "OH")
] # TODO: Query the database 
ACS_TO_CBSA_VINTAGE = [
	{"acs_start": 2020, "acs_end": 2024, "cbsa_vintage": "2020"},
	{"acs_start": 2015, "acs_end": 2019, "cbsa_vintage": "2010"},
	{"acs_start": 2010, "acs_end": 2014, "cbsa_vintage": "2010"},
	{"acs_start": 2005, "acs_end": 2009, "cbsa_vintage": "2000"},
]
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


def normalize_state(state_input):
	'''
	If state input is a full name, normalize to abbrev.
	'''
	state_input = state_input.strip().title()  # "ohio" -> "Ohio"
	if state_input in STATE_ABBR:
		return STATE_ABBR[state_input]
	elif state_input.upper() in STATE_ABBR.values():
		return state_input.upper()  # Already abbreviation
	else:
		raise ValueError(f"Unknown state: {state_input}")

def cbsa_vintage_for_acs_year(acs_year):
	'''
	Map CBSA vintage and ACS vintage.
	'''
	for row in ACS_TO_CBSA_VINTAGE:
		if row["acs_start"] <= acs_year <= row["acs_end"]:
			return row["cbsa_vintage"]
	raise ValueError(f"No CBSA vintage mapping for ACS year {acs_year}")

def city_state_to_cbsa_with_micro(city, state, acs_year):
	'''
	Lookup CBSA for city, state pair and given ACS Vintage. Includes fallback if muMSA (<50k).
	
	:param city: Description
	:param state: Description
	:param acs_year: Description
	'''
	cbsa_vintage = cbsa_vintage_for_acs_year(acs_year)

	for layer, type_label in [("CBSA", "metro"), ("MICRO", "micro")]:
		params = {
			"address": f"{city}, {state}",
			"benchmark": "Public_AR_Current",
			"vintage": f"Current_{cbsa_vintage}",
			"layers": layer,
			"format": "json"
		}

		try:
			r = requests.get(
				"https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress",
				params=params,
				timeout=10
			)
			r.raise_for_status()
			geos = r.json()["result"]["geographies"].get(layer, [])
		except requests.exceptions.RequestException:
			geos = []

		# Return first match if available
		if geos:
			cbsa = geos[0]
			time.sleep(SLEEP_TIME) 
			return {
				"cbsa_code": cbsa["GEOID"],
				"cbsa_name": cbsa["NAME"],
				"cbsa_vintage": cbsa_vintage,
				"type": type_label
			}

	# No CBSA or muSA found
	return None

def resolve_cbsas(city_state_list, acs_year):
	'''
	Get CBSA for city, state in list of city, states
	
	:param city_state_list: Description
	:param acs_year: Description
	'''
	cbsa_map = {}

	for city, state in city_state_list:
		result = city_state_to_cbsa_with_micro(city, state, acs_year)
		if result:
			cbsa_code = result["cbsa_code"]
			cbsa_name = result["cbsa_name"]
			if cbsa_code not in cbsa_map:
				cbsa_map[cbsa_code] = {
					"cbsa_name": cbsa_name,
					"cities": [],
					"type": result["type"],
					"cbsa_vintage": result["cbsa_vintage"]
				}
			cbsa_map[cbsa_code]["cities"].append(f"{city}, {state}")

	return cbsa_map


def query_acs5_cbsa(cbsa_code, variables, year):
	'''
	Query ACS5 for a single CBSA
	
	:param cbsa_code: Description
	:param variables: Description
	:param year: Description
	'''
	params = {
		"get": ",".join(["NAME"] + variables),
		"for": f"cbsa:{cbsa_code}"
	}

	try:
		r = requests.get(
			f"https://api.census.gov/data/{year}/acs/acs5",
			params=params,
			timeout=10
		)
		r.raise_for_status()
		data = r.json()
	except requests.exceptions.RequestException:
		return None

	header, values = data[:2]
	time.sleep(SLEEP_TIME) 
	return dict(zip(header, values))

def run_pipeline(city_state_list, variables, acs_year):
	'''
	Full pipeline with MSA and muMSA handling. Intake cities output ACS results.
	
	:param city_state_list: Description
	:param variables: Description
	:param acs_year: Description
	'''
	results = []
	cbsa_map = resolve_cbsas(city_state_list, acs_year)

	for cbsa_code, info in cbsa_map.items():
		record = query_acs5_cbsa(cbsa_code, variables, acs_year)
		if record:  # skip failed ACS queries
			record.update({
				"cbsa_code": cbsa_code,
				"cbsa_name": info["cbsa_name"],
				"type": info["type"],              # metro or micro
				"cbsa_vintage": info["cbsa_vintage"],
				"cities": info["cities"]           # provenance
			})
			results.append(record)

	return results

acs_results = run_pipeline(
	city_state_list=CITY_STATE_LIST,
	variables=ACS_VARIABLES,
	year=ACS_YEAR
)

for r in acs_results:
	print(r)
