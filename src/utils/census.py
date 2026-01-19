# Imports
import os, requests, time, re
import sqlite3
from dotenv import load_dotenv
import pandas as pd

ACS_TO_CBSA_VINTAGE = [
	{"acs_start": 2020, "acs_end": 2024, "cbsa_vintage": "2020"},
	{"acs_start": 2015, "acs_end": 2019, "cbsa_vintage": "2010"},
	{"acs_start": 2010, "acs_end": 2014, "cbsa_vintage": "2010"},
	{"acs_start": 2005, "acs_end": 2009, "cbsa_vintage": "2000"},
]

def acs_vintage_benchmark_dict():
	# Get benchmarks
	benchmarks_url = "https://geocoding.geo.census.gov/geocoder/benchmarks?format=json"
	benchmarks = requests.get(benchmarks_url).json()["benchmarks"]
	# Join vintages available per benchmark
	acs_vintage_benchmark = []
	pattern_desc = r"(\w+) Vintage - (\w+) Benchmark" # pattern for vintageDescription in vintage values
	pattern_year = r"ACS(\w+)"
	for benchmark in benchmarks:
		benchmark_id, benchmark_name = benchmark["id"], benchmark["benchmarkName"]
		vintages_url = f"https://geocoding.geo.census.gov/geocoder/vintages?benchmark={benchmark_id}&format=json"
		vintages = requests.get(vintages_url).json()["vintages"]    
		for vintage in vintages:
			vintage_id, vintage_name, vintage_desc = vintage['id'], vintage['vintageName'], vintage['vintageDescription']
			if re.search(r"ACS", vintage_name) and not re.search(r"Census", vintage_name):
				match = re.search(pattern_desc, vintage_desc)
				vintage, benchmark = match.group(1), match.group(2)
				# Convert to int OR if Current, convert to current year; match to get the orig values
				vintage_year, benchmark_year = (m := re.search(pattern_year, vintage)) and m.group(1) or "9999", (m := re.search(pattern_year, benchmark)) and m.group(1) or "9999"
				acs_vintage_benchmark.append({"vintage":vintage, "benchmark":benchmark, "vintage_id":vintage_id, "vintage_year":vintage_year, "vintage_name":vintage_name, "vintage_desc":vintage_desc,
									"benchmark_id":benchmark_id, "benchmark_year":benchmark_year, "benchmark_name":benchmark_name})
			else:
				continue
	return pd.DataFrame(acs_vintage_benchmark)


# Previous work: used CBSA as a starting point/bridge
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
