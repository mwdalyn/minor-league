# Imports
import time
import pandas as pd
import datetime as dt
import numpy as np
import os, re
from geopy.geocoders import Nominatim
import sqlite3
from src.utils.html import find_latest_html, cook_html, set_user_agent
import json

# Constants
SLEEP_TIME = 1
USER_AGENT = set_user_agent(headers_file=os.path.abspath(os.path.join(".","user-agent.txt")))

# Constants
DB_PATH = os.path.abspath(os.path.join('.','database','milb.sqlite'))

REQUIRED_COLUMNS = ['city', 'country', 'state', 'metro', 'urban_area', 'csa', 'county', 'province', 
					'elevation', 'population_density', 'population_urbandensity', 'population_csa_density', 'fips_code', 
					'year_founded_max', 'year_founded_min', 'area_max', 'area_min', 'pop_max', 'pop_min', 
					'gdp_max', 'gdp_min', 'gnis_est', 'msa_est']

CREATE_TABLE_SQL = """
	CREATE TABLE IF NOT EXISTS cities (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	city TEXT NOT NULL,
	state TEXT NOT NULL,
	country TEXT,
	metro TEXT,
	urban_area TEXT,
	csa INTEGER,
	county TEXT,
	province TEXT,
	elevation TEXT,
	population_density TEXT,
	population_urbandensity TEXT, 
	population_csa_density TEXT, 
	fips_code TEXT, 
	year_founded_max INTEGER, 
	year_founded_min INTEGER, 
	area_max FLOAT, 
	area_min FLOAT, 
	pop_max FLOAT, 
	pop_min FLOAT, 
	gdp_max FLOAT, 
	gdp_min FLOAT, 
	gnis_est TEXT, 
	msa_est TEXT,
	created_on TEXT DEFAULT CURRENT_TIMESTAMP,
	updated_on TEXT DEFAULT CURRENT_TIMESTAMP,
	UNIQUE (city, state)
);
"""

CREATE_UPDATE_TRIGGER_SQL = """
	CREATE TRIGGER IF NOT EXISTS trg_cities_updated
	AFTER UPDATE ON cities
	FOR EACH ROW
	BEGIN
		UPDATE cities
		SET updated_on = CURRENT_TIMESTAMP
		WHERE id = OLD.id;
	END;
	"""

UPSERT_SQL = """
INSERT INTO cities (
	city, country, state, metro, urban_area, csa, county, province, elevation, 
	population_density, population_urbandensity, population_csa_density,
	fips_code, year_founded_max, year_founded_min, area_max, area_min,
	pop_max, pop_min, gdp_max, gdp_min, gnis_est, msa_est
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (city, state) DO UPDATE SET
	country=excluded.country,
	metro=excluded.metro,
	urban_area=excluded.urban_area,
	csa=excluded.csa,
	county=excluded.county,
	province=excluded.province,
	elevation=excluded.elevation,
	population_density=excluded.population_density,
	population_urbandensity=excluded.population_urbandensity,
	population_csa_density=excluded.population_csa_density,
	fips_code=excluded.fips_code,
	year_founded_max=excluded.year_founded_max,
	year_founded_min=excluded.year_founded_min,
	area_max=excluded.area_max,
	area_min=excluded.area_min,
	pop_max=excluded.pop_max,
	pop_min=excluded.pop_min,
	gdp_max=excluded.gdp_max,
	gdp_min=excluded.gdp_min,
	gnis_est=excluded.gnis_est,
	msa_est=excluded.msa_est,
	updated_on=CURRENT_TIMESTAMP;
"""


# Functions
def read_city_soup(soup, output_csv_path=None):
	# Check if soup looks like HTML
	if not hasattr(soup, "find"):
		return None

	# Grab the infobox table
	table = soup.find("table", class_=re.compile("infobox"))
	if table is None:  # No infobox found
		return None

	# Extract key-value pairs from the infobox
	infobox = {}
	prior_header = ""
	for row in table.find_all("tr"):
		header = row.find("th")
		value = row.find("td")
		row_class = row.get("class")
		
		if header and not value and row_class and "mergedtoprow" in row_class:
			prior_header = re.sub(
				r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]',
				'',
				header.get_text(" ", strip=True).replace('\xa0', '')
			).strip()
		
		if header and value:
			header_text = header.get_text(" ", strip=True).replace('\xa0', '').strip()
			if header_text and header_text[0] == "•":
				header_text = prior_header + " " + re.sub(
					r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]',
					'',
					header_text
				).strip()
			else:
				header_text = re.sub(
					r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]',
					'',
					header_text
				).strip()
			
			value_text = re.sub(
				r'[\(\[\{].*?[\)\]\}]',
				'',
				value.get_text(" ", strip=True).replace('\xa0', '')
			).strip().replace("(", "").replace(")", "")
			
			infobox[header_text] = value_text

	# Convert to DataFrame
	df = pd.DataFrame({k: [v] for k, v in infobox.items()})
	if output_csv_path:
		df.to_csv(output_csv_path, index=False)
	
	return df

def add_lat_lon(city, state, header=None):
	## Begin attempts to geolocate/grab data on cities
	if not header:
		# TODO: Throw exception?
		return None, None
	geolocator = Nominatim(user_agent=header) 
	# Lat Lon 
	if (city is None) | (state is None):
		lat, lon = 999, 999
	else:
		location = geolocator.geocode(city + ", " + state) # Address goes in arg
		time.sleep(SLEEP_TIME)
		try:
			lat = location.latitude
			lon = location.longitude
		except:
			lat, lon = 999, 999
	return lat, lon

def clean_wiki_infobox(x):
	'''Baked into the read_ function; Originally to be applied to infobox text content to handle special characters, footnotes, etc.'''
	if pd.isna(x):
		return x
	x = re.sub(r"\[.*?\]", "", x)
	x = re.sub(r"\(.*?\)", "", x)
	x = re.sub(r"\{}.*?\}", "", x)
	x = str(x).strip()
	x = x.replace(",", "")
	return x

def extract_year(x):
	'''For use in cleaning, parsing most recent year from a dated city DataFrame column.'''
	if pd.isna(x):
		return np.nan
	if isinstance(x, pd.Timestamp):
		return x.year
	if isinstance(x, (int, float)) and 1200 <= x <= 2100:
		return int(x)
	years = re.findall(r'\b(1[2-9]\d{2}|20\d{2})\b', str(x))
	return max(map(int, years)) if years else np.nan

def extract_area_sqmi(x):
	KM2_TO_MI2 = 0.386102
	if pd.isna(x): # Handle NaN
		return np.nan
	if isinstance(x, (int, float)): # Already a float
		return float(x)
	text = str(x).lower().replace(",", "") # Strip out ','
	m = re.search(
		r"([\d.]+)\s*(sq\s*mi|mi²|mi2|square\s*miles?)", text
	) # Strip out existing sq mi labeling (assume not in km)
	if m: 
		return float(m.group(1))
	m = re.search(
		r"([\d.]+)\s*(km²|km2|sq\s*km|square\s*kilometers?)",
		text
	) # Strip out existing km labeling (assume not in sq mi)
	if m:
		return float(m.group(1)) * KM2_TO_MI2
	# Else
	return np.nan

def extract_pop(x):
	'''For use in cleaning, parsing GDP estimates from a city DataFrame.'''
	if pd.isna(x):
		return np.nan
	# Already fully numeric input
	if isinstance(x, (int, float)):
		return x  # numeric value
	# Else convert to string
	s = str(x).lower().strip()
	# Check for "billion" signifier
	if re.search(r'\b(billion|bn|b)\b', s):
		match = re.search(r'[\d,.]+', s)
		if match:
			number = float(match.group(0).replace(',', ''))
			return number * 1000000000
	# Check for "million" signifier
	if re.search(r'\b(million|mil|m)\b', s):
		match = re.search(r'[\d,.]+', s)
		if match:
			number = float(match.group(0).replace(',', ''))
			return number * 1000000
	# Extract numbers only (strip commas)
	number_match = re.search(r'[\d,.]+', s)
	if number_match:
		number = float(number_match.group(0).replace(',', ''))
		# Check if this looks like a year
		if 1200 <= number <= 2100:
			return int(number)
		return number

def extract_gdp(x):
	'''For use in cleaning, parsing population estimates from a city DataFrame. Always returns in Millions.'''
	if pd.isna(x):
		return np.nan
	# Convert to string right away
	s = str(x).lower().strip()
	s = s.replace('$','')
	# Check for "billion" signifier
	if re.search(r'\b(billion|bn|b)\b', s):
		match = re.search(r'[\d,.]+', s)
		if match:
			number = float(match.group(0).replace(',', ''))
			return number * 1000000000 / 1000000
	# Check for "million" signifier
	if re.search(r'\b(million|mil|m)\b', s):
		match = re.search(r'[\d,.]+', s)
		if match:
			number = float(match.group(0).replace(',', ''))
			return number * 1000000 / 1000000
	# Extract numbers only (strip commas)
	number_match = re.search(r'[\d,.]+', s)
	if number_match:
		number = float(number_match.group(0).replace(',', ''))
		return number / 1000000

def extract_gnis(x):
	'''For use in cleaning, parsing GNIS IDs from a city DataFrame.
	FYI: 'https://www.usgs.gov/faqs/how-can-i-acquire-or-download-geographic-names-information-system-gnis-data'
	'''
	if pd.isna(x):
		return np.nan
	# Convert to string
	s = str(x).lower().strip()
	if s is None:
		return np.nan
	return s.split(",")[0].strip()

def choose_value(df):
	for val in df.iloc[0]:
		if pd.notna(val) and isinstance(val, (int, np.integer)):
			return int(val)
	return np.nan

def extract_msa(x):
	'''For use in selecting MSA from a city DataFrame.'''
	if pd.isna(x):
		return np.nan
	# Convert to int if string
	try:
		msa_int = int(x)
		return msa_int
	except:
		return np.nan

def clean_value(val):
		if pd.isna(val):
			return None
		if isinstance(val, (list, dict, set, np.generic)):
			return json.dumps(val)  # Convert to JSON string
		return val

def upsert_cities_more_robust(df, db_path=DB_PATH):
	"""
	Upsert city records into SQLite database safely.
	Cleans unsupported types and ensures proper records format.
	"""
	if not isinstance(df, pd.DataFrame):
		raise TypeError("Input must be a pandas DataFrame")

	# Ensure all required columns exist
	missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
	if missing_cols:
		raise ValueError(f"The following required columns are missing from the DataFrame: {missing_cols}")

	# Create list of tuples for executemany
	records = [
		tuple(clean_value(row[col]) for col in REQUIRED_COLUMNS)
		for row in df.to_dict("records")
	]

	if not records:
		return  # Nothing to insert
	
	with open(os.path.abspath(os.path.join(".","data","mid","cities_records.txt")), "w") as f:
		f.write(str(records))

	# Upsert into SQLite
	try:
		conn = sqlite3.connect(db_path)
		cursor = conn.cursor()
		cursor.execute(CREATE_TABLE_SQL)
		cursor.execute(CREATE_UPDATE_TRIGGER_SQL)
		cursor.executemany(UPSERT_SQL, records)
		conn.commit()
		conn.close()
	except Exception as e: 
		print(e)

def drop_cities_table(db_path=DB_PATH):
	# Connect to your database
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	# Drop the table if it exists
	cursor.execute("DROP TABLE IF EXISTS cities")
	# Commit changes and close connection
	conn.commit()
	conn.close()

# def clean_cities():
## Grab cities from database
conn = sqlite3.connect(DB_PATH)
query = "SELECT City, State FROM minor_league_teams;"
cities_list, cities_df, infobox_unique_cols, failed_cities = [], [], [], []
i = 0
for row in conn.execute(query):
	try:
		cities_list.append(row)
		city, state = row
		soup_html = cook_html(find_latest_html(os.path.abspath(os.path.join('.','data','raw','wikipedia','city')),
											internal_text = "{},_{}".format(city.replace(" ","_").lower(),state.replace(" ","_").lower())))
		table = read_city_soup(soup_html)
		[infobox_unique_cols.append(x) for x in table.columns.tolist() if x not in infobox_unique_cols]
		# Additional cleaning steps
		table.insert(0, "State Name", state)
		table.insert(0, "City Name", city)
		table['Latitude'], table['Longitude'] = add_lat_lon(city, state, header=USER_AGENT)
		first_values = {col: table[col].dropna().iloc[0] if not table[col].dropna().empty else None for col in table.columns}
		for k, v in first_values.items():
			if v is None:
				print(f"{k} has all None values")
			else:
				print(k, " : ", v)
		# Strip and consolidate information
		keep_cols = ["City Name", "State Name", "Country", "Metro", "MSA", "Metropolitan statistical area", "Urban Area", "CSA", "County", "Province", 
					"Area City", "Area Urban", "Area Metro", "Area CSA", "Area Censusdesignated place", "Area Federal capital city",
					"Area City and provincial capital",
					"Elevation", 
					"Population City", "Population Density", "Population Urban", "Population Federal capital city",
					"Population Urbandensity", "Population CSA density", "Population Metro", "Population CSA", "Population Region", "Population TriCities", 
					"Population Censusdesignated place", "Population City and provincial capital",
					"GDP Metro", "GDP", "GDP MSA", "GDP Total", "GDP Greensboro",
					"FIPS code", "GNIS ID", "GNIS IDs"] 
		group_agg = {"Founded": ["First settled","Founded","Named","Incorporated","Established", "First settlement", "Charter", "Chartered", "Adopted", 
									"Foundation", "Founding", "City Charter", "Laid out", "Laid Out", "Incorporated as a town", "Incorporated as a city", "Incorporated as a village", "Incorporation",
									"Constituted", "Municipal corporation"],
					"Area_Geo":["Area City", "Area Urban", "Area Metro", "Area CSA", "Area Censusdesignated place", "Area Federal capital city",
								"Area City and provincial capital"],
					"Pop_Est":["Population City", "Population Urban", "Population Federal capital city", "Population Metro", 
								"Population CSA", "Population Region", "Population TriCities", "Population Censusdesignated place", "Population City and provincial capital"],
					"GDP":["GDP Metro", "GDP", "GDP MSA", "GDP Total", "GDP Greensboro"],
					"GNIS":["GNIS ID", "GNIS IDs", "GNIS feature ID"],
					"MSA":["MSA", "Metropolitan statistical area"]}
		# Build out desired columns
		table = table.reindex(columns=keep_cols + [item for lst in group_agg.values() for item in lst if item not in keep_cols])
		# Aggregate founding
		# founded_col = table.columns.intersection(group_agg["Founded"]) # Use these columns to filter; became unnecessary after reindex force including all listed above
		founded_table = table[group_agg["Founded"]]
		years_founded = founded_table.apply(lambda c: c.map(extract_year))
		table["year_founded_max"] = years_founded.max(axis=1).astype("Int64")
		table["year_founded_min"] = years_founded.min(axis=1).astype("Int64")	
		table = table.drop(columns = group_agg["Founded"])
		# Aggregate geo area
		area_table = table[group_agg["Area_Geo"]]
		area_sizes = area_table.apply(lambda c: c.map(extract_area_sqmi))
		table["area_max"] = area_sizes.max(axis=1).astype("float")
		table["area_min"] = area_sizes.min(axis=1).astype("float")
		# Aggregate population
		pop_table = table[group_agg["Pop_Est"]]
		pop_ests = pop_table.apply(lambda c: c.map(extract_pop))
		table["pop_max"] = pop_ests.max(axis=1).astype("float")
		table["pop_min"] = pop_ests.min(axis=1).astype("float")
		# Aggregate GDP 
		gdp_table = table[group_agg["GDP"]]
		gdp_ests = gdp_table.apply(lambda c: c.map(extract_gdp))
		table["gdp_max"] = gdp_ests.max(axis=1).astype("float")
		table["gdp_min"] = gdp_ests.min(axis=1).astype("float")
		# Select GNIS
		gnis = table[group_agg["GNIS"]]
		table["gnis_est"] = choose_value(gnis.apply(lambda c: c.map(extract_gnis)))
		# Select MSA
		msas = table[group_agg["MSA"]]
		table["msa_est"] = choose_value(msas.apply(lambda c: c.map(extract_msa)))
		# Reindex
		table = table.reindex(columns=[col for col in table.columns.tolist() if col not in [item for lst in group_agg.values() for item in lst]])
		# Update column names to match SQL convention
		table.columns = [col.lower().replace(" ","_") for col in table.columns.tolist()]
		table = table.rename(columns={"city_name":"city"})
		table = table.rename(columns={"state_name":"state"})
		# Add to tables container
		cities_df.append(table)
		i += 1
		if i >= 300:
			break
	except Exception as e:
		print(f"Failed for {city}, {state}: {e}")
		failed_cities.append((city, state))
		continue

# Close connection
conn.close()
# Columns to consider for future development 
with open(os.path.abspath(os.path.join(".","data","mid","cities_html_all_infobox_data.txt")), "w") as f:
	for item in infobox_unique_cols:
		f.write(f"{item}\n")
# # Print failed cities (temporary)
# print(failed_cities)

# Create df
cities_df = pd.concat(cities_df, axis=0)
cities_df.to_csv(os.path.abspath(os.path.join(".","data","fin","cities_df.csv")))

## Inject cities_df into DB table
# upsert_cities_robust(cities_df, db_path=DB_PATH) # NOTE: Doesn't work, param 13 error nonstandard
# Atttempt to use "more_robust"
# upsert_cities_more_robust(cities_df, db_path=DB_PATH) # NOTE: Doesn't work, param 13 error nonstandard

# New test
df = cities_df.copy()
if not isinstance(df, pd.DataFrame):
	raise TypeError("Input must be a pandas DataFrame")

# Ensure all required columns exist
missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
if missing_cols:
	raise ValueError(f"The following required columns are missing from the DataFrame: {missing_cols}")

# Create list of tuples for executemany
records = [
	tuple(clean_value(row[col]) for col in REQUIRED_COLUMNS)
	for row in df.to_dict("records")
]

# if not records:
	# return  # Nothing to insert

with open(os.path.abspath(os.path.join(".","data","mid","cities_records.txt")), "w") as f:
	f.write(str(records))

# Upsert into SQLite
try:
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()
	cursor.execute(CREATE_TABLE_SQL)
	cursor.execute(CREATE_UPDATE_TRIGGER_SQL)
	cursor.executemany(UPSERT_SQL, records)
	conn.commit()
	conn.close()
except Exception as e: 
	print(e)
# clean_cities()
