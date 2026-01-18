# Imports
import time
import pandas as pd
import datetime as dt
import numpy as np
import os, re
from io import StringIO
from geopy.geocoders import Nominatim
import sqlite3
from src.utils.html import find_latest_html, cook_html, set_ua_headers

# Constants
SLEEP_TIME = 10
USER_AGENT = set_ua_headers(ua_file_path=os.path.abspath(os.path.join(".","user-agent.txt")))

# Constants
DB_PATH = os.path.abspath(os.path.join('.','database','milb.sqlite'))

CREATE_TABLE_SQL = """
	CREATE TABLE IF NOT EXISTS cities (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	City TEXT NOT NULL,
	State TEXT NOT NULL,
	
	created_on TEXT DEFAULT CURRENT_TIMESTAMP,
	updated_on TEXT DEFAULT CURRENT_TIMESTAMP,
	UNIQUE (City, State)
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
		City, State
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (City, State) DO UPDATE SET
		Division=excluded.Division,
		State=excluded.State,
		Stadium=excluded.Stadium,
		Capacity=excluded.Capacity,
		Affiliate=excluded.Affiliate,
		TableIndex=excluded.TableIndex,
		Affiliates=excluded.Affiliates,
		Mascot=excluded.Mascot;
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

def add_lat_lon(city, state, user_agent=None):
	## Begin attempts to geolocate/grab data on cities
	if not user_agent:
		# TODO: Throw exception?
		return None, None
	geolocator = Nominatim(user_agent) 
	# Lat Lon 
	if (city is None) | (state is None):
		lat, lon = 999, 999
	else:
		location = geolocator.geocode(city + ", " + state) # Address goes in arg
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

def upsert_cities(df, db_path=DB_PATH):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()

	cursor.execute(CREATE_TABLE_SQL)
	cursor.execute(CREATE_UPDATE_TRIGGER_SQL)
	records = [
	(
		row.City,
		row.State,
		# TODO: Add
	)
	for row in df.itertuples(index=False)
	]
	cursor.executemany(UPSERT_SQL, records)
	conn.commit()
	conn.close()
 
# def clean_city():
## Grab cities from database


conn = sqlite3.connect(DB_PATH)
query = "SELECT City, State FROM minor_league_teams;"
cities_list = []
for row in conn.execute(query):
	cities_list.append(row)
	city, state = row
	soup_html = cook_html(find_latest_html(os.path.abspath(os.path.join('.','data','raw','wikipedia','city')),
										internal_text = "{},_{}".format(city.replace(" ","_").lower(),state.replace(" ","_").lower())))
	table = read_city_soup(soup_html)
	# TODO: Add additional cleaning steps; see below
	table['Latitude'], table['Longitude'] = add_lat_lon(city, state, user_agent=USER_AGENT)
	break

# upsert_cities(table, db_path=DB_PATH)


# # Review
# first_values = {col: cities_df[col].dropna().iloc[0] if not cities_df[col].dropna().empty else None for col in cities_df.columns}
# for k, v in first_values.items():
# 	print(k, " : ", v)
# # Strip and consolidate information
# keep_cols = ["Country", "State", "city_name", "Metro", "MSA", "Metropolitan statistical area", "Urban Area", "CSA", "County", "Province", 
# 			  "Area City", "Area Urban", "Area Metro", "Area CSA", "Area Censusdesignated place", "Area Federal capital city",
# 			  "Area City and provincial capital",
# 			  "Elevation", 
# 			  "Population City", "Population Density", "Population Urban", "Population Federal capital city",
# 			  "Population Urbandensity", "Population CSA density", "Population Metro", "Population CSA", "Population Region", "Population TriCities", 
# 			  "Population Censusdesignated place", "Population City and provincial capital",
# 			  "GDP Metro", "GDP", "GDP MSA", "GDP Total", "GDP Greensboro",
# 			  "FIPS code", "GNIS ID", "GNIS IDs"] 
# group_agg = {"Founded": ["First settled","Founded","Named","Incorporated","Established", "First settlement", "Charter", "Chartered", "Adopted", 
# 							 "Foundation", "Founding", "City Charter", "Laid out", "Laid Out", "Incorporated as a town", "Incorporated as a city", "Incorporated as a village", "Incorporation",
# 							 "Constituted", "Municipal corporation"],
# 			 "Area_Geo":[],
# 			 "GNIS":["GNIS ID", "GNIS IDs"],
# 			 "Population_Est":["Population City", "Population Urban", "Population Federal capital city", "Population Metro", 
# 						   "Population CSA", "Population Region", "Population TriCities", "Population Censusdesignated place", "Population City and provincial capital"]}
# # Begin aggregation
# cities_df_clean = cities_df[keep_cols + group_agg["Founded"]] # TODO: Update, depending on what groupings you want to perform
# # Aggregate founding
# years_df = cities_df_clean[group_agg["Founded"]].apply(lambda c: c.map(extract_year))
# cities_df_clean["year_founded"] = years_df.max(axis=1).astype("Int64")
# cities_df_clean = cities_df_clean.drop(columns = group_agg["Founded"])
# # Aggregate geo area
# # TODO: Modify the above. Are "Areas" similar enough that we can give them the "Founded" treatment? Func is above regardless
# # area_df = main_df_clean[group_agg["Area_Geo"]].apply(lambda c: c.map(extract_area_sqmi))
# # main_df_clean["area_sq_mi"] = area_df.max(axis=1)
# # Aggregate population estimate, etc.

# # More cleaning steps

# # Export
# cities_df_clean.to_csv("main_city_infobox_df_clean.csv")
# # Replace working main_cities_df