
# Imports
import requests 
import time
import pandas as pd
import datetime as dt
import numpy as np
import os, re
from io import StringIO
from geopy.geocoders import Nominatim

# Constants
SLEEP_TIME = 12

# Functions
def read_city_soup(soup):
	# Check if soup looks like HTML
	if not hasattr(soup, "find"):
		return None
	# Grab the infobox table
	table = soup.find("table", class_=re.compile("infobox"))
	if table is None: # No infobox found
		return None
	# Extract key-value pairs from the infobox
	infobox = {}
	for row in table.find_all("tr"):
		header = row.find("th")
		value = row.find("td")
		row_class = row.get("class")
		if header and not value and row_class and "mergedtoprow" in row_class:
			prior_header = re.sub(r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]', '', header.get_text(" ",strip=True).replace('\xa0', '')).strip()
		if header and value:
			if header.get_text(" ", strip=True) and header.get_text(" ", strip=True)[0] == "•":
				# print(prior_header + " : " + header.get_text(" ", strip=True))
				header = prior_header + " " + re.sub(r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]', '', header.get_text(" ",strip=True).replace('\xa0', '')).strip()
			else:
				header = re.sub(r'[\(\[\{].*?[\)\]\}]|[^a-zA-Z\s]', '', header.get_text(" ",strip=True).replace('\xa0', '')).strip() # header.get_text(" ", strip=True)
			value = re.sub(r'[\(\[\{].*?[\)\]\}]', '', value.get_text(" ", strip=True).replace('\xa0', '')).strip().replace("(","").replace(")","")
			infobox[header] = value

	if not infobox:
		# Infobox table exists but no usable key-values
		return None

    # Convert to DataFrame
	df = pd.DataFrame({k: [v] for k, v in infobox.items()})
	return df

def add_lat_lon(cities, states, useragent):
	## Begin attempts to geolocate/grab data on cities
	geolocator = Nominatim(user_agent=useragent) 
	# Lat Lon for Map
	lat_list, lon_list = [], []
	for c, s in zip(cities, states):
		if (c is None) | (s is None):
			lat_list.append(-999), lon_list.append(-999)
			continue
		else:
			print(c, s)
			location = geolocator.geocode(c + ", " + s) # Address goes in arg
			try:
				lat_list.append(location.latitude), lon_list.append(location.longitude)
			except:
				lat_list.append(-999), lon_list.append(-999)
			time.sleep(SLEEP_TIME)
	# TODO: Add check func that ensures lat lon lists are equal length to input
	return lat_list, lon_list

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


## Grab cities_df
cities_df = ''

## Get coordinates for towns
# lats, lons = add_lat_lon(team_df["City"], team_df["State"], wiki_user_headers["User-Agent"])
# cities_df["Lat"], cities_df["Lon"] = lats, lons

# Review
first_values = {col: cities_df[col].dropna().iloc[0] if not cities_df[col].dropna().empty else None for col in cities_df.columns}
for k, v in first_values.items():
    print(k, " : ", v)
# Strip and consolidate information
keep_cols = ["Country", "State", "city_name", "Metro", "MSA", "Metropolitan statistical area", "Urban Area", "CSA", "County", "Province", 
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
             "Area_Geo":[],
             "GNIS":["GNIS ID", "GNIS IDs"],
             "Population_Est":["Population City", "Population Urban", "Population Federal capital city", "Population Metro", 
                           "Population CSA", "Population Region", "Population TriCities", "Population Censusdesignated place", "Population City and provincial capital"]}
# Begin aggregation
cities_df_clean = cities_df[keep_cols + group_agg["Founded"]] # TODO: Update, depending on what groupings you want to perform
# Aggregate founding
years_df = cities_df_clean[group_agg["Founded"]].apply(lambda c: c.map(extract_year))
cities_df_clean["year_founded"] = years_df.max(axis=1).astype("Int64")
cities_df_clean = cities_df_clean.drop(columns = group_agg["Founded"])
# Aggregate geo area
# TODO: Modify the above. Are "Areas" similar enough that we can give them the "Founded" treatment? Func is above regardless
# area_df = main_df_clean[group_agg["Area_Geo"]].apply(lambda c: c.map(extract_area_sqmi))
# main_df_clean["area_sq_mi"] = area_df.max(axis=1)
# Aggregate population estimate, etc.

# More cleaning steps

# Export
cities_df_clean.to_csv("main_city_infobox_df_clean.csv")
# Replace working main_cities_df