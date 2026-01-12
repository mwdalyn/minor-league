'''
Created on: July 16, 2023 
Updated: July 22, 2025 (oops)
Goal: 
Collect data and logos for all minor league baseball teams documented at the following link
https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams
Store the data in a convenient way and retrieve. 
'''

# Imports
import requests 
import time
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import numpy as np
import os, re
from io import StringIO

# Constants
SLEEP_TIME = 10 # seconds for sleep
TEAMS_LINK = "https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams"

# Functions
def set_ua_headers(ua_file_path='user-agent.txt'):
	wiki_user_headers = {}
	with open(ua_file_path, 'r') as f:
		for line in f:
			try:
				# Parsing the txt
				key, value = line.strip().split(':', 1)
				key, value = key.replace("'","").strip(), value.replace("'","").strip()
				wiki_user_headers[key] = value
			except ValueError:
				print(f"Skip: {line.strip()}")

def cook_soup(url, headers, output_html_path = None, parser = 'html.parser'):
	## Cook Soup
	try:
		response = requests.get(
		url = url,
		headers = headers)
		timestamp = dt.datetime.now().strftime(format="%Y%m%d_%H%M%S")
		response.raise_for_status()
		# Parse soup
		soup = BeautifulSoup(response.content, parser)
		# Archive as an html file with date name
		if output_html_path:
			url_page = "_".join(url.split("/")[-1].split("_")[0:4]).lower() # Grab first four phrases in the final segment of wiki url as unique ID
			with open(os.path.join(output_html_path, f"wiki_{url_page}_{str(timestamp)}.html"), "w", encoding='utf-8-sig') as file: 
				file.write(str(soup))
	except Exception as e:
		print(f'Error: {e}')
		soup = f'Error: {e}'
	return soup

def read_milb_soup(soup, output_csv_path="read_city_soup_test.csv"):
	if str(soup)[:15] != '<!DOCTYPE html>':
		print("Error: Soup string failed, not a proper HTML return.")
		return None
	# Parse sequentially: want headers as column value joined w their associated tables
	ti = 1
	df_list = []
	for tag in soup.find_all(): # Sequential 
		if tag.name in ['h1','h2','h3','h4']: # Section headers 
			h_txt = tag.text.replace("\n","").replace("\t","") # Not 'Contents'; Set this so that the following table can grab
		elif tag.name in ['table']:
			if h_txt=="Dominican Summer League": # Exclude DSL for now
				continue
			# Setting up the dataframe(s)
			df = pd.read_html(StringIO(str(tag)))[0] 
			# Adding info and handling nonstandard columns
			df["League"], df["TableIndex"] = h_txt, ti # Grab most recent Header text, establish table seq number
			city_dict = {'City (all in Florida)':'Florida','City (all in California)':'California', 'City (all in Arizona)':'Arizona'}
			city_col = [c for c in df.columns if c in list(city_dict.keys())] # Need to handle nonstandard City columns
			if set(city_col): # set(col_headers).intersection(set(['City (all in Florida)','City (all in California)', 'City (all in Arizona)'])):
				# Replace 'City in XX' columns
				df["City"], df["State"] = df[city_col[0]], city_dict[city_col[0]]
				df = df.drop(columns=city_col)
			if h_txt=="Arizona Fall League": # Known 'State' col missing from this wikitable
				df["State"] = "Arizona"
			try:
				df["State"] = df["Province"]
			except:
				pass
			if 'State' not in df.columns and 'Province' not in df.columns:
				try:
					df["State"] = df.filter(regex='^State/', axis=1).iloc[:,0]
					df = df.drop(columns=df.filter(regex='^State/', axis=1).columns.tolist())
				except:
					print("ERROR: New case in input tables.")
					pass
			# Strip leading and trailing " " from City, State
			df["City"], df["State"] = df["City"].astype(str).str.strip(), df["State"].astype(str).str.strip()
			# Export just in case
			df_list.append(df)
			ti += 1
   
	# Join and reformat all dfs
	df = pd.concat(df_list, axis=0).reset_index(drop=True)
	df.to_csv(output_csv_path)
	return df

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

# if __name__ == "__main__":
# Establish wikipedia user agent
wiki_user_headers = set_ua_headers()

# Get team soup and parse (custom function, NOT universal)
team_soup = cook_soup(TEAMS_LINK, wiki_user_headers, output_html_path=os.path.join("data","html","milb"))
team_df = read_milb_soup(team_soup, output_csv_path="wiki_team_tables.csv")

# Find City information and stats
team_df["City"], team_df["State"] = team_df["City"].strip(), team_df["State"].strip()
city_dfs, city_state_list = [], []
for index, row in team_df.iterrows():
	city, state, city_state = row["City"], row["State"], row["City"] + ", " + row["State"]
	print(city+", "+state)
	if city_state not in city_state_list:
		url = "https://en.wikipedia.org/wiki/" + city.replace(" ","_") + ",_" + state.replace(" ","_") # NOTE: Need to make these more robust, sometimes (Franklin, WI) county is in the middle
		city_soup = cook_soup(url, wiki_user_headers, output_html_path=os.path.join("data","html","city"))
		time.sleep(SLEEP_TIME)
		df = read_city_soup(city_soup)
		if not isinstance(df, pd.DataFrame):
			print("\nContinue: Not a dataframe\n")
			continue
		df["city_name"] = city 
		city_dfs.append(df)
		city_state_list.append(city_state)
	else:
		print("Continue: Already downloaded.")
		continue

# Concat
main_df = pd.concat(city_dfs, ignore_index=True)
# Export raw
main_df.to_csv("main_city_infobox_df.csv") # NOTE: Can use other cols ('first settled', etc.) to supplement "founded"
