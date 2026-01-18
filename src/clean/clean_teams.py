# Imports
import pandas as pd
import datetime as dt
# import numpy as np
# import os, re
from io import StringIO
import sqlite3

# Constants
# NA

# Functions
def read_milb_soup(soup, output_csv_path="read_city_soup_test.csv"):
	# Check if soup looks like HTML
	if not hasattr(soup, "find"):
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

def get_mascot_name(row):
    '''Estimate the mascot name based on criteria applied to team name str.'''
    team, city = row["Team"], row["City"]
    # Remove City name in team if present
    if city in team:
        return team.replace(city, "").strip()
	# If remaining string >2 take final value
    if len(team.split()) >= 2:
        return team.split()[-1]
    # Else none
    else:
        return team



dtypes = {
    "Index": "INTEGER",
    "Team": "TEXT",
    "Division": "TEXT",
    "City": "TEXT",
    "State": "TEXT",
    "Stadium": "TEXT",
    "Capacity": "INTEGER",
    "Affiliate": "TEXT"
}

conn = sqlite3.connect("minor_league.db")
cursor = conn.cursor()

# Drop table if exists
cursor.execute("DROP TABLE IF EXISTS teams;")

# Build CREATE TABLE SQL using the dtypes dict
columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in dtypes.items()])
create_table_sql = f"CREATE TABLE teams ({columns_sql});"
cursor.execute(create_table_sql)
conn.commit()

# Insert DataFrame rows into the table
for _, row in df.iterrows():
    placeholders = ", ".join(["?"] * len(row))
    cursor.execute(f"INSERT INTO teams VALUES ({placeholders})", tuple(row))
conn.commit()

# Verify
query = "SELECT * FROM teams LIMIT 5;"
for row in cursor.execute(query):
    print(row)

# Close connection
conn.close()