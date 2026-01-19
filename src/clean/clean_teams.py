# Imports
import pandas as pd
import datetime as dt
# import numpy as np
import os, re, glob
from io import StringIO
import sqlite3
from src.utils.html import find_latest_html, cook_html

# Constants
DB_PATH = os.path.abspath(os.path.join('.','database','milb.sqlite'))

CREATE_TABLE_SQL = """
	CREATE TABLE IF NOT EXISTS minor_league_teams (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	Team TEXT NOT NULL,
	Division TEXT,
	City TEXT,
	State TEXT,
	Stadium TEXT,
	Capacity INTEGER,
	Affiliate TEXT,
	League TEXT,
	TableIndex INTEGER,
	Affiliates TEXT,
	Mascot TEXT,
	created_on TEXT DEFAULT CURRENT_TIMESTAMP,
	updated_on TEXT DEFAULT CURRENT_TIMESTAMP,
	UNIQUE (Team, City, League)
);
"""

CREATE_UPDATE_TRIGGER_SQL = """
	CREATE TRIGGER IF NOT EXISTS trg_minor_league_teams_updated
	AFTER UPDATE ON minor_league_teams
	FOR EACH ROW
	BEGIN
		UPDATE minor_league_teams
		SET updated_on = CURRENT_TIMESTAMP
		WHERE id = OLD.id;
	END;
	"""

UPSERT_SQL = """
	INSERT INTO minor_league_teams (
		Team, Division, City, State, Stadium,
		Capacity, Affiliate, League,
		TableIndex, Affiliates, Mascot
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (Team, City, League) DO UPDATE SET
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
def read_milb_soup(soup, output_csv_path=None):
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
				df = df.drop(columns="Province")
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
	if output_csv_path:
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

def upsert_minor_league_teams(df, db_path=DB_PATH):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()

	cursor.execute(CREATE_TABLE_SQL)
	cursor.execute(CREATE_UPDATE_TRIGGER_SQL)
	records = [
	(
		row.Team,
		row.Division,
		row.City,
		row.State,
		row.Stadium,
		int(row.Capacity) if pd.notna(row.Capacity) else None,
		row.Affiliate,
		row.League,
		int(row.TableIndex) if pd.notna(row.TableIndex) else None,
		row.Affiliates,
		row.Mascot,
	)
	for row in df.itertuples(index=False)
	]
	cursor.executemany(UPSERT_SQL, records)
	conn.commit()
	conn.close()

def clean_teams():
	soup_html = cook_html(find_latest_html(os.path.abspath(os.path.join('.','data','raw','wikipedia','milb'))))
	table = read_milb_soup(soup_html)
	table["Mascot"] = table.apply(get_mascot_name, axis=1)
	upsert_minor_league_teams(table, db_path=DB_PATH)

# clean_teams()