# Imports
import requests 
from bs4 import BeautifulSoup
import datetime as dt
import os, re

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

def cook_soup(url, headers, html_file_path = None, parser = 'html.parser'):
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
		if html_file_path:
			url_page = "_".join(url.split("/")[-1].split("_")[0:4]).lower() # Grab first four phrases in the final segment of wiki url as unique ID
			with open(os.path.join(html_file_path, f"wiki_{url_page}_{str(timestamp)}.html"), "w", encoding='utf-8-sig') as file: 
				file.write(str(soup))
	except Exception as e:
		print(f'Error: {e}')
		soup = f'Error: {e}'
	return soup

def find_latest_html(folder_path, internal_text=None):
	if internal_text:
		pattern = re.compile(
			rf"wiki_{re.escape(internal_text)}_(\d{{8}}_\d{{6}})\.html"
		)
		timestamp_group = 1
	else:
		pattern = re.compile(r"wiki_(.*?)_(\d{8}_\d{6})\.html")
		timestamp_group = 2

	latest_file, latest_date = None, None

	for filename in os.listdir(folder_path):
		match = pattern.fullmatch(filename)
		if not match:
			continue

		date_str = match.group(timestamp_group)
		file_date = dt.datetime.strptime(date_str, "%Y%m%d_%H%M%S")

		if latest_date is None or file_date > latest_date:
			latest_date = file_date
			latest_file = filename

	if latest_file is None:
		return None

	return os.path.join(folder_path, latest_file)

def cook_html(html_file_path):
	with open(html_file_path, 'r', encoding="utf-8-sig") as f:
		# Read the file's content into a variable
		html_content = f.read()	
		# Create a BeautifulSoup object by passing the HTML content and specifying a parser
		soup = BeautifulSoup(html_content, 'html.parser')
	return soup
