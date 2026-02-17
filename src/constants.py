# Imports
import os
from src.utils.html import set_user_agent
from dotenv import load_dotenv
load_dotenv()

# API keys
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
USER_AGENT = set_user_agent(header_file=os.path.abspath(os.path.join(".","user-agent.txt")))

# Data source years
CBP_YEAR = 2023
ACS_YEAR = 2023
FRED_START_YEAR = 2000 # Needed?

# NAICS
NAICS_CODE = "00"

# DB
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "database", "milb.sqlite")

# Settings
SLEEP_TIME = 6
BENCHMARK_YEAR = 9999

# Wiki links
TEAMS_LINK = "https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams"
