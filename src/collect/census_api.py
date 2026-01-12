import os 
from dotenv import load_dotenv
import pandas as pd
import census 
from us import states

dotenv_path = os.path.join(os.path.dirname(__file__), "..", "...", ".env") # One level up
load_dotenv(dotenv_path=dotenv_path)
census_api_key = os.getenv("CENSUS_API_KEY")

def add_census_data(city, state):
    ## Pull city statistics straight from the Census
    c, s = city, state
    # Example: Get total population for all cities in Massachusetts (FIPS code 25)
    data = census.Census(census_api_key).acs5.get(('NAME', 'B01003_001E'), {'for': 'place:'+str(c), 'in': f'state:{states.s.fips}'})
	# NOTE: Use this instead, getattr(state, 'NY').fips
	# Total pop for previous Censuses (inc. population change)
	# Median wage in town vs. state, unemployment rate, family/household size, people under X age or over X age, average weather or climate zone
	# Size of service economy, avg. age, distance to largest city in the state or similar

	# Convert to a pandas DataFrame for easier handling
    dfc = pd.DataFrame(data)
    print(dfc.head())
