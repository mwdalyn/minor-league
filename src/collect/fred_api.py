import os 
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "..", "...", ".env") # One level up
load_dotenv(dotenv_path=dotenv_path)
fred_api_key = os.getenv("FRED_API_KEY")
