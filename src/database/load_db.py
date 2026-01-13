import sqlite3
import pandas as pd
from datetime import datetime

DB_FILE = "database/milb.sqlite"

def get_connection():
    return sqlite3.connect(DB_FILE)