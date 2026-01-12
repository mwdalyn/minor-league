# MinorLeague

## Description


## Procedure

Collect: Create (3) major tables through scraping, other sourcing methods.
1. Teams (Wikipedia, scrape)
2. City (Wikipedia, scrape; US Govt inc. Census, FRED; Regrid; OSMnx)
3. Players (MLB-StatsAPI)

Clean: Self-explanatory.



## API Key Setup

USCB 
Economic Census 2022 https://api.census.gov/data/2022/ecnbasic (API Key)
Population Estimate APIs v.2023 api.census.gov/data/2023/pep/charv (API Key)
Census DHC 2020 https://www.census.gov/library/video/2023/adrm/2020-demographic-and-housing-characteristics-file-using-the-census-data-api-to-get-a-table.html

Get API Key:

Visit the Provided Link [Click Here](Provide the link here).
Follow the instructions to create an account and obtain your API key.

## Project Structure (Tweaked from Suggestion, CGPT5.2)
milb_city_analysis/
│
├── README.md
├── requirements.txt 
├── .env # 
│
├── data/
│   ├── raw/
│   │   ├── wikipedia/
│   │   ├── census/
│   │   ├── fred/
│   │   └── mlb/
│   │
│   ├── mid/ 
│   │
│   └── fin/
│       ├── city_features.parquet
|       ├── team_features.parquet
|       ├── player_features.parquet
│       └── modeling_table.parquet
│
├── database/
│   └── milb.sqlite # Can be .db but .sqlite is more informative, e.g. defines the originating module  
│
├── src/ # "Library code" (reusable, importable, testable); "how things work"
│   ├── __init__.py
│   │
│   ├── collect/
│   │   ├── __init__.py
│   │   ├── wikipedia_scraper.py
│   │   ├── census_api.py
│   │   ├── fred_api.py
│   │   └── mlb_api.py
│   │
│   ├── clean/
│   │   ├── __init__.py
│   │   ├── clean_teams.py
│   │   ├── clean_cities.py
│   │   └── crosswalks.py # Allegedly a standard file, that standardizes names and creates mappings (e.g. for multiple sources of data on the same entity, like a city)
│   │
│   ├── database/
│   │   ├── __init__.py
│   │   ├── schema.py
│   │   ├── db_utils.py
│   │   └── load_tables.py
│   │
│   ├── features/ # Future
│   │
│   ├── modeling/ # Future
│   │
│   └── utils/ # Future
│
├── notebooks/ # Future (Streamlit)
│
└── scripts/ # Optional, likely not using; single entry points were suggested 
    ├── run_ingest.py
    ├── build_db.py
    └── run_model.py
