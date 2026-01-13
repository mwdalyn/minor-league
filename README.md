## MinorLeague

### Description

Exploring a curiosity about Minor League Baseball. 
- What makes a Minor League team viable? 
- Where do Minor League teams thrive?
- What kind of city best houses a Minor League team?
- How has the Minor League landscape changed over time? 
- Who creates 'stars'? Who gets 'stars'?
- Who owns the teams?

### Procedure

Collect: Create (3) major tables through scraping, other sourcing methods.
1. Teams (Wikipedia, scrape)
2. City (Wikipedia, scrape; US Govt inc. Census, FRED; Regrid; OSMnx)
3. Players (MLB-StatsAPI)

Clean: Self-explanatory. Get ready for database. 

Database: Point to cleaned data, organize based on schema. Load into the database using sqlite3, etc.

Features, Model, Viz, Notebooks: TBD.

### API Key Setup
#### U.S. Census Bureau (USCB)
Economic Census 2022 https://api.census.gov/data/2022/ecnbasic (API Key)
Population Estimate APIs v.2023 api.census.gov/data/2023/pep/charv (API Key)
Census DHC 2020 https://www.census.gov/library/video/2023/adrm/2020-demographic-and-housing-characteristics-file-using-the-census-data-api-to-get-a-table.html

Get API Key(s): [US Census API](https://api.census.gov/data/key_signup.html).

#### FRED
TBD

Get API Key(s): [Federal Reserve Bank of STL](https://fredaccount.stlouisfed.org/login/secure/).

### Project Structure 
Tweaked from suggestions made by ChatGPT v5.2
minor_league/
│  
├── README.md  
├── .venv/
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
│   └── milb.sqlite  
│  
├── src/   
│   ├── __init__.py  
│   │  
│   ├── clean/  
│   │   ├── __init__.py  
│   │   ├── clean_teams.py  
│   │   ├── clean_cities.py  
│   │   └── crosswalks.py   
│   ├── collect/  
│   │   ├── __init__.py  
│   │   ├── wikipedia.py  
│   │   ├── census_api.py  
│   │   ├── fred_api.py  
│   │   └── mlb_api.py  
│   ├── database/
│   │   ├── __init__.py
│   │   ├── schema.py
│   │   ├── db_utils.py
│   │   └── load_tables.py
│   │  
│   ├── viz/ # Future  
│   │    
│   ├── features/ # Future  
│   │  
│   ├── modeling/ # Future  
│   │  
│   └── utils/ # Future (for now mostly HTML interaction)
│  
├── notebooks/ # Future via Streamlit or other  
│  
└── scripts/ # Single entrypoints  
    ├── run_ingest.py  
    ├── build_db.py  
    └── run_model.py  

### Database Schema
Tentative. Players table not yet included. Want Team to have time dimension/facts table to track rebrands, history, etc.
```
erDiagram
    %% =======================
    %% Dimensions
    %% =======================

    DIM_CITY {
        int city_key PK
        string name
        string state
        string county
        string metro_area
        int year_founded
        int fips
        float elevation
        float city_sqmi
        float metro_sqmi
        timestamp created_on
        timestamp updated_on
    }

    DIM_TEAM {
        int team_key PK
        int year_founded
        timestamp created_on
        timestamp updated_on
    }

    DIM_TIME {
        int time_key PK
        int year
        int quarter
        int month
    }

    TEAM_HISTORY {
        int team_history_id PK
        int team_id FK
        string team_name
        string mascot
        string league
        string owner
        string affiliate
        string stadium
        int stadium_capacity
        int city_id FK
        date effective_start_year
        date effective_end_year
        boolean is_current
        timestamp created_on
        timestamp updated_on
    }

    %% =======================
    %% Fact Table
    %% =======================

    FACT_CITY_METRICS {
        int city_key FK
        int time_key FK
        int industry_key FK
        int population
        int population_delta
        int metro_population
        float employment_pct
        float avg_age
        float avg_income
        float median_income
        float housing_units
        float housing_vacancy
        float economic_activity_index
    }

    FACT_TEAM_PERFORMANCE {
        int team_id FK
        int time_id FK
        int wins
        int losses
        int attendance
        float revenue
    }

    %% =======================
    %% Relationships
    %% =======================
    DIM_CITY ||--o{ DIM_TEAM : "has"
    DIM_CITY ||--o{ FACT_CITY_METRICS : "measured by"
    DIM_TIME ||--o{ FACT_CITY_METRICS : "at"
    
    DIM_TEAM ||--o{ TEAM_HISTORY : "has history"
    DIM_CITY ||--o{ TEAM_HISTORY : "hosts"

    TEAM_HISTORY ||--o{ FACT_TEAM_PERFORMANCE : "applies to"
    DIM_TIME ||--o{ FACT_TEAM_PERFORMANCE : "at"
```
Miscellaneous other feature, datapoint ideas:
- Average temperature in March/July/October
- Proximity to nearest major city by car
- Population within X driving miles or hours (custom, not MSA)
- Google Search frequency
- Median and Average wage (in city and in state, and the ratio between)
- Average annual restaurant spend 
- Rental vs. homeowner ratio among single-family units above 1,000 sqft
- University in town
- Size of university population in town
- Bin size of age brackets (number of children, etc.)
- Married vs. single population between 20 - 45