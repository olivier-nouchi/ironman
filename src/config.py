import os
import json
from pathlib import Path

# Directories
# root directory of the project
ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute()
project_dir = Path(ROOT_DIR).parent.absolute()
input_dir = "input"
pickle_dir = "pickles"

# Filenames
user_config_filename = "user_config.json"
dict_event_id_race_filename = "dict_event_id_race.pkl"

# Various links to the ironman.com website
URL_ALL_RACES = """https://sheets.googleapis.com/v4/spreadsheets/1yLtxUETnuF3UZLmypYkAK6Vj4PE9Fo_BT-WsA4oE_YU/values/Race-Catalog?key=AIzaSyC9s2sNhwUZOUXJfnyt-cD4k4nUyY-3HBs """
URL_RESULTS = """https://api.competitor.com/public/result/subevent/"""
URL_EVENT = """https://api.competitor.com/public/events/"""
IRONMAN_URL = 'https://ironman.com'

URL_GEO_LOCATION = """https://api.opencagedata.com/geocode/v1/json?q="""
PARAMETERS_URL_GEO_LOCATION = """&key=641c51bed8ab490184632ad8526e29ad&no_annotations=1&language=en"""

# open the user config json file
user_config_filepath = os.path.join(ROOT_DIR, input_dir, user_config_filename)
with open(user_config_filepath, 'rb') as f:
    user_config = json.load(f)

# Headers for the scraping with the private key
HEADERS = {"wtc_priv_key": user_config.get("wtc_priv_key")}
LOCAL_DB_PASSWORD = user_config.get("user_local_db_password")

# Position of the race link in the dicitionary races
RACE_LINK_DICT_RACES = 0
FIRST_RESULT = 0

# Mapping continents with acronyms
dict_continents = {"Africa": "AF", "Asia": "AS", "Europe": "EU", "North America": "NA", "Oceania": "OC",
                   "South America": "SA"}

# Mapping distances miles to kms
dict_miles_to_kms = {70.30: 113.00, 140.60: 226.00, 32.00: 51.50}

# Pickles
dict_event_id_pickle_filepath = os.path.join(ROOT_DIR, pickle_dir, dict_event_id_race_filename)

# Database names
database_name = 'ironman_db'
