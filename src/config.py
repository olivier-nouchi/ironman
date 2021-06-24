import os
import json

# Directories
cwd = os.getcwd()
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

# open the user config json file
user_config_filepath = os.path.join(cwd, input_dir, user_config_filename)
with open(user_config_filepath, 'rb') as f:
    user_config = json.load(f)

# Headers for the scraping with the private key
HEADERS = {"wtc_priv_key": user_config.get("wtc_priv_key")}
LOCAL_DB_PASSWORD = user_config.get("user_local_db_password")

# Position of the race link in the dicitionary races
RACE_LINK_DICT_RACES = 0

# Pickles
dict_event_id_pickle_filepath = os.path.join(cwd, pickle_dir, dict_event_id_race_filename)

# Database names
database_name = 'ironman_db'


