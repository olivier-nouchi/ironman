import helper_functions
from tqdm import tqdm
import config
from bs4 import BeautifulSoup as soup
import requests
import time
import create_db
import pandas as pd

def update_race_status():
    """
    Update the race status since the registration can change over time
    :return:
    """
    #TODO create a function to update the race status over time
    pass

def get_existing_event_id(connection, pk_column='event_id', table='events_im'):
    """
    Get a list of the existing event id in the database
    :param connection:
    :param pk_column:
    :param table:
    :return:
    """
    q = f"select {pk_column} from {table}"
    list_existing_event_id = pd.read_sql(q, connection)[pk_column].tolist()

    return list_existing_event_id


def get_site_event_id(dictionary_races):
    """
    get a list of the event id listed on the website ironman.com
    :param dictionary_races:
    :return:
    """
    dict_event_id = get_dict_event_id(dictionary_races)
    list_site_event_id = list(dict_event_id.keys())
    return list_site_event_id


def get_to_scrape_event_id(list_site_event_id, list_existing_event_id):
    """
    get a list of the event id to scrape from (intersection of what already exists in the db and the ones listed on
    the website)
    :param list_site_event_id:
    :param list_existing_event_id:
    :return:
    """
    list_scrape_from_event_id = set(list_site_event_id) - set(list_site_event_id).intersection(
        set(list_existing_event_id))
    return list_scrape_from_event_id


def get_race_names():
    """
    Returns a dictionary with the information about the races (including their links)
    """
    page = requests.get(config.URL_ALL_RACES)
    all_races = page.json()

    SERIES, TITLE, CONTINENT, COUNTRY, RACE_CARD, RACE_STATUS, SWIM, BIKE, RUN = 3, 4, 5, 6, 7, 10, 11, 12, 13
    AIR_AVG_TEMP, WATER_AVG_TEMP, AIRPORT, RACE_LINK = 14, 15, 16, 17
    list_race_features = [RACE_LINK, SERIES, TITLE, CONTINENT, COUNTRY, RACE_CARD, RACE_STATUS, SWIM, BIKE, RUN,
                          AIR_AVG_TEMP, WATER_AVG_TEMP, AIRPORT]

    dict_races = {"race_link": [], "series": [], "title": [], "continent": [], "country": [], "race_card": [],
                  "race_status": [], "swim": [], "bike": [], "run": [],
                  "air_avg_temp": [], "water_avg_temp": [], "airport": []}

    assert (len(list_race_features) == len(dict_races))

    for race in tqdm(all_races.get('values')[1:]):  # the first one is the model
        for key, feature in zip(dict_races, list_race_features):
            dict_races[key].append(race[feature])

    # Converting to int and degrees the temperature
    for temp_feature in ['air_avg_temp', 'water_avg_temp']:
        dict_races[temp_feature] = [int(helper_functions.fahrenheit_to_celcius(int(temp))) if temp.isnumeric() else 0
                                    for temp in dict_races[temp_feature]]

    print(f"There are {len(dict_races.get('race_link'))} active ironman main events.")

    return dict_races


def get_dict_event_id(dict_races):
    """
    Build from the races links a dictionary with the mapping event_id: race
    """
    # Define the list of races (PK to identify the race)
    list_race_links = dict_races.get('race_link')
    dict_event_id_race = {}

    for race_url in tqdm(list_race_links):

        results_url = race_url + "-results"

        page = requests.get(results_url)
        content = soup(page.content, 'html.parser')

        for link in content.find_all("a", class_="tab-remote", href=True):
            year_link = link["href"]
            full_url = config.IRONMAN_URL + year_link

            year_page = requests.get(full_url)
            content_year = soup(year_page.content, 'html.parser')
            try:
                event_id = content_year.find("iframe")['src'].split('/')[-1]
                # Building a map of which event corresponds to which race
                dict_event_id_race[event_id] = race_url
            except Exception as e:
                print(e)
                # print(f'An error was found for: {results_url}')

    print(f"There are {len(dict_event_id_race)} ironman events.")
    return dict_event_id_race


def scrape_events(connection, dict_event_id_race, list_scrape_from_event_id):
    """
    Scrape the data related to events and stores it in the events table in the ironman database
    input: a list of event ids
    output: none
    """

    start_time = time.time()  # start time to evaluate how much time does the process take
    update_threshold = 0  # an update displays when the pct_progress > threshold

    # Scrape the data for each event
    for counter, event_id in tqdm(enumerate(list_scrape_from_event_id, 1)):

        # 1. Get the event details:
        url_event = config.URL_EVENT + event_id
        data = requests.get(url_event, headers=config.HEADERS).json()

        event = data.get('Event')
        subevent = data.get('SubEvent')
        date = data.get('Date')
        lat = float(data.get('Latitude')) if data.get('Latitude') is not None else data.get('Latitude')
        long = float(data.get('Longitude')) if data.get('Longitude') is not None else data.get('Longitude')
        alias = data.get('Alias')
        distance_km = float(data.get('DistanceInKM')) if data.get('DistanceInKM') is not None else data.get(
            'DistanceInKM')
        distance_miles = float(data.get('DistanceInMiles')) if data.get('DistanceInMiles') is not None else data.get(
            'DistanceInMiles')
        city = data.get('City')
        continent = data.get('Continent')
        registration_status = data.get('RegistrationStatus')

        # Linking to the race page
        race_link = dict_event_id_race.get(event_id)

        # Adding to the event the number of records for a sanity check later on
        parameters = """?%24limit=200&%24skip=0&%24sort%5BFinishRankOverall%5D=1"""
        url_results = config.URL_RESULTS + event_id + parameters
        total_records_event = requests.get(url_results, headers=config.HEADERS).json().get('total')

        event_features = (
            event_id, race_link, event, subevent, date, lat, long, alias, distance_km, distance_miles, city, continent,
            registration_status, total_records_event)

        # Updating the events table
        create_db.update_events_table(connection, event_features)

        # Following progress
        pct_progress = 100 * counter / (len(list_scrape_from_event_id) + 1)

        # Update only every 1%
        if pct_progress > update_threshold:
            # Give an ETA
            helper_functions.display_eta(start_time, pct_progress)
            # Update threshold
            update_threshold += 5


def scrape_event_results(connection, list_event_id_to_scrape_from):
    """
    Scrape the results for each event and stores it in the results table in the ironman database
    input: a list of event ids
    output: none
    """
    start_time = time.time()  # start time to evaluate how much time does the process take
    update_threshold = 0  # an update displays when the pct_progress > threshold

    # Scrape the data for each event
    for counter, event_id in tqdm(enumerate(list_event_id_to_scrape_from, 1)):

        parameters = """?%24limit=200&%24skip=0&%24sort%5BFinishRankOverall%5D=1"""
        url_results = config.URL_RESULTS + event_id + parameters
        total_records_event = requests.get(url_results, headers=config.HEADERS).json().get('total')
        # Creates a list with all the features to add for the event
        result_feature_list = []

        for skip in range(0, total_records_event, 200):

            parameters = """?%24limit=200&%24skip=""" + str(skip) + """&%24sort%5BFinishRankOverall%5D=1"""

            url_results = config.URL_RESULTS + event_id + parameters

            data = requests.get(url_results, headers=config.HEADERS).json()

            if data:

                for result in data.get('data'):
                    country = result.get('CountryISO2')
                    subevent_name = result.get('SubeventName')
                    age_group = result.get('AgeGroup')
                    event_status = result.get('EventStatus')

                    swim_time = result.get('SwimTime')
                    t1_time = result.get('Transition1Time')
                    bike_time = result.get('BikeTime')
                    t2_time = result.get('Transition2Time')
                    run_time = result.get('RunTime')
                    finish_time = result.get('FinishTime')
                    finish_rank_group = result.get('FinishRankGroup')
                    finish_rank_gender = result.get('FinishRankGender')
                    finish_rank_overall = result.get('FinishRankOverall')

                    rank_points = result.get('RankPoints')
                    athlete_name = result.get('Contact').get('FullName') if result.get('Contact') else ''
                    gender = result.get('Contact').get('Gender') if result.get('Contact') else ''

                    result_feature = (
                        event_id, subevent_name, country, age_group, event_status, swim_time, t1_time, bike_time,
                        t2_time, run_time, finish_time, finish_rank_group,
                        finish_rank_gender, finish_rank_overall, rank_points, athlete_name, gender)

                    result_feature_list.append(result_feature)

        # update the db
        create_db.update_results_table(connection, result_features=result_feature_list)

        # Following progress
        pct_progress = 100 * counter / (len(list_event_id_to_scrape_from) + 1)

        # Update only every 5%
        if pct_progress > update_threshold:
            # Give an ETA
            helper_functions.display_eta(start_time, pct_progress)

            # Update threshold
            update_threshold += 5

    print("FINISHED")

if __name__ == '__main__':
    pass
    # event_id = '4B014045-C399-E911-A97A-000D3A37468C'
    # event_id = 'E79709F2-36A2-E811-A960-000D3A3740B7'
    # parameters = """?%24limit=200&%24skip=0&%24sort%5BFinishRankOverall%5D=1"""
    # url_results = config.URL_RESULTS + event_id + parameters
    # print(url_results)
    # total_records_event = requests.get(url_results, headers=config.HEADERS).json()
    # print(total_records_event)