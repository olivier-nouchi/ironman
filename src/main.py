import pickle
from src import config
import create_db
import scrape_ironman
import pymysql

if __name__ == "__main__":

    # Boolean whether to scrape the event ids (takes ~10 minutes)
    is_scrape_event_id = True

    # Creates a connection to the local database
    connection = create_db.create_connection_db()

    # If the database "ironman" exists, we use it, otherwise we create it and the according tables
    try:
        create_db.use_ironman_db(connection, config.database_name)
        create_db.create_tables(connection, which_tables=["results", "events_im", "races"])

    except pymysql.err.OperationalError:
        print("Did not find the Ironman database. Creating it...")
        create_db.create_database(connection, config.database_name)
        print("Creating the tables...")
        create_db.create_tables(connection, which_tables=["results", "events_im", "races"])
        create_db.use_ironman_db(connection, config.database_name)

    # create_db.drop_table(connection, which_tables=["events"])

    # Creates a dictionary that contains the links to all the existing races on the Ironman website ironman.com
    dict_races = scrape_ironman.get_race_names()

    # Creates an empty list to store the ids of the events
    list_event_id = []
    # Creates a dictionary to store the information relative to the events
    dict_event_id_race = {}

    # Scrape the events
    try:
        # Open the existing list eventIDs from the pkl:
        with open(config.dict_event_id_pickle_filepath, 'rb') as handle:
            dict_event_id_race = pickle.load(handle)

        # If we want to scrape, we'll update the dictionary with the new event ids:
        if is_scrape_event_id:
            scraped_dict_event_id_race = scrape_ironman.get_dict_event_id(dict_races)
            dict_event_id_race.update(scraped_dict_event_id_race)

    except FileNotFoundError:
        print("No pickle file found")
        # Have to create a dictionary with the event ids
        dict_event_id_race = scrape_ironman.get_dict_event_id(dict_races)

    # Save this new dictionary updated or not
    print(f"A total of {len(dict_event_id_race)} events are being saved in the pickle...")
    with open(config.dict_event_id_pickle_filepath, 'wb') as handle:
        pickle.dump(dict_event_id_race, handle)

    # 1. RACES

    # Retrieve from the database all the existing race links
    lst_existing_races = scrape_ironman.get_existing_event_id(connection, pk_column='race_link', table='races')
    # Create a list with all the scraped race links
    lst_site_races = dict_races.get('race_link')
    # Only scrape the races which are not yet in the database (intersection between existing ones in the db and scraped)
    lst_scrape_from_races = scrape_ironman.get_to_scrape_event_id(lst_site_races, lst_existing_races)

    # List of tuples where the first element is the race link and its race features
    race_features = [v for v in zip(*[dict_races[k] for k in dict_races.keys()])]

    # Keep only the races to scrape from associated with their features
    race_features_to_scrape_from = []
    for race in race_features:
        if race[config.RACE_LINK_DICT_RACES] in lst_scrape_from_races:
            race_features_to_scrape_from.append(race)

    print(f"There are {len(race_features_to_scrape_from)} races to scrape from.")

    # Add them to the races table
    create_db.update_races_table(connection, race_features_to_scrape_from)

    # 2. EVENTS

    # Retrieve from the database all the existing event ids
    lst_existing_event_id = scrape_ironman.get_existing_event_id(connection, pk_column='event_id', table='events_im')
    # if we did not scrape any events, then we need to scrape
    if not dict_event_id_race:
        lst_site_event_id = scrape_ironman.get_site_event_id(dict_races)
    else:
        lst_site_event_id = list(dict_event_id_race.keys())

    # Only scrape the event ids which are not yet in the database (intersection between existing ones in the db and
    # scraped)
    lst_scrape_from_event_id = scrape_ironman.get_to_scrape_event_id(lst_site_event_id, lst_existing_event_id)
    print(f"There are {len(lst_scrape_from_event_id)} events to scrape from.")

    scrape_ironman.scrape_events(connection, dict_event_id_race, lst_scrape_from_event_id)

    # 3. RESULTS

    # Retrieve from the database all the existing results (per event id)
    lst_existing_event_id_results = scrape_ironman.get_existing_event_id(connection, pk_column='event_id',
                                                                         table='results')

    # Only scrape the event ids which are not yet in the database (intersection between existing ones in the db and
    # scraped)
    lst_scrape_from_event_id_results = scrape_ironman.get_to_scrape_event_id(lst_site_event_id,
                                                                             lst_existing_event_id_results)
    print(f"There are {len(lst_scrape_from_event_id_results)} events (results) to scrape from.")

    scrape_ironman.scrape_event_results(connection, lst_scrape_from_event_id_results)
