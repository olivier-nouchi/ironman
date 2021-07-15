import requests
import config
import urllib
import create_db
import scrape_ironman


def get_missing_latitude_longitude(place_name_to_look_for):
    """
    Site internet: https://www.gps-coordinates.net/
    :param place_name_to_look_for: place looking for the latitude and longitude, string
    :return: (latitude, longitude), tuple
    """
    url = config.URL_GEO_LOCATION + urllib.parse.quote(place_name_to_look_for) + config.PARAMETERS_URL_GEO_LOCATION
    # Default header, otherwise the request wouldn't go through
    page = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, "
                                                    "like Gecko) Chrome/51.0.2704.103 Safari/537.36"})
    response = page.json()
    try:
        first_result = response.get('results')[config.FIRST_RESULT]
        latitude = first_result.get('geometry').get('lat')
        longitude = first_result.get('geometry').get('lng')
        return latitude, longitude
    except IndexError:
        # place_name_to_look_for = " ".join(place_name_to_look_for.split(",")[:-1])
        print(url)
        print(f"No latitude, longitude found for {place_name_to_look_for}")
        return 'NULL', 'NULL'


def fill_missing_lat_long(cnx):
    """
    Filling missing latitudes, longitudes
    :param cnx:
    :return: None
    """
    # Gets a list of all the events because supposedly no events is missing the event_id
    list_events_missing_lat_long = create_db.get_event_id_missing_feature(cnx, table="events_im",
                                                                          missing_feature="event_id")
    for event_id in list_events_missing_lat_long:
        place_name = create_db.get_race_card_from_event_id(cnx, event_id)
        lat, long = get_missing_latitude_longitude(place_name)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="latitude", value=lat)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="longitude", value=long)


def fill_missing_distances(cnx):
    """
    Filling missing distances miles and kms based on the series feature in the races table
    :param cnx:
    :return: None
    """
    # Some events don't have the right distance, and therefore we update all the events with the right distances

    list_all_events = scrape_ironman.get_existing_event_id(con=connection, pk_column="event_id", table="events_im")

    for event_id in list_all_events:
        dist_miles, dist_kms = create_db.deduct_distances_from_series_feature(cnx, event_id)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="dist_mi", value=dist_miles)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="dist_km", value=dist_kms)


def fill_missing_city(cnx):
    """
    Filling missing cities in the events_im table based on info from the races table
    :param cnx:
    :return: None
    """
    list_events_missing_city = create_db.get_event_id_missing_feature(cnx, table="events_im",
                                                                      missing_feature="city")

    for event_id in list_events_missing_city:
        city = create_db.deduct_city_from_race_card(cnx, event_id)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="city", value=city)


def fill_missing_continent(cnx):
    """
    Filling missing continent in the events_im table based on info from the races table
    :param cnx:
    :return: None
    """
    list_events_missing_continent = create_db.get_event_id_missing_feature(cnx, table="events_im",
                                                                           missing_feature="continent")

    for event_id in list_events_missing_continent:
        continent = create_db.deduct_continent_from_races(cnx, event_id)
        create_db.update_missing_date_from_event_id(cnx, event_id=event_id, table="events_im",
                                                    feature_to_fill="continent", value=continent)


def correct_wrong_distances(cnx):
    """
    Some distances are wrong: IRONMAN are sometimes
    :param cnx:
    :return:
    """


if __name__ == '__main__':

    # Creates a connection to the local database
    connection = create_db.create_connection_db()
    create_db.use_ironman_db(connection, config.database_name)

    # Fill the latitude and longitude (if latitude is missing, we assume that longitude is missing as well
    fill_missing_lat_long(connection)

    # Fill the missing distances in km and miles
    fill_missing_distances(connection)

    # Fill the missing city
    fill_missing_city(connection)

    # Fill the missing continent
    fill_missing_continent(connection)
