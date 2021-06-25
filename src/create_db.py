import pymysql
from src import config


def create_connection_db():
    """

    :return:
    """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password=config.LOCAL_DB_PASSWORD,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def use_ironman_db(connection, db_name):
    """

    :param connection:
    :param db_name:
    :return:
    """
    with connection.cursor() as cursor:
        cursor.execute(f"""USE {db_name}""")


def create_database(connection, db_name):
    query_create_db = f"""CREATE DATABASE IF NOT EXISTS {db_name}"""
    with connection.cursor() as cursor:
        cursor.execute(query_create_db)


def create_tables(connection, which_tables=[]):
    query_create_races_table = f"""CREATE TABLE IF NOT EXISTS races(
                                race_link VARCHAR(100) PRIMARY KEY,
                                series VARCHAR(50),
                                title VARCHAR(50),
                                continent VARCHAR(50),
                                country VARCHAR(50),
                                race_card VARCHAR(50),
                                race_status VARCHAR(50),
                                swim VARCHAR(30),
                                bike VARCHAR(30),
                                run VARCHAR(30),
                                avg_air_temp INT,
                                avg_water_temp INT,
                                airport VARCHAR(3)
                                )"""

    query_create_events_table = f"""CREATE TABLE IF NOT EXISTS events_im(
                                event_id VARCHAR(100) NOT NULL PRIMARY KEY,
                                race_link VARCHAR(50) NOT NULL,
                                event VARCHAR(100) NOT NULL,
                                subevent VARCHAR(100) NOT NULL,
                                date DATE,
                                latitude DECIMAL(10,6),
                                longitude DECIMAL(10,6),
                                alias VARCHAR(100),
                                dist_km DECIMAL(6,2),
                                dist_mi DECIMAL(6,2),
                                city VARCHAR(100),
                                continent VARCHAR(50),
                                registration_status VARCHAR(50),
                                total_records INT
                                )"""

    query_create_results_table = f"""CREATE TABLE IF NOT EXISTS results(
                                result_id INT AUTO_INCREMENT UNIQUE KEY,
                                event_id VARCHAR(100),
                                subevent_name VARCHAR(50),
                                country VARCHAR(100),
                                age_group VARCHAR(50),
                                event_status VARCHAR(50),
                                swim_time_sec INT,
                                t1_time_sec INT,
                                bike_time_sec INT,
                                t2_time_sec INT,
                                run_time_sec INT,
                                finish_time_sec INT,
                                finish_rank_group INT,
                                finish_rank_gender INT,
                                finish_rank_overall INT,
                                rank_point DECIMAL(8,2),
                                name VARCHAR(100),
                                gender CHAR(1)
                                )"""

    # creates a mapping between the names of the tables to create and the queries
    dict_create_tables = {"races": query_create_races_table, "events_im": query_create_events_table,
                          "results": query_create_results_table}

    with connection.cursor() as cursor:
        for table in which_tables:
            cursor.execute(dict_create_tables.get(table))


def drop_table(connection, which_tables=[]):
    """

    :param connection:
    :param which_tables:
    :return:
    """
    for table in which_tables:
        query_drop_table = f"""DROP TABLE IF EXISTS {table}"""
        with connection.cursor() as cursor:
            cursor.execute(query_drop_table)


def update_races_table(connection, race_features):
    query_insert_races_table = f"""INSERT INTO races(race_link, series, title, continent, country, race_card, 
    race_status, swim, bike, run, avg_air_temp, avg_water_temp, airport) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s) """

    with connection.cursor() as cursor:
        cursor.executemany(query_insert_races_table, race_features)
        connection.commit()


def update_events_table(connection, event_features):
    query_insert_events_table = f"""INSERT INTO events_im(event_id, race_link, event, subevent, date, latitude, 
    longitude, alias, dist_km, dist_mi, city, continent, registration_status, total_records) VALUES(%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s) """

    with connection.cursor() as cursor:
        cursor.execute(query_insert_events_table, event_features)
        connection.commit()


def update_results_table(connection, result_features):
    query_insert_results_table = f"""INSERT INTO results(event_id, subevent_name, country, age_group, event_status, 
    swim_time_sec, t1_time_sec, bike_time_sec, t2_time_sec, run_time_sec, finish_time_sec, finish_rank_group, 
    finish_rank_gender, finish_rank_overall, rank_point, name, gender) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s) """

    with connection.cursor() as cursor:
        cursor.executemany(query_insert_results_table, result_features)
        connection.commit()


def get_event_id_missing_feature(connection, table, missing_feature):
    """
    Get the a list of event ids in a given table where data is missing in a specific column (=feature
    :param connection:
    :param table:
    :param missing_feature:
    :return:
    """

    query_missing_data = f"""SELECT event_id FROM {table} WHERE {missing_feature} is NULL"""

    with connection.cursor() as cursor:
        cursor.execute(query_missing_data)
        list_event_ids = [dictionary.get('event_id') for dictionary in cursor.fetchall()]

    return list_event_ids


def get_race_card_from_event_id(connection, event_id):
    """
    Gets the race card given an event id
    :param connection:
    :param event_id:
    :return:
    """

    query_race_card = f"""SELECT race_card FROM races as r JOIN events_im AS e ON r.race_link = e.race_link
                        WHERE event_id = '{event_id}'"""

    with connection.cursor() as cursor:
        cursor.execute(query_race_card)
        race_card = cursor.fetchone().get('race_card')

    return race_card


def update_missing_date_from_event_id(connection, event_id, table, feature_to_fill, value):
    """
    Updating the database with the value to fill with in for the given feature
    :param connection:
    :param event_id:
    :param table:
    :param feature_to_fill:
    :param value:
    :return:
    """
    if isinstance(value, float) or value == "NULL":
        query_update_missing_value = f"""UPDATE {table} SET {feature_to_fill} = {value} WHERE event_id = '{event_id}'"""
    elif isinstance(value, str):
        query_update_missing_value = f"""UPDATE {table} SET {feature_to_fill} = "{value}" WHERE event_id = '{event_id}'"""
    elif value is None:
        query_update_missing_value = f"""UPDATE {table} SET {feature_to_fill} = NULL WHERE event_id = '{event_id}'"""
    else:
        query_update_missing_value = """"""

    with connection.cursor() as cursor:
        cursor.execute(query_update_missing_value)
        connection.commit()


def deduct_distances_from_series_feature(connection, event_id):
    """
    Deduct the distance of the event from the series name
    :param connection:
    :param event_id:
    :return:
    """

    query_deduct_distance = f"""SELECT CASE 
                                            WHEN series LIKE '%70.3%' THEN 70.30
                                            WHEN series LIKE '%5150%' THEN 32.00
                                            ELSE 140.60
                                        END AS distance_miles
                            FROM races AS r LEFT JOIN events_im AS e ON r.race_link = e.race_link 
                            WHERE event_id = '{event_id}' """

    with connection.cursor() as cursor:
        cursor.execute(query_deduct_distance)
        distance_miles = float(cursor.fetchone().get('distance_miles'))

    distance_kms = config.dict_miles_to_kms.get(distance_miles)

    return distance_miles, distance_kms


def deduct_city_from_race_card(connection, event_id):
    """
    Deduct the city from the race_card name
    :param connection:
    :param event_id:
    :return:
    """
    query_deduct_city = f"""SELECT race_card
                            FROM races AS r LEFT JOIN events_im AS e ON r.race_link = e.race_link 
                            WHERE event_id = '{event_id}' """

    with connection.cursor() as cursor:
        cursor.execute(query_deduct_city)
        race_card_name = cursor.fetchone().get('race_card')

    city = race_card_name.split(',')[config.FIRST_RESULT]

    return city


def deduct_continent_from_races(connection, event_id):
    """
    Deduct the continent from races continent
    :param connection:
    :param event_id:
    :return:
    """
    query_deduct_continent = f"""SELECT r.continent
                            FROM races AS r LEFT JOIN events_im AS e ON r.race_link = e.race_link 
                            WHERE event_id = '{event_id}' """

    with connection.cursor() as cursor:
        cursor.execute(query_deduct_continent)
        races_continent = cursor.fetchone().get('continent')

    events_continent = config.dict_continents.get(races_continent)

    return events_continent


if __name__ == '__main__':

    connection = create_connection_db()
    use_ironman_db(connection, config.database_name)
    print(deduct_continent_from_races(connection, event_id='00AFC145-4270-E611-940F-005056951BF1'))
