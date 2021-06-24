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
