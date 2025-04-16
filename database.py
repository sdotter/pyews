from datetime import datetime
import sqlite3
import logging

from globals import DATA_PATH

def save_to_db(data):
    """Save data to a SQLite database."""
    try:
        connection = sqlite3.connect(DATA_PATH + '/weather_data.db')
        cursor = connection.cursor()

        logging.info("Trying to save data to database...")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                temp REAL,
                temp_in REAL,
                humidity INTEGER,
                humidity_in INTEGER, 
                pressure_abs REAL,
                pressure_rel REAL,
                rain_rate REAL,
                rain_event REAL,
                rain_hourly REAL,
                rain_daily REAL,
                rain_weekly REAL,
                rain_monthly REAL,
                rain_yearly REAL,
                wind_degree REAL,
                wind_gust REAL,
                wind_gust_maxdaily REAL,
                wind_speed REAL,
                solarradiation REAL,
                uv INTEGER
            )
        ''')

        cursor.execute('''
            INSERT INTO weather_observations (timestamp, temp, temp_in, humidity, humidity_in, pressure_abs, pressure_rel, rain_rate, rain_event, rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly, wind_degree, wind_gust, wind_gust_maxdaily, wind_speed, solarradiation, uv)
            VALUES (:timestamp, :temp, :temp_in, :humidity, :humidity_in, :pressure_abs, :pressure_rel, :rain_rate, :rain_event, :rain_hourly, :rain_daily, :rain_weekly, :rain_monthly, :rain_yearly, :wind_degree, :wind_gust, :wind_gust_maxdaily, :wind_speed, :solarradiation, :uv)
        ''', data)

        connection.commit()

        # Log the success of the commit operation
        logging.info("Data successfully saved to database")

    except sqlite3.Error as e:
        logging.error("SQLite error: {}".format(e))
    finally:
        connection.close()