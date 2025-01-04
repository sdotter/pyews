import sqlite3
import logging

def save_to_db(data):
    """Save data to a SQLite database."""
    try:
        connection = sqlite3.connect('./data/weather_data.db')
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                temperature REAL,
                pressure REAL,
                wind_gust REAL,
                wind_degree REAL,
                rain REAL,
                solarradiation REAL
            )
        ''')

        cursor.execute('''
            INSERT INTO readings (timestamp, temperature, pressure, wind_gust, wind_degree, rain, solarradiation)
            VALUES (:timestamp, :temperature, :pressure, :wind_gust, :wind_degree, :rain, :solarradiation)
        ''', data)

        connection.commit()

    except sqlite3.Error as e:
        logging.error("SQLite error: {}".format(e))
    finally:
        connection.close()