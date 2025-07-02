from utils.ssh_tunnel import get_ssh_tunnel  
from datetime import datetime
import pymysql
import sqlite3
import logging

from globals import DATA_PATH, MYSQL_CONFIG, SSH_CONFIG

from datetime import datetime

def import_sqlite_to_mysql(mysql_connection):
    """Import all data from SQLite to MySQL database using existing MySQL connection."""

    sqlite_db_path = DATA_PATH + '/weather_data.db'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_connection.cursor()

    try:
        mysql_cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_observations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                temp FLOAT,
                temp_in FLOAT,
                humidity INT,
                humidity_in INT, 
                pressure_abs FLOAT,
                pressure_rel FLOAT,
                rain_rate FLOAT,
                rain_event FLOAT,
                rain_hourly FLOAT,
                rain_daily FLOAT,
                rain_weekly FLOAT,
                rain_monthly FLOAT,
                rain_yearly FLOAT,
                wind_degree FLOAT,
                wind_gust FLOAT,
                wind_gust_maxdaily FLOAT,
                wind_speed FLOAT,
                solarradiation FLOAT,
                uv INT
            )
        ''')

        sqlite_cursor.execute('''
            SELECT timestamp, temp, temp_in, humidity, humidity_in, pressure_abs, pressure_rel,
                   rain_rate, rain_event, rain_hourly, rain_daily, rain_weekly, rain_monthly,
                   rain_yearly, wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                   solarradiation, uv
            FROM weather_observations
        ''')
        rows = sqlite_cursor.fetchall()
        total = len(rows)
        logging.info("Found %d rows in SQLite to import.", total)

        if total > 0:
            insert_sql = '''
                INSERT INTO weather_observations (
                    timestamp, temp, temp_in, humidity, humidity_in,
                    pressure_abs, pressure_rel, rain_rate, rain_event,
                    rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                    wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                    solarradiation, uv
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''

            def convert_batch(batch):
                for row in batch:
                    row = list(row)
                    if isinstance(row[0], datetime):
                        row[0] = row[0].strftime('%Y-%m-%d %H:%M:%S')
                    yield tuple(row)

            mysql_connection.autocommit(False)
            batch_size = 250

            for i in range(0, total, batch_size):
                batch = rows[i:i+batch_size]
                converted = list(convert_batch(batch))
                mysql_cursor.executemany(insert_sql, converted)
                logging.info("Imported %d of %d rows...", min(i + batch_size, total), total)

            mysql_connection.commit()
            logging.info("Import complete: %d total rows imported.", total)
        else:
            logging.info("No rows found in SQLite. Nothing to import.")

    except Exception as e:
        logging.error("Import failed: %s", str(e))

    finally:
        sqlite_cursor.close()
        sqlite_conn.close()
        mysql_cursor.close()

def table_exists(mysql_connection):
    """Check if the weather_observations table exists in the MySQL database."""
    cursor = mysql_connection.cursor()
    try:
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_name = %s
            AND table_schema = %s
        """, ('weather_observations', MYSQL_CONFIG['database']))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logging.error(f"Error checking if table exists: {e}", exc_info=True)
        return False
    finally:
        cursor.close()

def save_to_db(data, db_type='sqlite', conn=None):
    """
    Save data to a SQLite or MySQL database based on the specified db_type.
    Uses a persistent connection for MySQL.
    """
    try:
        if db_type == 'sqlite':
            # Handle SQLite database operations
            with sqlite3.connect(DATA_PATH + '/weather_data.db') as connection:
                cursor = connection.cursor()
                logging.info("Trying to save data to SQLite database...")
                
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

                logging.info("Data successfully saved to SQLite database.")
        
        elif db_type == 'mysql' and conn is not None:
            
            # Handle MySQL database operations using the persistent connection
            cursor = conn.cursor()
            logging.info("Trying to save data to MySQL database...")

            try:
                cursor.execute('''
                    INSERT INTO weather_observations (
                        timestamp, temp, temp_in, humidity, humidity_in,
                        pressure_abs, pressure_rel, rain_rate, rain_event,
                        rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                        wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                        solarradiation, uv
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    data['timestamp'], data['temp'], data['temp_in'], data['humidity'], data['humidity_in'],
                    data['pressure_abs'], data['pressure_rel'], data['rain_rate'], data['rain_event'],
                    data['rain_hourly'], data['rain_daily'], data['rain_weekly'], data['rain_monthly'],
                    data['rain_yearly'], data['wind_degree'], data['wind_gust'], data['wind_gust_maxdaily'],
                    data['wind_speed'], data['solarradiation'], data['uv']
                ))
                
                conn.commit()
                logging.info("Data successfully saved to MySQL database.")
        
            except pymysql.MySQLError as e:
                logging.error(f"Database error (mysql): {e}")
                if conn and not conn.open:
                    # Try reconnecting once if connection is lost
                    logging.info("Re-establishing MySQL connection...")
                    conn.ping(reconnect=True)
                    # Retry the query after reconnect
                    save_to_db(data, db_type, conn)
                    
    except Exception as e:
        logging.error(f"Unexpected error during database operation: {e}")
    finally:
        if db_type == 'mysql' and cursor:
            cursor.close()