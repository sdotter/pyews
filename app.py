
import os

import sys
import signal
import pymysql
import requests
import threading
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request
from utils.ssh_tunnel import get_ssh_tunnel  
from utils.logging import logging, configure_logging
from data_processing import process_weather_data, should_process_data, save_to_24h_json, save_to_1w_json, save_to_1m_json, save_to_1y_json, save_to_custom_json, save_to_xml
from utils.ftp import upload_to_ftp
from database import save_to_db, import_sqlite_to_mysql, table_exists
from globals import *

app = Flask(__name__)

load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env'))

# Configure logging
configure_logging()

# Ensures import_from_sqlite_if_table_missing() is completed before continuing —
# even if a new /data/report/ API call is made while an import is still running
import_lock = threading.Lock()

# Global MySQL connection for the app
mysql_connection = None

def signal_handler(sig, frame):
    """Handle termination signals and cleanup resources properly."""
    logging.info("Termination signal received. Cleaning up...")
    close_mysql_connection()
    sys.exit(0)

# Register signal handlers for gracefully shutting down the application
signal.signal(signal.SIGINT, signal_handler)    # Handle interrupt signal (Ctrl+C)
signal.signal(signal.SIGTERM, signal_handler)   # Handle termination signal

def get_mysql_connection():
    """Get a persistent MySQL connection through the SSH tunnel."""
    global mysql_connection
    if mysql_connection is None or not mysql_connection.open:  
        ssh_tunnel = get_ssh_tunnel()  
        mysql_connection = pymysql.connect(
            host='127.0.0.1',
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            db=MYSQL_CONFIG['database'],
            port=ssh_tunnel.local_bind_port
        )
    return mysql_connection

def close_mysql_connection():
    """Close the MySQL connection when the application is shutting down."""
    global mysql_connection
    if mysql_connection:
        mysql_connection.close()
        mysql_connection = None
        logging.info("MySQL connection closed.")

def import_from_sqlite_if_table_missing():
    """Check MySQL table and import from SQLite if table doesn't exist (over SSH tunnel)."""
    try:
        conn = get_mysql_connection()
        if not table_exists(conn):
            logging.info("MySQL table does not exist. Importing data from SQLite...")
            import_sqlite_to_mysql(conn)

    except Exception as e:
        logging.error(f"Failed to check or import data: {e}")

def reset_ids_in_order(conn):
    """Resets the id column of the weather_archive table based on timestamp order."""
    
    table_name = "weather_archive"
    temp_table_name = "temp_weather_archive"
    cursor = conn.cursor()

    try:
        conn.begin()

        # Step 1: Create a temporary table with the same structure
        create_temp_table_query = f"CREATE TABLE {temp_table_name} LIKE {table_name};"
        cursor.execute(create_temp_table_query)

        # Step 2: Insert data sorted by timestamp into the temporary table
        insert_into_temp_table_query = f"""
            INSERT INTO {temp_table_name} (timestamp, temp, temp_in, humidity, humidity_in,
                pressure_abs, pressure_rel, rain_rate, rain_event,
                rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                solarradiation, uv)
            SELECT timestamp, temp, temp_in, humidity, humidity_in,
                pressure_abs, pressure_rel, rain_rate, rain_event,
                rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                solarradiation, uv
            FROM {table_name}
            ORDER BY timestamp ASC;
        """
        cursor.execute(insert_into_temp_table_query)

        # Step 3: Truncate the original table
        truncate_table_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_table_query)

        # Step 4: Copy data back from the temporary table
        insert_from_temp_table_query = f"""
            INSERT INTO {table_name} (timestamp, temp, temp_in, humidity, humidity_in,
                pressure_abs, pressure_rel, rain_rate, rain_event,
                rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                solarradiation, uv)
            SELECT timestamp, temp, temp_in, humidity, humidity_in,
                pressure_abs, pressure_rel, rain_rate, rain_event,
                rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                solarradiation, uv
            FROM {temp_table_name};
        """
        cursor.execute(insert_from_temp_table_query)

        # Step 5: Drop the temporary table
        drop_temp_table_query = f"DROP TABLE {temp_table_name};"
        cursor.execute(drop_temp_table_query)

        conn.commit()
        logging.info("✅ ID reset completed successfully.")
    except Exception as e:
        logging.error(f"Failed to reset IDs: {e}")
        conn.rollback()
    finally:
        cursor.close()

def import_saved_data_to_mysql(data_root='data', datatype='raw'):
    """One-time import of historical data from local files into MySQL."""

    logging.info("Starting one-time import of historical data to MySQL...")
    
    table_name = "weather_archive"

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL UNIQUE,
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
        );
    """

    insert_query = f"""
        INSERT IGNORE INTO {table_name} (
            timestamp, temp, temp_in, humidity, humidity_in,
            pressure_abs, pressure_rel, rain_rate, rain_event,
            rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
            wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
            solarradiation, uv
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    ssh = get_ssh_tunnel()

    conn = pymysql.connect(
        host='127.0.0.1',
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password'],
        db=MYSQL_CONFIG['database'],
        port=ssh.local_bind_port
    )

    cursor = conn.cursor()
    logging.info("Trying to create MySQL table if not exists...")
    cursor.execute(create_table_query)

    base_path = os.path.join(data_root, datatype)
    total_files = 0
    row_count = 0
    skipped = 0
    errors = 0

    for year in sorted(os.listdir(base_path)):
        year_path = os.path.join(base_path, year)
        if not os.path.isdir(year_path):
            continue

        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue

            for fname in sorted(os.listdir(month_path)):
                if not fname.endswith('.txt'):
                    continue

                total_files += 1
                file_path = os.path.join(month_path, fname)
                logging.info(f"📂 Importing file: {file_path}")

                batch = []

                with open(file_path, 'r') as file:
                    for line in file:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue

                        parts = line.split(',')
                        if len(parts) not in [12, 14]:
                            skipped += 1
                            continue

                        # Always define timestamp from part[0]
                        try:
                            timestamp = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                        except Exception as e:
                            logging.info(f"⚠️ Invalid timestamp on line: {line} → {e}")
                            errors += 1
                            continue

                        while len(parts) < 14:
                            parts.append(None)

                        try:
                            datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                            temp = float(parts[5]) if parts[5] else None
                            temp_in = float(parts[3]) if parts[3] else None
                            humidity = int(float(parts[4])) if parts[4] else None
                            humidity_in = int(float(parts[2])) if parts[2] else None
                            pressure_abs = float(parts[6]) if parts[6] else None
                            pressure_rel = None
                            rain_rate = float(parts[10]) if parts[10] else 0.0
                            rain_event = 0.0
                            rain_hourly = 0.0
                            rain_daily = 0.0
                            rain_weekly = 0.0
                            rain_monthly = 0.0
                            rain_yearly = 0.0
                            wind_degree = float(parts[9]) if parts[9] else None
                            wind_gust = float(parts[8]) if parts[8] else None
                            wind_gust_maxdaily = 0.0
                            wind_speed = float(parts[7]) if parts[7] else None
                            solarradiation = float(parts[12]) if parts[12] else None
                            uv = int(float(parts[13])) if parts[13] else None

                            data = (
                                timestamp, temp, temp_in, humidity, humidity_in,
                                pressure_abs, pressure_rel, rain_rate, rain_event,
                                rain_hourly, rain_daily, rain_weekly, rain_monthly, rain_yearly,
                                wind_degree, wind_gust, wind_gust_maxdaily, wind_speed,
                                solarradiation, uv
                            )

                            batch.append(data)

                            if len(batch) >= 1000:
                                cursor.executemany(insert_query, batch)
                                row_count += len(batch)
                                batch.clear()

                        except Exception as e:
                            logging.info(f"❌ Error parsing line: {line} → {e}")
                            errors += 1

                if batch:
                    cursor.executemany(insert_query, batch)
                    row_count += len(batch)
                    batch.clear()

    conn.commit()

    # Reset IDs after import
    reset_ids_in_order(conn)
    
    cursor.close()
    conn.close()

    logging.info(f"✅ Import done. Files: {total_files}, Rows: {row_count}, Skipped: {skipped}, Errors: {errors}")

@app.before_first_request
def setup():
    """Setup for the Flask app before the first request."""
    get_mysql_connection()  

    logging.info("Initial import from SQLite to MySQL...")

    # Import from SQLite to MySQL if the table is missing
    import_from_sqlite_if_table_missing()

@app.teardown_appcontext
def cleanup(exception):
    """Keep this function for any per-request cleanup that doesn't include closing the MySQL connection."""
    logging.info("ℹ️ MySQL connection stays open!")

@app.route('/data/report/', methods=['POST'])
def receive_ecowitt():
    """Receive and process weather data."""

    # Lock entire operation: ensures only one request at a time processes incoming data,
    # including the import from SQLite to MySQL
    with import_lock:

        # logging.info the complete POST data for logging
        logging.info("POST received from: {}".format(request.remote_addr))

        # Get the persistent MySQL connection
        conn = get_mysql_connection()

        # Prepare the data structure
        weather_data = request.form.to_dict()
        raw_data_to_store, raw_data_to_custom, xml_data_to_store, db_data_to_store, formatted_data = process_weather_data(weather_data)

        # Example extracting the timestamp directly from the incoming data payload
        timestamp_str = weather_data.get("dateutc", None)

        # Forward the POST request to the other server
        url = HASS_URL
        try:
            response = requests.post(url, data=weather_data)
            if response.status_code == 200:
                logging.info("POST forwarded successfully to Home Assistant")
            else:
                logging.info("Failed to forward the POST request to Home Assistant")
        except Exception as e:
            logging.error("Error while forwarding POST request: {}".format(str(e)))

        # Save to SQLite database
        save_to_db(db_data_to_store, 'sqlite')

        # Save to MySQL database using the persistent connection
        save_to_db(db_data_to_store, 'mysql', conn)

        # Save to local storage
        DATA_STORE.save_data(raw_data_to_store, datatype='raw')

        if should_process_data("60sec", 1):
            logging.info("60-sec condition met. Preparing to save data...")
            save_to_xml(xml_data_to_store)
            upload_to_ftp(DATA_PATH + "/live.xml", FTP_PATH + '/live.xml')
        
        if should_process_data("5min", 5):
            logging.info("5-minute condition met. Preparing to process and upload data...")
            save_to_24h_json(formatted_data)  
            save_to_custom_json({
                "temperature": raw_data_to_custom["temperature"],
                "pressure": raw_data_to_custom["pressure"],
                "rain": raw_data_to_custom["rain"],
                "wind_gust": raw_data_to_custom["wind_gust"],
                "wind_degree": raw_data_to_custom["wind_degree"],
                "solarradiation": raw_data_to_custom["solarradiation"],
            }, timestamp_str)  

            upload_to_ftp(DATA_PATH + "/24h.json", FTP_PATH + '/24h.json')
            upload_to_ftp(DATA_PATH + "/custom.json", FTP_PATH + '/custom.json')

        if should_process_data("25min", 25):
            logging.info("25-minute condition met. Preparing to process and upload data...")
            save_to_1w_json(formatted_data)  
            upload_to_ftp(DATA_PATH + "/1w.json", FTP_PATH + '/1w.json')

        if should_process_data("50min", 50):
            logging.info("50-minute condition met. Preparing to process and upload data...")
            save_to_1m_json(formatted_data)
            save_to_1y_json(formatted_data)    
            upload_to_ftp(DATA_PATH + "/1y.json", FTP_PATH + '/1y.json')
            upload_to_ftp(DATA_PATH + "/1m.json", FTP_PATH + '/1m.json')

        if should_process_data("6hour", 360):
            logging.info("6-hour condition met. Preparing to process and upload data...")
            upload_to_ftp(DATA_PATH + '/weather_data.db', FTP_PATH + '/weather_data.db')

        logging.info("POST processing done!")

    return '', 200

if __name__ == "__main__":
    logging.info("Script is running...")

    # To run a one-time import from historical files to MySQL, uncomment:
    #import_saved_data_to_mysql()
    
    try:
        app.run(debug=True, host="0.0.0.0", port=8090, use_reloader=False)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        close_mysql_connection()
