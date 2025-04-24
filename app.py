
import os

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

def import_from_sqlite_if_table_missing():
    """Check MySQL table and import from SQLite if table doesn't exist (over SSH tunnel)."""
    
    ssh = get_ssh_tunnel()

    mysql_connection = pymysql.connect(
        host='127.0.0.1',
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password'],
        db=MYSQL_CONFIG['database'],
        port=ssh.local_bind_port
    )

    try:
        if not table_exists(mysql_connection):
            logging.info("Importing data from SQLite to MySQL!")
            import_sqlite_to_mysql(mysql_connection)
    finally:
        mysql_connection.close()

@app.route('/data/report/', methods=['POST'])
def receive_ecowitt():
    """Receive and process weather data."""

    # Lock entire operation: ensures only one request at a time processes incoming data,
    # including the import from SQLite to MySQL
    with import_lock:

        # logging.info the complete POST data for logging
        logging.info("POST received from: {}".format(request.remote_addr))

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

        # Import from SQLite to MySQL if the table is missing
        import_from_sqlite_if_table_missing()

        # Save to SQLite database
        save_to_db(db_data_to_store, 'sqlite')

        # Save to MySQL database
        save_to_db(db_data_to_store, 'mysql')

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
    app.run(debug=True, host="0.0.0.0", port=8090, use_reloader=False)