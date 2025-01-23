
import os

import requests
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request
from utils.logging import logging, configure_logging
from data_processing import process_weather_data, should_process_data, save_to_24h_json, save_to_1w_json, save_to_1m_json, save_to_1y_json, save_to_custom_json, save_to_xml
from utils.ftp import upload_to_ftp
from database import save_to_db
from globals import *

app = Flask(__name__)

load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env'))

# Configure logging
configure_logging()

@app.route('/data/report/', methods=['POST'])
def receive_ecowitt():
    """Receive and process weather data."""
    # Check the time
    current_time = datetime.now(TIMEZONE)

    # logging.info the complete POST data for logging
    logging.info("POST received from: {}".format(request.remote_addr))

    # Prepare the data structure
    weather_data = request.form.to_dict()
    raw_data_to_store, xml_data_to_store, db_data_to_store, formatted_data = process_weather_data(weather_data)

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
    save_to_db(db_data_to_store)

    # Save to local storage
    DATA_STORE.save_data(raw_data_to_store, datatype='raw')

    # Save to live realtime XML
    save_to_xml(xml_data_to_store)
    upload_to_ftp(DATA_PATH + "/live.xml", FTP_PATH + '/live.xml')
    
    if should_process_data("5min", 5):
        logging.info("5-minute condition met. Preparing to save data...")
        save_to_24h_json(formatted_data)  
        save_to_custom_json({
            "temperature": raw_data_to_store["temp_out"],
            "pressure": raw_data_to_store["abs_pressure"],
            "rain": raw_data_to_store["rain_rate"],
            "wind_gust": raw_data_to_store["wind_gust"],
            "wind_degree": raw_data_to_store["wind_dir"],
            "solarradiation": raw_data_to_store["illuminance"],
        }, timestamp_str)  
        upload_to_ftp(DATA_PATH + "/24h.json", FTP_PATH + '/24h.json')
        upload_to_ftp(DATA_PATH + "/custom.json", FTP_PATH + '/custom.json')

    if should_process_data("25min", 25):
        logging.info("25-minute condition met. Preparing to save data...")
        save_to_1w_json(formatted_data)  
        upload_to_ftp(DATA_PATH + "/1w.json", FTP_PATH + '/1w.json')

    if should_process_data("50min", 50):
        logging.info("50-minute condition met. Preparing to save data...")
        save_to_1m_json(formatted_data)
        save_to_1y_json(formatted_data)    
        upload_to_ftp(DATA_PATH + "/1y.json", FTP_PATH + '/1y.json')
        upload_to_ftp(DATA_PATH + "/1m.json", FTP_PATH + '/1m.json')

    if should_process_data("60sec", 1):
        logging.info("60-sec condition met. Preparing to save data...")


    logging.info("POST processing done!")

    return '', 200

if __name__ == "__main__":
    logging.info("Script is running...")
    app.run(debug=True, host="0.0.0.0", port=8090, use_reloader=False)