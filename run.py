#!/usr/bin/env python3

import os
import sys
import json
import logging
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from flask import Flask, request, Response
from datetime import datetime, timedelta
from logging.handlers import WatchedFileHandler
import time
import ftplib
import pytz
from store import CustomWeatherStore

currentdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(currentdir, "pywws/src"))

from pywws.conversions import degrees_to_wind_direction, f_to_c, feels_like, get_dew_point_c, inHg_to_hPa, mph_to_kph, wind_chill, wm2_illuminance, inches_to_mm

app = Flask(__name__)

class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

# Configure the path where the JSON data will be saved
TIMEZONE = pytz.timezone('Europe/Amsterdam')
BASE_DIR = currentdir
DATA_PATH = BASE_DIR + "/data"
LAST_SAVE_TIMES = {
    "5min": datetime.min.replace(tzinfo=TIMEZONE),
    "25min": datetime.min.replace(tzinfo=TIMEZONE),
    "50min": datetime.min.replace(tzinfo=TIMEZONE),
}

# Load environment variables from .env file
load_dotenv(BASE_DIR + '/.env')

# FTP server gegevens
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')
FTP_PATH = os.getenv('FTP_PATH')

DATA_STORE = CustomWeatherStore(DATA_PATH)

# Set log level
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Configure file handler
file_handler = WatchedFileHandler(BASE_DIR + '/pyews.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Configure stream handler to output to stdout
stream_handler = logging.StreamHandler() 
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

# Add both handlers to the logger
# logging.basicConfig(level=logging.INFO, handlers=[FlushHandler(), file_handler, stream_handler])
logging.basicConfig(level=logging.INFO, handlers=[FlushHandler(), file_handler])

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

def save_to_json(data):
    ''' Save the provided data to a JSON file.
    '''

    # Attempt to load any existing data in the file
    try:
        with open(DATA_PATH + "/data.json", "r") as f:
            file_data = json.load(f)
    except Exception:
        file_data = {"data": []}

    # Append the new data
    file_data["data"].append(data)

    # Write the updated data back to the file
    with open(DATA_PATH + "/data.json", "w") as f:
        json.dump(file_data, f, indent=4)

def save_to_24h_json(data):
    ''' Save the provided data to the 24h.json file, ensuring only the last 24 hours of data is retained. '''

    current_time = datetime.now(TIMEZONE)

    # Read the existing data
    try:
        with open(DATA_PATH + "/24h.json", 'r') as f:
            file_data = json.load(f)
            existing_data = file_data["data"]
    except Exception:
        existing_data = []

    # Filter records to keep only those from the last 24 hours
    twenty_four_hours_ago = current_time - timedelta(hours=24)
    new_data = []
    for record in existing_data:
        for timestamp_str, _ in record.items():
            timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M").replace(tzinfo=TIMEZONE)
            if timestamp > twenty_four_hours_ago:
                new_data.append(record)

    # Add the new data
    formatted_datetime = current_time.strftime('%m/%d/%Y %H:%M')
    new_data.append(data)

    # Save the updated data back to the file
    with open(DATA_PATH + "/24h.json", 'w') as f:
        json.dump({"data": new_data}, f, indent=4)
        logging.info("{} - Data successfully saved to 24h.json.".format(current_time))

def save_to_1w_json(data):
    ''' Save the provided data to the 1w.json file, appending with max 1 week of data. '''

    current_time = datetime.now(TIMEZONE)

    # Read the existing data
    try:
        with open(DATA_PATH + "/1w.json", 'r') as f:
            file_data = json.load(f)
            existing_data = file_data["data"]
    except Exception:
        existing_data = []

    # Filter records older than 1 week
    one_week_ago = current_time - timedelta(days=7)
    new_data = []
    for record in existing_data:
        for timestamp_str, _ in record.items():
            timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M").replace(tzinfo=TIMEZONE)
            if timestamp > one_week_ago:
                new_data.append(record)
    
    # Add the new data
    formatted_datetime = current_time.strftime('%m/%d/%Y %H:%M')
    new_data.append(data)
    
    # Save the updated data back to the file
    with open(DATA_PATH + "/1w.json", 'w') as f:
        json.dump({"data": new_data}, f, indent=4)
        logging.info("{} - Data successfully saved to 1w.json.".format(current_time))

def save_to_1m_json(data):
    ''' Save the provided data to the 1m.json file, appending with max 1 month of data. '''

    current_time = datetime.now(TIMEZONE)
       
    # Attempt to read the existing data
    try:
        with open(DATA_PATH + "/1m.json", 'r') as f:
            file_data = json.load(f)
            existing_data = file_data.get("data", [])
    except Exception:
        existing_data = []

    # Filtering function to remove records older than 1 month
    one_month_ago = current_time - timedelta(days=30)  # Rough approximation of one month

    new_data = []
    for record in existing_data:
        for timestamp_str, _ in record.items():
            timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M").replace(tzinfo=TIMEZONE)
            if timestamp > one_month_ago:
                new_data.append(record)

    # Add the new data
    formatted_datetime = current_time.strftime('%m/%d/%Y %H:%M')
    new_data.append(data)
    
    # Save the updated data back to the file
    with open(DATA_PATH + "/1m.json", 'w') as f:
        json.dump({"data": new_data}, f, indent=4)
        logging.info("{} - Data successfully saved to 1m.json.".format(current_time))
    
def save_to_xml(data):
    '''Save the provided data to an XML file.'''
    root = ET.Element("meteo")

    now = datetime.now(TIMEZONE)
    timestamp = ET.SubElement(root, "timestamp")
    timestamp.text = str(int(time.mktime(now.timetuple())))  

    # Assuming `data` is a dictionary containing the weather data
    for key, value in data.items():
        element = ET.SubElement(root, key)
        element.text = str(value)

    tree = ET.ElementTree(root)
    tree.write(DATA_PATH + "/live.xml", encoding='utf-8', xml_declaration=True)
    logging.info("{} - Data successfully saved to live.xml.".format(datetime.now(TIMEZONE)))


def should_process_data(interval_key, minutes):
    global LAST_SAVE_TIMES
    current_time = datetime.now(TIMEZONE)
    if current_time - LAST_SAVE_TIMES[interval_key] >= timedelta(minutes=minutes):
        LAST_SAVE_TIMES[interval_key] = current_time 
        return True
    return False
    
def upload_to_ftp(filename, remote_path):
    """
    Helper function to upload a file to an FTP server.
    """
    ftp = None
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
        with open(filename, 'rb') as file:
            ftp.storbinary('STOR {}'.format(remote_path), file)
            logging.info("{} -  > Uploaded {} successfully...".format(datetime.now(TIMEZONE), filename.split('/')[-1]))
    except Exception as e:
        logging.info("{} - FTP operation failed with error: {}".format(datetime.now(TIMEZONE), e))
    finally:
        if ftp:
            ftp.quit()
    
@app.route('/data/report/', methods=['POST'])
def receiveEcoWitt():
    ''' Receive a post in Ecowitt protocol format and process it into JSON format '''
    
    # Check the time
    current_time = datetime.now(TIMEZONE)

    # logging.info the complete POST data for logging
    logging.info("{} - POST received from: {}".format(datetime.now(TIMEZONE), request.remote_addr))
    
    # Prepare the data structure
    weather_data = request.form.to_dict()
    formatted_datetime = datetime.now(TIMEZONE).strftime('%m/%d/%Y %H:%M')
    formatted_data = {
        formatted_datetime: {}
    }

    # print(weather_data)
    # {
    #     "monthlyrainin":"1.402",
    #     "PASSKEY":"",
    #     "stationtype":"",
    #     "winddir":"227",
    #     "model":"",
    #     "freq":"868M",
    #     "windgustmph":"11.41",
    #     "tempinf":"63.5",
    #     "baromrelin":"30.233",
    #     "maxdailygust":"13.65",
    #     "wh65batt":"0",
    #     "yearlyrainin":"1.402",
    #     "wh25batt":"0",
    #     "dateutc":"2024-12-24 14:01:20",
    #     "weeklyrainin":"0.539",
    #     "dailyrainin":"0.028",
    #     "tempf":"43.5",
    #     "uv":"0",
    #     "interval":"60",
    #     "solarradiation":"23.06",
    #     "windspeedmph":"4.70",
    #     "hourlyrainin":"0.008",
    #     "eventrainin":"0.299",
    #     "heap":"",
    #     "humidityin":"54",
    #     "baromabsin":"30.233",
    #     "rainratein":"0.000",
    #     "humidity":"97",
    #     "runtime":""
    # }

    # List of fields we are interested in (for brevity some are excluded)
    fields = [
        ('baromabsin', 'AbsPressure'),
        ('dewpoint', 'DewPoint'),
        ('dailyrainin', 'Rain'),
        ('feelsLike', 'FeelsLike'),
        ('humidityin', 'HumidityIn'),
        ('humidity', 'HumidityOut'),
        ('tempinf', 'TempIn'),
        ('tempf', 'TempOut'),
        ('winddir', 'WindDirection'),
        ('windchillf', 'WindChill'),
        ('windgustmph', 'WindGust'),
        ('windspeedmph', 'WindAvg'),
    ]
        
    # Initialize all fields to None by default
    for field, json_key in fields:
        formatted_data[formatted_datetime][json_key] = None

    for field, json_key in fields:
        if field in weather_data:
            value = float(weather_data[field])
                
            if field in ['baromabsin']:
                value = round(inHg_to_hPa(value), 1)
                
            if field in ['tempinf', 'tempf']:
                value = round(f_to_c(value), 1)
                json_key = json_key.replace("f", "c")
                
            if "windspeedmph" in field or "windgustmph" in field:
                value = int(round(mph_to_kph(value)))
                json_key = json_key.replace("mph", "kph") 
                
            if field in ['dailyrainin']:
                value = round(inches_to_mm(value), 1)
                
            if field in ['humidity'] or "humidityin" in field:
                value = int(round(value))
                
            if field in ['winddir']:
                value = degrees_to_wind_direction(value)
                
            formatted_data[formatted_datetime][json_key] = value
            
    # Wind Chill Calculation: Requires Temperature in C and Wind Speed in KPH
    temp_out_c = formatted_data[formatted_datetime]['TempOut']
    wind_avg_kph = formatted_data[formatted_datetime]['WindAvg']
    humidity_out = formatted_data[formatted_datetime]['HumidityOut']
    humidity_in = formatted_data[formatted_datetime]['HumidityIn']
    
    # Dew Point Calculation: Requires Temperature in C and Humidity
    formatted_data[formatted_datetime]['DewPoint'] = (
        int(round(get_dew_point_c(temp_out_c, humidity_out)))
        if temp_out_c is not None and humidity_out is not None
        else None
    )
    
    formatted_data[formatted_datetime]['WindChill'] = (
        int(round(wind_chill(temp_out_c, wind_avg_kph)))
        if temp_out_c is not None and wind_avg_kph is not None and temp_out_c <= 10 and wind_avg_kph > 4.8
        else None
    )
    
    # 'FeelsLike' can be calculated from temperature, humidity, and wind speed
    formatted_data[formatted_datetime]['FeelsLike'] = (
        int(round(feels_like(temp_out_c, humidity_out, wind_avg_kph)))
        if temp_out_c is not None and humidity_out is not None and wind_avg_kph is not None
        else None
    )

    weather_data_xml_format = {
        "hum_in": formatted_data[formatted_datetime]["HumidityIn"],
        "temp_in":formatted_data[formatted_datetime]['TempIn'],
        "hum_out": formatted_data[formatted_datetime]['HumidityOut'],
        "temp_out": formatted_data[formatted_datetime]['TempOut'],
        "abs_pressure": formatted_data[formatted_datetime]['AbsPressure'],
        "wind_ave": formatted_data[formatted_datetime]['WindAvg'],
        "wind_gust":formatted_data[formatted_datetime]['WindGust'],
        "wind_dir": formatted_data[formatted_datetime]['WindDirection'],
        "rain": formatted_data[formatted_datetime]['Rain'],
    }

    raw_data = {
        "idx": datetime.strptime(
            weather_data["dateutc"], "%Y-%m-%d %H:%M:%S"
        ).strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "delay": weather_data["interval"],
        "hum_in": weather_data["humidityin"],
        "temp_in": weather_data["tempinf"],
        "hum_out": weather_data["humidity"],
        "temp_out": weather_data["tempf"],
        "abs_pressure": float(weather_data["baromabsin"]),
        "wind_ave": weather_data["windspeedmph"],
        "wind_gust": weather_data["windgustmph"],
        "wind_dir": weather_data["winddir"],
        "rain": float(weather_data["dailyrainin"]),
        "status": 0.0,
        "illuminance": wm2_illuminance(weather_data["solarradiation"]),
        "uv": weather_data["uv"],
    }

    DATA_STORE.save_data(raw_data, datatype='raw')

    # Save to live realtime XML
    save_to_xml(weather_data_xml_format)
    upload_to_ftp(DATA_PATH + "/live.xml", FTP_PATH + '/live.xml')
   
    if should_process_data("5min", 5):
        logging.info("{} - 5-minute condition met. Preparing to save data...".format(datetime.now(TIMEZONE)))
        save_to_24h_json(formatted_data)  
        upload_to_ftp(DATA_PATH + "/24h.json", FTP_PATH + '/24h.json')

    if should_process_data("25min", 25):
        logging.info("{} - 25-minute condition met. Preparing to save data...".format(datetime.now(TIMEZONE)))
        save_to_1w_json(formatted_data)  
        upload_to_ftp(DATA_PATH + "/1w.json", FTP_PATH + '/1w.json')

          
    if should_process_data("50min", 50):
        logging.info("{} - 50-minute condition met. Preparing to save data...".format(datetime.now(TIMEZONE)))
        save_to_1m_json(formatted_data)  
        upload_to_ftp(DATA_PATH + "/1m.json", FTP_PATH + '/1m.json')
        
    logging.info("{} - POST processing done!".format(datetime.now(TIMEZONE)))

    return '', 200

if __name__ == "__main__":
    logging.info("{} - Script is running...".format(datetime.now(TIMEZONE)))
    app.run(debug=True,host="0.0.0.0", port=8090,use_reloader=False)
