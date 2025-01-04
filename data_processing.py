import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from utils.conversions import degrees_to_wind_direction, f_to_c, feels_like, get_dew_point_c, inHg_to_hPa, mph_to_kph, wind_chill
from globals import *

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
        logging.info("Data successfully saved to 24h.json")

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
        logging.info("Data successfully saved to 1w.json")

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
        logging.info("Data successfully saved to 1m.json")

def save_to_custom_json(weather_data, timestamp_str):
    current_time = datetime.now(TIMEZONE)

    # Initialize final data structure
    final_data = {
        "solarradiation": {
            "id": "solarradiation",
            "name": "Zonnestraling",
            "data": [],
            "index": 5,
            "unit": " W/m\u00b2"
        },
        "rain": {
            "id": "rain",
            "name": "Neerslag",
            "data": [],
            "index": 2,
            "unit": " mm"
        },
        "pressure": {
            "id": "pressure",
            "name": "Luchtdruk",
            "data": [],
            "index": 1,
            "unit": " hPa"
        },
        "temperature": {
            "id": "temperature",
            "name": "Temperatuur",
            "data": [],
            "index": 0,
            "unit": "\u00b0C"
        },
        "wind_gust": {
            "id": "wind_gust",
            "name": "Windvlaag",
            "data": [],
            "index": 3,
            "unit": " m/s"
        },
        "wind_degree": {
            "id": "wind_degree",
            "name": "Windrichting",
            "data": [],
            "index": 4,
            "unit": "\u00b0"
        }
    }

    # Load existing data from JSON file
    try:
        with open(DATA_PATH + "/custom.json", 'r') as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = list(final_data.values())

    # Calculate cutoff time for 24 hours ago
    cutoff_time = current_time - timedelta(hours=24)

    # Process existing data
    for metric in existing_data:
        if "id" in metric and "data" in metric:
            for timestamp_ms, value in metric["data"]:
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, TIMEZONE)
                if timestamp >= cutoff_time:
                    populate_final_data(final_data, timestamp_ms, {metric["id"]: value})

    try:
        # Correct the format string to match your timestamp
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TIMEZONE)
        if timestamp >= cutoff_time:
            timestamp_ms = int(timestamp.timestamp() * 1000)
            populate_final_data(final_data, timestamp_ms, weather_data)
    except ValueError:
        logging.error("Invalid timestamp format in new record: {}".format(timestamp_str))

    result_data = list(final_data.values())

    # Write back to the JSON file
    with open(DATA_PATH + "/custom.json", 'w') as f:
        json.dump(result_data, f, indent=4, ensure_ascii=False)

    logging.info("Data successfully saved to custom.json")


def populate_final_data(final_data, timestamp_ms, measurements):
    """ Helper function to populate the final_data with timestamped measurements """
    for key, value in measurements.items():
        if key in final_data:
            if key == "wind_gust":
                try:
                    value = float(value)  # Convert string to float
                    wind_gust_mps = value * 1000 / 3600
                    final_data[key]["data"].append([timestamp_ms, round(wind_gust_mps, 2)])
                except ValueError:
                    logging.error("Invalid value for wind_gust: {}".format(value))
            else:
                try:
                    value = float(value)  # Convert string to float for other measurements if necessary
                    final_data[key]["data"].append([timestamp_ms, value])
                except ValueError:
                    logging.error("Invalid value for {}: {}".format(key, value))

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
    logging.info("Data successfully saved to live.xml")

def should_process_data(interval_key, minutes):
    global LAST_SAVE_TIMES
    current_time = datetime.now(TIMEZONE)
    if current_time - LAST_SAVE_TIMES[interval_key] >= timedelta(minutes=minutes):
        LAST_SAVE_TIMES[interval_key] = current_time 
        return True
    return False

def process_weather_data(weather_data):
    """Process and normalize weather data."""
    formatted_datetime = datetime.now(TIMEZONE).strftime('%m/%d/%Y %H:%M')
    formatted_data = {formatted_datetime: {}}
    
    fields = [
        ('baromabsin', 'AbsPressure'),
        ('dewpoint', 'DewPoint'),
        ('rainratein', 'Rain'),
        ('feelsLike', 'FeelsLike'),
        ('humidityin', 'HumidityIn'),
        ('humidity', 'HumidityOut'),
        ('solarradiation', 'SolarRadiation'),
        ('tempinf', 'TempIn'),
        ('tempf', 'TempOut'),
        ('winddir', 'WindDirection'),
        ('windchillf', 'WindChill'),
        ('windgustmph', 'WindGust'),
        ('windspeedmph', 'WindAvg'),
    ]

    for field, json_key in fields:
        formatted_data[formatted_datetime][json_key] = None

    db_data_to_store = {
        'timestamp': weather_data["dateutc"],
        'temp': None,
        'temp_in': None,
        'humidity': None,
        'humidity_in': None,
        'pressure_abs': None,
        'pressure_rel': None,
        'rain_rate': None,
        'rain_event': None,
        'rain_hourly': None,
        'rain_daily': None,
        'rain_weekly': None,
        'rain_monthly': None,
        'rain_yearly': None,
        'wind_degree': None,
        'wind_gust': None,
        'wind_gust_maxdaily': None,
        'wind_speed': None,
        'solarradiation': None,
        'uv': None
    }

    xml_data_to_store = {
        "hum_in": None,
        "temp_in": None,
        "hum_out": None,
        "temp_out": None,
        "abs_pressure": None,
        "wind_ave": None,
        "wind_gust":None,
        "wind_dir": None,
        "rain": None,
    }

    raw_data_to_store = {
        "idx": datetime.strptime(weather_data["dateutc"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "delay": int(weather_data["interval"]) // 60,
        "hum_in": None,
        "temp_in": None,
        "hum_out": None,
        "temp_out": None,
        "abs_pressure": None,
        "wind_ave": None,
        "wind_gust":None,
        "wind_dir": None,
        "rain": None,
        "status": 0,
        "illuminance": None,
        "uv": None
    }
    
    for field, json_key in fields:
        if field in weather_data:
            value = float(weather_data[field])
                
            if field in ['baromabsin']:
                value = round(inHg_to_hPa(value), 1)

            if field in ['tempinf', 'tempf']:
                value = round(f_to_c(value), 1)

            if "windspeedmph" in field or "windgustmph" in field:
                value = int(round(mph_to_kph(value)))

            if field in ['dailyrainin']:
                value = round(value * 25.4, 1)  # Convert inches to mm

            if field in ['humidity'] or "humidityin" in field:
                value = int(round(value))

            if field in ['winddir']:
                value = degrees_to_wind_direction(value)

            if field in ['solarradiation']:
                value = round(value, 1)

            formatted_data[formatted_datetime][json_key] = value

    # Data object for stroing in the database
    db_data_to_store['temp'] = formatted_data[formatted_datetime].get('TempOut')
    db_data_to_store['temp_in'] = formatted_data[formatted_datetime].get('TempIn')
    db_data_to_store['humidity'] = formatted_data[formatted_datetime].get('HumidityOut')
    db_data_to_store['humidity_in'] = formatted_data[formatted_datetime].get('HumidityIn')
    db_data_to_store['pressure_abs'] = float(weather_data.get('baromabsin', None))
    db_data_to_store['pressure_rel'] = float(weather_data.get('baromrelin', None))
    db_data_to_store['rain_rate'] = float(weather_data.get('rainratein', None))
    db_data_to_store['rain_event'] = float(weather_data.get('eventrainin', None))
    db_data_to_store['rain_hourly'] = float(weather_data.get('hourlyrainin', None))
    db_data_to_store['rain_daily'] = float(weather_data.get('dailyrainin', None))
    db_data_to_store['rain_weekly'] = float(weather_data.get('weeklyrainin', None))
    db_data_to_store['rain_monthly']  = float(weather_data.get('monthlyrainin', None))
    db_data_to_store['rain_yearly'] = float(weather_data.get('yearlyrainin', None))
    db_data_to_store['wind_degree'] = formatted_data[formatted_datetime].get('WindDirection')
    db_data_to_store['wind_gust'] = formatted_data[formatted_datetime].get('WindGust')
    db_data_to_store['wind_gust_maxdaily'] = float(weather_data.get('maxdailygust', None))
    db_data_to_store['wind_speed'] = float(weather_data.get('windspeedmph', None))
    db_data_to_store['solarradiation'] = formatted_data[formatted_datetime].get('SolarRadiation')
    db_data_to_store['uv'] = int(weather_data.get('uv', None))

    # XML data object
    xml_data_to_store['hum_in'] =  formatted_data[formatted_datetime]['HumidityIn']
    xml_data_to_store['temp_in'] =  formatted_data[formatted_datetime]['TempIn']
    xml_data_to_store['hum_out'] =  formatted_data[formatted_datetime]['HumidityOut']
    xml_data_to_store['temp_out'] =  formatted_data[formatted_datetime]['TempOut']
    xml_data_to_store['abs_pressure'] =  formatted_data[formatted_datetime]['AbsPressure']
    xml_data_to_store['wind_ave'] =  formatted_data[formatted_datetime]['WindAvg']
    xml_data_to_store['wind_gust'] =  formatted_data[formatted_datetime]['WindGust']
    xml_data_to_store['wind_dir'] =  formatted_data[formatted_datetime]['WindDirection']
    xml_data_to_store['rain'] =  formatted_data[formatted_datetime]['Rain']

    # Raw data object
    raw_data_to_store['hum_in'] = weather_data["humidityin"]
    raw_data_to_store['temp_in'] = weather_data["tempinf"]
    raw_data_to_store['hum_out'] = weather_data["humidity"]
    raw_data_to_store['temp_out'] = weather_data["tempf"]
    raw_data_to_store['abs_pressure'] = float(weather_data["baromabsin"])
    raw_data_to_store['wind_ave'] = weather_data["windspeedmph"]
    raw_data_to_store['wind_gust'] = weather_data["windgustmph"]
    raw_data_to_store['wind_dir'] = weather_data["winddir"]
    raw_data_to_store['rain'] = float(weather_data["rainratein"])
    raw_data_to_store['illuminance'] = weather_data["solarradiation"]
    raw_data_to_store['uv'] = weather_data["uv"]

    return raw_data_to_store, xml_data_to_store, db_data_to_store, formatted_data