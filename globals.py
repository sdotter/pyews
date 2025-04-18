import os
import sys
import pytz
from datetime import datetime
from dotenv import load_dotenv
from store import CustomWeatherStore

currentdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(currentdir, "pywws/src"))

TIMEZONE = pytz.timezone('Europe/Amsterdam')
BASE_DIR = currentdir
DATA_PATH = BASE_DIR + "/data"
LAST_SAVE_TIMES = {
    "5min": datetime.min.replace(tzinfo=TIMEZONE),
    "25min": datetime.min.replace(tzinfo=TIMEZONE),
    "50min": datetime.min.replace(tzinfo=TIMEZONE),
    "60sec": datetime.min.replace(tzinfo=TIMEZONE),
}

# Load environment variables from .env file
load_dotenv(BASE_DIR + '/.env')

# FTP server gegevens
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')
FTP_PATH = os.getenv('FTP_PATH')
HASS_URL = os.getenv('HASS_URL')

DATA_STORE = CustomWeatherStore(DATA_PATH)

# Database configuratie voor MySQL
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': 3306
}

# MySQL database only accessible via SSH tunnel
SSH_CONFIG = {
    'ssh_host': os.getenv('SSH_HOST'),
    'ssh_port': os.getenv('SSH_PORT'),
    'ssh_username': os.getenv('SSH_USER'),
    'ssh_password': os.getenv('SSH_PASSWORD'),
}