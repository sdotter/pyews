import os
import logging
import warnings
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler, WatchedFileHandler
from globals import BASE_DIR

class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

def configure_logging(): 
    # Set log level
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Configure file handler
    file_handler = RotatingFileHandler(BASE_DIR + '/pyews.log', maxBytes=1 * 1024 * 1024, backupCount=5)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Configure stream handler to output to stdout
    stream_handler = logging.StreamHandler() 
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    # Configure logging for paramiko to show only warnings and errors
    paramiko_logger = logging.getLogger("paramiko")
    paramiko_logger.setLevel(logging.WARNING)

    # Filter warnings in pymysql-cursor
    warnings.filterwarnings("ignore", category=Warning, module='pymysql.cursors')

    # Add both handlers to the logger
    # logging.basicConfig(level=logging.INFO, handlers=[FlushHandler(), file_handler, stream_handler])
    logging.basicConfig(level=logging.INFO, handlers=[FlushHandler(), file_handler])

    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)