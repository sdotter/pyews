import os
import ftplib
import logging
from datetime import datetime
from globals import FTP_HOST, FTP_USER, FTP_PASS, DATA_PATH

def upload_to_ftp(filename, remote_path):
    """Upload a file to an FTP server."""
    ftp = None
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)

        local_path = os.path.join(DATA_PATH, filename)
        remote_path = remote_path.lstrip('/')  # Zorgt ervoor dat er geen voorloop slashes zijn

        with open(local_path, 'rb') as file:
            ftp.storbinary('STOR {}'.format(remote_path), file)  # Upload naar de FTP
            logging.info("Uploaded {} successfully...".format(filename.split('/')[-1]))
    except ftplib.all_errors as e:
        logging.error("FTP operation failed with error: {}".format(e))
    except FileNotFoundError:
        logging.error("File not found: {}".format(local_path))
    except Exception as e:
        logging.error("An unexpected error occurred: {}".format(e))
    finally:
        if ftp:
            ftp.quit()