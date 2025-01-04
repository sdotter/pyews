import ftplib
import logging
from datetime import datetime
from globals import FTP_HOST, FTP_USER, FTP_PASS

def upload_to_ftp(filename, remote_path):
    """Upload a file to an FTP server."""
    ftp = None
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
        with open(filename, 'rb') as file:
            ftp.storbinary('STOR {}'.format(remote_path), file)
            logging.info("Uploaded {} successfully...".format(filename.split('/')[-1]))
    except Exception as e:
        logging.info("FTP operation failed with error: {}".format(e))
    finally:
        if ftp:
            ftp.quit()