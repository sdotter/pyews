from sshtunnel import SSHTunnelForwarder
from globals import MYSQL_CONFIG, SSH_CONFIG, SSH_TUNNEL
import logging

def get_ssh_tunnel():
    """Start or return existing SSH tunnel (singleton)."""
    
    global SSH_TUNNEL

    if SSH_TUNNEL is None or not SSH_TUNNEL.is_active:
        logging.info("Starting SSH tunnel...")

        ssh = SSHTunnelForwarder(
            (SSH_CONFIG['ssh_host'], int(SSH_CONFIG['ssh_port'])),
            ssh_username=SSH_CONFIG['ssh_username'],
            ssh_password=SSH_CONFIG['ssh_password'],
            remote_bind_address=(MYSQL_CONFIG['host'], int(MYSQL_CONFIG['port'])),
            local_bind_address=('127.0.0.1', 3306)
        )
        ssh.start()
        SSH_TUNNEL = ssh

        logging.info("SSH tunnel established on localhost:%d", ssh.local_bind_port)
    else:
        logging.debug("SSH tunnel already active")

    return SSH_TUNNEL