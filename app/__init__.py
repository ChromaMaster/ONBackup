import os
from pathlib import Path
import yaml

from .ceph import ceph
from util import color

import logging

logger = logging.getLogger(__name__)


CONFIG_DIR = Path("etc")
DEFAULT_CONFIG_FILE = CONFIG_DIR.joinpath("ceph_default.yaml")
CONFIG_FILE = CONFIG_DIR.joinpath("ceph.yaml")

AVAILABLES_BACKUP_TYPES = ["full", "diff"]

default_config = {
    "app":{
        "verbose": False,
        "log_file": "onbackup.log"
    },
    "cluster": {
        "conf_file": "etc/ceph/ceph.conf",
        "user_keyring": "etc/ceph/ceph.client.onebackup.keyring",
        "client": "onebackup"
    },
    "backup": {
        "type": "full",
        "pool": "",
        "directory": "",
        "images": ["*"]
    }
}

def load_config():
    # If config file does not exists create with the default config
    # and return the default configuration and
    if not CONFIG_FILE.exists():
        logger.info("Ceph config file does not exist...")
        with open(DEFAULT_CONFIG_FILE, "w+") as f:
            logger.info(
                "Creating default ceph config file [ceph_default.yaml]...")
            yaml.dump(default_config, f)
        return default_config

    # Read configuration from config file
    logger.info("Loading ceph config...")
    with open(CONFIG_FILE, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    return config

def check_config(config):   
    logger.info("Checking the app config...")
    app_config = config["app"]
    if not app_config["verbose"]:
        # logger.warning("Logging level not set")
        app_config["verbose"] = False

    logger.info("Checking the ceph config...")
    # Checks if there are blank values into the config
    backup_config = config["backup"]
    if not backup_config["pool"].strip():
        logger.critical("Backup pool not set")
        raise
    if not backup_config["directory"].strip():
        logger.critical("Backup directory not set")
        raise
    if backup_config["type"] not in AVAILABLES_BACKUP_TYPES:
        logger.critical(f"Backup type \"{backup_config['type']}\" not allowed, please use {AVAILABLES_BACKUP_TYPES}")
        raise    
    if not Path(backup_config["directory"]).exists():
        logger.critical(f"Backup directory \"{backup_config['directory']}\" does not exist")
        raise

def setup_app(app_config):
    # Sets the global logger as info
    logger.setLevel(logging.INFO)

    if app_config["verbose"]:
        # Console log
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        c_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)

    # File log
    try:
        f_handler = logging.FileHandler(app_config["log_file"], "a")
    except PermissionError:
        logger.critical(f"Cannot create log \"{app_config['log_file']}\" file, permission denied")
        raise
    
    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter(
        'F_HANDLER - %(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    # File log for pandora monitoring. Different because it will be erased
    # all the time
    pandora_handler = logging.FileHandler(
        "app/monitoring/pandorafms/pandora_data.log", "w")
    pandora_handler.setLevel(logging.INFO)
    pandora_format = logging.Formatter('%(asctime)s - %(message)s')
    pandora_handler.setFormatter(pandora_format)
    logger.addHandler(pandora_handler)
