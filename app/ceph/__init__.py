import rados
import rbd

from pathlib import Path
import yaml

import logging
logger = logging.getLogger(__name__)

from . import ceph
# from . import ceph_backup

CONFIG_DIR = Path("etc")
DEFAULT_CONFIG_FILE = CONFIG_DIR.joinpath("ceph_default.yaml")
CONFIG_FILE = CONFIG_DIR.joinpath("ceph.yaml")

AVAILABLES_BACKUP_TYPES = ["full", "diff"]

default_config = {
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