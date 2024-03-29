from argparse import ArgumentParser
import os
import sys
os.environ['BASE_PATH'] = os.path.abspath(os.path.dirname(__file__))

import app

import logging
logger = logging.getLogger(__name__)


def parse_args(parser: ArgumentParser, ceph_config: dict):
    """

    """
    parser.add_argument(
        '--ceph',
        metavar="PATH",
        help="Path of the ceph config file",
        default=ceph_config["cluster"]["conf_file"])
    parser.add_argument(
        '--user_keyring',
        metavar="PATH",
        help="Path of the client keyring file",
        default=ceph_config["cluster"]["user_keyring"])
    parser.add_argument(
        '--user',
        help="Client name (without 'client.' prefix)",
        default=ceph_config["cluster"]["client"])
    parser.add_argument('-p', '--pool', help="Source pool name")
    parser.add_argument(
        '-i',
        '--images',
        nargs="+",
        help="List of images to backup ('*' for all)")
    parser.add_argument(
        '-d',
        '--directory',
        help="Target directory where backups will be stored")
    parser.add_argument(
        '-v',
        '--verbose',
        action="store_true",
        default=ceph_config["app"]["verbose"],
        help="Make the program verbose")
    parser.add_argument(
        '--log-file',
        default=ceph_config["app"]["log_file"],
        help="Set the logging file path")

    # GROUP FULL AND DIFF
    backup_type_group = parser.add_mutually_exclusive_group()
    backup_type_group.add_argument(
        '--full', action="store_true", help="Perform a full image backup")
    backup_type_group.add_argument(
        '--diff',
        action="store_true",
        help="Perform a incremental image backup")

    args = parser.parse_args()

    # Use the values provided by the current config file (or default if not exists)
    ceph_config["app"]["verbose"] = args.verbose
    ceph_config["app"]["log_file"] = args.log_file

    ceph_config["cluster"]["conf_file"] = args.ceph
    ceph_config["cluster"]["user_keyring"] = args.user_keyring
    ceph_config["cluster"]["user"] = args.user

    # Check for arguments if they are set, they will override the file config
    # values
    if args.full:
        ceph_config["backup"]["type"] = "full"

    if args.diff:
        ceph_config["backup"]["type"] = "diff"

    if args.pool:
        ceph_config["backup"]["pool"] = args.pool

    if args.images:
        ceph_config["backup"]["images"] = args.images

    if args.directory:
        ceph_config["backup"]["directory"] = args.directory


def main(ceph_config):

    cluster_config = ceph_config["cluster"]
    backup_config = ceph_config["backup"]

    try:
        cluster = app.ceph.Ceph(
            cluster_config["conf_file"], cluster_config["user_keyring"],
            cluster_config["client"], backup_config["pool"],
            backup_config["images"], backup_config["directory"])
    except:
        sys.exit(1)

    backup_type = backup_config["type"]
    cluster.print_overview()

    if backup_type == "full":
        cluster.full_backup()
    elif backup_type == "diff":
        cluster.full_diff_backup()
    else:
        logger.critical("Wrong backup type. Must be <full/diff>")


if __name__ == "__main__":
    parser = ArgumentParser()

    # Get config from config file
    ceph_config = app.load_config()

    # Override with the user args
    parse_args(parser, ceph_config)

    try:
        # Setup the application
        app.setup_app(ceph_config["app"])
        # Checks if config has all the required fields
        app.check_config(ceph_config)
    except:
        sys.exit(1)

    main(ceph_config)
