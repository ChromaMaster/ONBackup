import logging
logger = logging.getLogger(__name__)

# from app.ceph import ceph
from app.ceph import rados
from app.ceph import rbd

from util.color import Color

import time
import datetime
import subprocess
from pathlib import Path


class Ceph():
    """
    
    """

    def __init__(self, conf_file: str, user_keyring: str, client: str, pool: str, 
                images: list, backup_dir: str):
        # super(CephBackup, self).__init__(conf_file, user_keyring, client)

        # Cluster parameters
        self.user_keyring = user_keyring
        self._handler = rados.Rados(
            conffile=conf_file, conf=dict(keyring=user_keyring), name=client)

        logger.info("\nlibrados version: {}".format(str(self._handler.version())))

        # Connect to the cluster
        self.connect()

        # Backup parameters
        self._pool = pool
        self._client = client
        self._images = images
        self._backup_dir = Path(backup_dir)
        self._diffs_dir_name = "diffs"
        self._dummy_snap_name = "dummy"            

        logger.info(f"Attempting to connect to pool {self._pool} ...")
        self._ioctx = self._handler.open_ioctx(self._pool)
        logger.info("Connected")

        self._rbd = rbd.RBD()

        # support wildcard for images
        if len(self._images) == 1 and self._images[0] == '*':
            logger.info("Loading all images...")
            self._images = self._get_images()

#######################################
# Ceph basic interaction
#######################################
    def connect(self):
        """ Connect to the ceph cluster """
        logger.info("Will attempt to connect to: {}".format(
            str(self._handler.conf_get("mon initial members"))))

        self._handler.connect()
        logger.info("Connected. cluster ID [{}]".format(
            self._handler.get_fsid().decode("utf-8")))

    def print_stats(self):
        print("\nCluster Statistics")
        print("==================")
        cluster_stats = self._handler.get_cluster_stats()

        for key, value in cluster_stats.items():
            print(key, value)

    def list_pools(self):
        print("\n\nPool Operations")
        print("===============")

        print("\nAvailable Pools")
        print("----------------")
        pools = self._handler.list_pools()

        for pool in pools:
            print(pool)

#######################################
# Rbd interaction
#######################################

    def full_backup(self):
        """

        """

        current_timestamp = Ceph._get_current_timestamp()
        for image in self._images:            
            try:
                # Check wheter image directory exists, if not, create it
                self._check_image_dir(image)

                # Export the image                
                self._export_image(image, current_timestamp, self._get_image_backup_dir(image))
            except:
                logger.info(f"Failed to do the full backup!")
                raise
    
    def full_diff_backup(self):
        """
        TODO: Cuando el snapshot dummy no esta creado, exportar la imagen completa
        para tener una base con la que recuperar
        """

        current_timestamp = Ceph._get_current_timestamp()
        for image in self._images:                    
            try:
                # Check wheter image differentials directory exists, if not, create it
                self._check_image_diff_dir(image)
                image_has_dummy = self._check_dummy_snap(image)
                # Creates a dummy snapshot if is not exists
                if not image_has_dummy:
                    logger.info("Image has no previous state! The full image it will be also exported")
                    logger.info("Exporting the full image (base image to restore)...")
                    
                    self._export_image(image, current_timestamp, self._get_image_backup_dir(image))            
                    logger.info("Creating the first reference snapshot...")
                    self._create_dummy_snapshot(image)            

                # Export the differential image
                self._export_diff_image(image, current_timestamp, self._dummy_snap_name, self._get_image_diff_backup_dir(image))                

                # Update the dummy snapshot
                self._update_dummy_snapshot(image)
            except:
                logger.info(f"Failed to do the full diff backup!")
                raise

    def print_overview(self):
        """
        
        """

        print(f"\n{Color.GREEN}Backup Overview")
        print(f"=================={Color.END}")
        print(f"{Color.BOLD}Images to backup{Color.END}")
        for image in self._images:
            print(f"\t{self._pool}/{image}")
        print(f"{Color.BOLD}Backup directory:{Color.END}\n\t{self._backup_dir}")

    def print_image_snapshots(self, image_name: str):
        """
        Prints the information about all the snapshots of a given image

         Parameters
        ----------
        image_name : str
            name of the image
        """

        title = f"\n{Color.GREEN}{image_name} snapshots"
        print(title)
        print(f"{'=' * (len(title) - len(Color.GREEN))}{Color.END}")
        for snap in self._get_image_snapshots(image_name):
            print(f"\tid: {snap['id']}, name: {snap['name']}, size: {snap['size']} KB")

#######################################
# Directories structure management
#######################################

    def _check_pool_dir(self):
        """
        Ckeck if the pool directory exists in the backup dir.

        Checks if the pool directory exists in the backup dir. If not, it will
        be created

         Parameters
        ----------
        pool_name : str
            name of the pool
        """
        pool_dir = self._get_pool_dir()
        if not pool_dir.exists():
            logger.info(f"Pool [{pool_dir}] directory does not exist. It will be created")
            pool_dir.mkdir()

    def _check_image_dir(self, image_name: str):
        """
        Ckeck if the image directory exists in the backup dir.

        Checks if the image directory exists in the backup dir. If not, it will
        be created

         Parameters
        ----------
        image_name : str
            name of the image
        """

        self._check_pool_dir()
        image_dir = self._get_image_backup_dir(image_name)
        if not image_dir.exists():
            logger.info(f"Image [{image_dir}] directory does not exist. It will be created")
            image_dir.mkdir()
    
    def _check_image_diff_dir(self, image_name: str):
        """
        Ckeck if the image differentials directory exists in the backup dir.

        Checks if the image differentials directory exists in the backup dir. If not, it will
        be created

         Parameters
        ----------
        image_name : str
            name of the image
        """

        self._check_image_dir(image_name)
        diff_dir = self._get_image_diff_backup_dir(image_name)
        if not diff_dir.exists():
            logger.info(f"Diff [{diff_dir}] directory does not exist. It will be created")
            diff_dir.mkdir()
    
    def _get_pool_dir(self):
        return self._backup_dir.joinpath(self._pool)

    def _get_image_backup_dir(self, image_name):
        return self._get_pool_dir().joinpath(image_name)        
    
    def _get_image_diff_backup_dir(self, image_name):
        return self._get_image_backup_dir(image_name).joinpath(self._diffs_dir_name)        

#######################################
# Images management
#######################################

    def _export_image(self, image_name: str, target_name: str, export_dir: str):
        """
        Export a image

        Export a full image from the pool in the image directory.
        - 1. The export consist of creating a snapshot of the image that ensures 
        the consistency of the backup.
        - 2. The export of that snapshot
        - 3. The removal of the snapshot
        
        Parameters
        ----------
        image_name : str
            name of the image
        target_name : str
            name of the exported snapshot. The final name it will be <image_name>_<target_name>.img
        export_dir : str
            directory where the image will be exported
        """

        try:
            # Create the snapshot
            self._create_snapshot(image_name, target_name)
        
            # Export the snapshot
            self._export_snapshot(image_name, target_name, export_dir)

            # Remove it after exporting
            self._delete_snapshot(image_name, target_name)
        except:
            logger.info(f"Failed to export image {image_name}")
            raise

    def _export_diff_image(self, image_name: str, target_name: str, from_snapshot_name: str, export_dir: str):
        """
        Export a differential image

        Export a differential of a image from the pool. 
        - 1. The export consist of creating a snapshot of the image that ensures 
        the consistency of the backup.
        - 2. The export of the differences between that snapshot and a reference snapshot
        in the past (from_snapshot_name)
        - 3. The removal of the snapshot
        
        Parameters
        ----------
        image_name : str
            name of the image
        target_name : str
            name of the exported snapshot. The final name it will be diff_<image_name>_<target_name>.img
        from_snapshot_name : str
            name of the snapshot from which the diferences are calculated
        export_dir : str
            directory where the snapshot image will be exported
        """

        try:
            # Create the snapshot
            self._create_snapshot(image_name, target_name)

            # Exports the snapshot but with differences from the dummy snap
            self._export_diff_snapshot(image_name, target_name, from_snapshot_name, export_dir)

            # Remove it after exporting
            self._delete_snapshot(image_name, target_name)
        except:
            logger.info(f"Failed to export diff image {image_name}")
            raise

#######################################
# Snapshots management
#######################################

    def _create_snapshot(self, image_name: str, snapshot_name: str):
        """
        
        """
        full_snapshot_name = self._get_full_snapshot_name(image_name, snapshot_name)
        logger.info(f"Attempting to create snapshot {full_snapshot_name}")
        try:
            image = rbd.Image(self._ioctx, image_name)
            image.create_snap(snapshot_name)
            logger.info(f"Snapshot {full_snapshot_name} successfully created")
        except (ImageExists) as e:
            logger.critical(f"Failed to create snapshot {full_snapshot_name}")
            raise e

    def _delete_snapshot(self, image_name: str, snapshot_name: str):
        """
        
        """

        full_snapshot_name = self._get_full_snapshot_name(image_name, snapshot_name)
        logger.info(f"Attempting to delete snapshot {full_snapshot_name}")
        try:
            image = rbd.Image(self._ioctx, image_name)
            image.remove_snap(snapshot_name)
            logger.info(f"Snapshot {full_snapshot_name} successfully deleted")
        except (ImageNotFound, ImageBusy, IOError) as e:
            logger.critical(f"Failed to delete snapshot {full_snapshot_name}")
            raise e

#######################################
# Snapshots export management
#######################################

    def _export_snapshot(self, image_name: str, snapshot_name: str, export_dir: str):
        """
        Export a snapshot

        Export a snapshot to a backup directory defined when the object was created

        The method will perform the following method:
            rbd export --pool pool --image image_name --snap snap_name --path path
        
        Parameters
        ----------
        image_name : str
            name of the image
        snapshot_name : str
            name of the snapshot that will be exported
        export_dir : str
            directory where the snapshot will be exported
        """        

        full_snapshot_name = self._get_full_snapshot_name(image_name, snapshot_name)
        logger.info(f"Attempting to export the snapshot {full_snapshot_name}")

        args = {
            "pool": f"{self._pool}",
            "image": f"{image_name}",
            "snap": f"{snapshot_name}",
            "path": f"{export_dir}/{image_name}_{snapshot_name}.img"
        }

        # Generate a list with all the command parameters
        command = ["rbd", "export"]
        for key, value in args.items():
            command.append(f"--{key}")
            command.append(f"{value}")
        
        # Execute that command
        logger.info(f"Executing command: {' '.join(command)}")
        p = subprocess.run(command,  capture_output=True)
        if p.returncode != 0:
            logger.critical(f"Failed to export snapshot {fullal_snapshot_name}: {p.stderr}")
            raise Exception

    def _export_diff_snapshot(self, image_name: str, snapshot_name: str, from_snapshot_name:str, export_dir):
        """
        Export a differential snapshot

        Export a differential snapshot of a imaged related to another snapshot and
        store it into the backup directory        

        The method will perform the following method:
            rbd export-diff --pool backup-one --image image_name --from-snap from_snapshot_name --snap snapshot_name
                --path path
        
        Parameters
        ----------
        image_name: str
            name of the image
        snapshot_name: str
            name of the snapshot that will be exported
        from_snapshot_name: str
            name of the snapshot from which differences are calculated
        export_dir : str
            directory where the differential snapshot will be exported
        """        

        full_snapshot_name = self._get_full_snapshot_name(image_name, snapshot_name)
        full_from_snapshot_name = self._get_full_snapshot_name(image_name, from_snapshot_name)
        logger.info(f"Attempting to export a diff of {full_snapshot_name} from {full_from_snapshot_name}")

        args = {
            "pool": f"{self._pool}",
            "image": f"{image_name}",
            "from-snap": f"{from_snapshot_name}",            
            "snap": f"{snapshot_name}",            
            "path": f"{export_dir}/diff_{image_name}_{snapshot_name}.img"
        }
        
        # Generate a list with all the command parameters
        command = ["rbd", "export-diff"]
        for key, value in args.items():
            command.append(f"--{key}")
            command.append(f"{value}")
        
        # Execute that command
        logger.info(f"Executing command: {' '.join(command)}")
        p = subprocess.run(command,  capture_output=True)
        if p.returncode != 0:
            logger.critical(f"Failed to export snapshot {fullal_snapshot_name}: {p.stderr}")
            raise Exception

    def get_pool_stats(self):
        """

        """

        print(self._ioctx.get_stats())

    def _get_images(self) -> list:
        """ 
        Fetches a list of all images inside the pool.
        """

        return self._rbd.list(self._ioctx)

    def _get_image_snapshots(self, image_name: str) -> list:
        """
        Get a list with all the snapshots of a given image

        Parameters
        ----------
        image_name : str
            name of the image

        Returns
        -------
        list
            list with the snapshots of an image

        """

        image = rbd.Image(self._ioctx, image_name)
        return image.list_snaps()

#######################################
# Dummy snapshot management
#######################################    

    def _create_dummy_snapshot(self, image_name: str):
        """ 
        Create the dummy snapshot for a given image

        Parameters
        ----------
        image_name : str
            name of the image to check

        """
        try:
            self._create_snapshot(image_name, self._dummy_snap_name)
        except:
          raise
    def _delete_dummy_snapshot(self, image_name: str):
        """
        Delete the dummy snapshot for a given image

        Parameters
        ----------
        image_name : str
            name of the image to check

        """
        try:
            self._delete_snapshot(image_name, self._dummy_snap_name)
        except:
            raise
    
    def _update_dummy_snapshot(self, image_name: str):
        """
        Update the dummy snapshot for a given image

        Recreates the dummy snapshot in order to have always the reference in
        time when the last diff export was done

        Parameters
        ----------
        image_name : str
            name of the image to check

        """
        logger.info(f"Attempting to update the dummy snapshot...")
        try:
            self._delete_dummy_snapshot(image_name)
            self._create_dummy_snapshot(image_name)
        except:
            raise
    
    def _check_dummy_snap(self, image_name: str) -> bool:
        """
        Checks whether the image has a dummy snapshot or not

        The dummy snapshot is used as a time reference to take the snapshot

        Parameters
        ----------
        image_name : str
            name of the image to check

        Returns
        -------
        bool
            the return will be true if the image has a dummy snapshot, false
            otherwise

        """

        snap_list = self._get_image_snapshots(image_name)
        snap_names = [snap["name"] for snap in snap_list]

        if self._dummy_snap_name in snap_names:
            return True
        return False            

    def close_pool_connection(self):
        """ 
        Close the connection with the pool. A connection has to he opened 
        """

        logger.info("\nClosing the connection.")
        self._ioctx.close()

    @staticmethod
    def _get_current_timestamp() -> str:
        """ 
        Returns the current timestamp with a defined format

        Returns
        -------
        str
            current timestamp
        """

        t = int(time.time())
        d = datetime.datetime.fromtimestamp(t).strftime("%Y%m%d")
        return f"{d}-{t}"

    @staticmethod
    def _get_full_snapshot_name(image_name: str, timestamp: int) -> str:
        """ 
        Create a full name for a snapshot using the image name and the timestamp

        Parameters
        ----------
        image_name : str
            name of the image
        timestamp : int
            unix-like timestamp that will be the snapshot name

        Returns
        -------
        str
            full composed name of the snapshot (image_name@timestamp)

        """

        return f"{image_name}@{timestamp}"
