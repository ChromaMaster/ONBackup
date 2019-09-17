# ONBackup

Utility used to perform OpenNebula backups.

In order to do that, your opennebula datastore must use ceph as backend for
your images datastore.

## Table of Contents

- [ONBackup](#onbackup)
  - [Table of Contents](#table-of-contents)
  - [TODO list](#todo-list)
  - [Dependencies](#dependencies)
  - [Configuration](#configuration)
  - [License](#license)

## TODO list

- [x] Perform a ceph pool full backup
- [x] Perform a ceph pool differential backup
- [x] Monitoring the process (PandoraFMS plugin and/or Prometheus)
- [ ] Interact with the vms before the backup in order to flush mysql
- [ ] Allow the user not only do the backup but also restoring

## Dependencies

- pipenv
- python3-rados
- python3-rbd

```sh
apt install pipenv python3-rados
```
**[Back to top](#table-of-contents)**

## Configuration

All the configuration will be stored in the project **etc** directory under a file
called **ceph.yaml**.

This file will contain the following config by default

```yaml
---
app:
  verbose: False  
  log_file: onbackup.log
cluster:
  conf_file: "etc/ceph/ceph.conf"
  user_keyring: "etc/ceph/ceph.client.onebackup.keyring"
  client: onebackup
backup:
  type: full
  pool: ""
  directory: ""
  images:    
    - "*"
```

This means that, by default, it will make a full backup of your ceph pool and
also you **must** specify the pool and the directory. 

This can be achieved by modifying the file using a text editor or using the 
program arguments.

```
python main.py --help

usage: main.py [-h] [--ceph PATH] [--user_keyring PATH] [--user USER]
               [-p POOL] [-i IMAGES [IMAGES ...]] [-d DIRECTORY] [-v]
               [--log-file LOG_FILE] [--full | --diff]

optional arguments:
  -h, --help            show this help message and exit
  --ceph PATH           Path of the ceph config file
  --user_keyring PATH   Path of the client keyring file
  --user USER           Client name (without 'client.' prefix)
  -p POOL, --pool POOL  Source pool name
  -i IMAGES [IMAGES ...], --images IMAGES [IMAGES ...]
                        List of images to backup ('*' for all)
  -d DIRECTORY, --directory DIRECTORY
                        Target directory where backups will be stored
  -v, --verbose         Make the program verbose
  --log-file LOG_FILE   Set the logging file path
  --full                Perform a full image backup
  --diff                Perform a incremental image backup
```

**[Back to top](#table-of-contents)**

## License

This project is licensed under the GPLv3 License - see the [LICENSE.md](LICENSE.md) file for details

**[Back to top](#table-of-contents)**
