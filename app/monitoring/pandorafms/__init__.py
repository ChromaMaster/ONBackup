#! /usr/bin/python3
import sys
from datetime import datetime
FMT = '%H:%M:%S'

# https://pandorafms.com/docs/index.php?title=Pandora:Documentation_es:Operacion
AVAILABLE_TYPES = [
    "generic_data", "generic_data_inc", "generic_data_inc_abs", "generic_proc", 
    "generic_data_string", "async_data", "async_string", "async_proc"]    

class Module:
    def __init__(self, name, type, description, data):
        self._name = name
        
        if type in AVAILABLE_TYPES:
            self._type = type
        else:
            raise
            
        self._description = description
        self._data = data

    def __str__(self):
        r = "<module>\n"        
        r+= f"\t<name><![CDATA[{self._name}]]></name>\n"
        r+= f"\t<type><![CDATA[{self._type}]]></type>\n"
        r+= f"\t<data><![CDATA[{self._data}]]></data>\n"
        r+= f"\t<description>{self._description}</description>\n"
        r+= "</module>\n"
        return r

def parse_log_file(log_file_path):

    modules = []
    lines = []
    

    with open(log_file_path, "r") as f:
        line = f.readline()
        while line:
            if "BACKUP" in line:
                lines.append(line)
            
            line = f.readline()    

    images_status = {}
    
    for line in lines:        
        parts = line.split(" - ")
        image_name = parts[4].rstrip()
        backup_type = parts[3]
        status = parts[2]
        date, time = parts[0].split(" ")
        
        if image_name not in images_status:
            images_status[image_name] = {}

        images_status[image_name][status] = {
            "backup_type": backup_type,
            "date":  date,
            "time": time.split(",")[0]
        }                
    
    for key, value in images_status.items():        
        backup_status_data = 0
        backup_elapsed_time_data = 0
        if "START" in value and "END" in value:        
            backup_status_data = 1
            backup_elapsed_time_data = datetime.strptime(value["END"]["time"], FMT) - datetime.strptime(value["START"]["time"], FMT)            

        m = Module(f"ceph_backup_{key}_status", "generic_data", "Estado del backup", backup_status_data)
        modules.append(m)
        m = Module(f"ceph_backup_{key}_elapsed_time", "generic_data", "Tiempo empleado en el backup", backup_elapsed_time_data.total_seconds())        
        modules.append(m)        
    
    for module in modules:
        print(module)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        parse_log_file(sys.argv[1])