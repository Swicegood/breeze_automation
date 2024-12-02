import sys
import os
sys.path.append('./pyBreezeChMS')
#sys.path.append('/mnt/y/My Drive/Computer/python/breeze/pyBreezeChMS')
from breezeapi import breeze_api
import json
from datetime import date

if __name__ == "__main__":
    people = breeze_api.get_people()
    #save people to file
    today = date.today().strftime("%Y-%m-%d")
    
    # Create backups directory if it doesn't exist
    backup_dir = "/backups"
    os.makedirs(backup_dir, exist_ok=True)

    # Join path with filename
    filename = os.path.join(backup_dir, f"people_{today}.json")

    with open(filename, 'w') as f:
        f.write(json.dumps(people))
        
    #Keep only 5 backups
    backups = os.listdir(backup_dir)
    backups.sort()
    if len(backups) > 5:
        os.remove(os.path.join(backup_dir, backups[0]))