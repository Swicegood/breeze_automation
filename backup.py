import sys
import os
sys.path.append('./pyBreezeChMS')
#sys.path.append('/mnt/y/My Drive/Computer/python/breeze/pyBreezeChMS')
from breezeapi import breeze_api
import json
from datetime import date

if __name__ == "__main__":
    people = breeze_api.get_people()

    giving = breeze_api.list_contributions(
        start_date=date(2000, 1, 1),
        end_date=date(2099, 12, 31)
    )

    #save people to file
    today = date.today().strftime("%Y-%m-%d")
    
    # Create backups directory if it doesn't exist
    backup_dir = "/backups"
    os.makedirs(backup_dir, exist_ok=True)

    # Join path with filename
    filename = os.path.join(backup_dir, f"people_{today}.json")
    filename_giving = os.path.join(backup_dir, f"giving_{today}.json")

    with open(filename, 'w') as f:
        f.write(json.dumps(people))

    with open(filename_giving, 'w') as f:
        f.write(json.dumps(giving))
        
    #Keep only 5 backups of each type, giving and people for a total of 10 backups
    backups = os.listdir(backup_dir)
    
    # Process people backups
    people_backups = [b for b in backups if b.startswith('people_')]
    people_backups.sort()  # Sort alphabetically and chronologically
    for backup in people_backups[:-5]:  # Keep only the 5 most recent people backups
        os.remove(os.path.join(backup_dir, backup))
    
    # Process giving backups
    giving_backups = [b for b in backups if b.startswith('giving_')]
    giving_backups.sort()  # Sort alphabetically and chronologically 
    for backup in giving_backups[:-5]:  # Keep only the 5 most recent giving backups
        os.remove(os.path.join(backup_dir, backup)) 
