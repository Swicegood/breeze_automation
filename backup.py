import sys
import os
sys.path.append('./pyBreezeChMS')
#sys.path.append('/mnt/y/My Drive/Computer/python/breeze/pyBreezeChMS')
from breezeapi import breeze_api
import json
from datetime import date
import requests
from urllib.parse import urljoin

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

    # Backup profile pictures
    profile_pics_dir = os.path.join(backup_dir, f"profile_pics_{today}")
    os.makedirs(profile_pics_dir, exist_ok=True)

    # Base URL for Breeze images - using the files domain
    base_url = "https://files.breezechms.com/"
    
    # Get API key from environment
    api_key = os.environ.get('API_KEY')
    if not api_key:
        print("Error: API_KEY environment variable not set")
        sys.exit(1)
    
    # Set up headers with API key
    headers = {
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    for person in people:
        if 'path' in person and person['path']:
            # Skip generic profile pictures
            if 'generic' in person['path']:
                continue
                
            # Get the filename from the path
            filename = os.path.basename(person['path'])
            
            # Construct the full URL
            image_url = urljoin(base_url, person['path'])
            
            # Save the image
            try:
                response = requests.get(image_url, headers=headers)
                if response.status_code == 200:
                    # Save with person's ID and name for easy reference
                    save_path = os.path.join(profile_pics_dir, f"{person['id']}_{person['first_name']}_{person['last_name']}_{filename}")
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                    print(f"Saved profile picture for {person['first_name']} {person['last_name']}")
                else:
                    print(f"Failed to download profile picture for {person['first_name']} {person['last_name']}: {response.status_code}")
                    if response.status_code == 403:
                        print("Access denied - check if API key is valid")
            except Exception as e:
                print(f"Error downloading profile picture for {person['first_name']} {person['last_name']}: {str(e)}")

    # Keep only the 5 most recent profile picture backups
    profile_pic_backups = [d for d in os.listdir(backup_dir) if d.startswith('profile_pics_')]
    profile_pic_backups.sort()
    for backup in profile_pic_backups[:-5]:
        backup_path = os.path.join(backup_dir, backup)
        if os.path.isdir(backup_path):
            for file in os.listdir(backup_path):
                os.remove(os.path.join(backup_path, file))
            os.rmdir(backup_path) 
