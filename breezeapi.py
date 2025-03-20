from breeze import breeze
import copy
import json
import time
import os
import logging
# Import the rate limiter
import breeze_rate_limiter

# Configure logging to write to stdout (which Docker will capture)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger('breezeapi')

# Use the singleton pattern for the API instance
breeze_api = breeze_rate_limiter.get_rate_limited_breeze_api()


def add_people_to_breeze(peopledata):
    logger.info(f"Adding/updating {len(peopledata)} people in Breeze")
    
    _data = copy.deepcopy(peopledata)
    fieldnames = ["firstname", "lastname",  "middlename", "nickname", "maidenname", "gender", 
                "status", "maritalstatus", "birthdate","familyid","familyrole", "school", "grade",
                "occupation", "phone","homephone","workphone","campus", "email", "numstreet", 
                "city", "state", "zip",]   
    
    processed_count = 0
    updated_count = 0
    added_count = 0
    
    for idx, line in enumerate(_data):
        match = False
        for field in list(line.keys()):
                if field not in fieldnames:
                    del line[field]                    
        fields = []
        if line["email"] != "":
            field0 = {}
            field0["field_id"] = "2040842366"
            field0["field_type"] = "email"
            field0["response"] = "true"
            field0["details"] = {}
            field0["details"]["address"] = line["email"]
            fields.append(field0)
        if line["numstreet"] != "" and line["city"] != "":
            field1 = {}
            field1["field_id"] = "1363781938"
            field1["field_type"] = "address"
            field1["response"] = "true"
            address1 = {}
            address1["street_address"] = line["numstreet"]
            address1["city"] = line["city"]
            address1["state"] = line["state"]
            address1["zip"] = line["zip"]
            field1["details"] = address1
            fields.append(field1)
        if line["phone"] != "":
            field2 = {}
            field2["field_id"] = "1530627561"
            field2["field_type"] = "phone"
            field2["response"] = "true"
            phone2 = {}
            phone2["phone_mobile"] = line["phone"]
            field2["details"] = phone2
            fields.append(field2)

        name = f"{line['firstname']} {line['lastname']}"
        logger.info(f"Processing person [{idx+1}/{len(_data)}]: {name}")
        
        people = breeze_api.get_people()

        for person in people: 
            if line["firstname"].upper() == person["first_name"].upper() and line["lastname"].upper() == person["last_name"].upper():
                match = True
                logger.info(f"Found existing person {person['first_name']} {person['last_name']} (ID: {person['id']}) - updating")
                updateperson = breeze_api.update_person(person["id"], json.dumps(fields))
                logger.info(f"Updated person: {updateperson['first_name']} {updateperson['last_name']}")
                updated_count += 1
                processed_count += 1
                break
                
        if not match and (line["firstname"] != "" or line["lastname"] != ""):  
            logger.info(f"Person {name} not found - adding to Breeze")
            addperson = breeze_api.add_person(line["firstname"], line["lastname"], json.dumps(fields))
            logger.info(f"Added person: {addperson['first_name']} {addperson['last_name']}")
            added_count += 1
            processed_count += 1
    
    logger.info(f"People processing complete. Processed: {processed_count}, Updated: {updated_count}, Added: {added_count}")

def get_batches(batchlist):
    logger.info(f"Getting contributions from batches: {batchlist}")
    
    batches = ""
    for i in range(len(batchlist)):
        if i < len(batchlist) - 1:
            batches += str(batchlist[i])+"-"
        else:
            batches += str(batchlist[i])

    contributions = breeze_api.list_contributions(batches={batches})
    logger.info(f"Retrieved {len(contributions)} contributions from batches")
    return contributions

def contributions_with_addresses(batch_data):
    logger.info(f"Processing {len(batch_data)} contributions to add address information")
    
    contribution_data = []
    for index, contribution in enumerate(batch_data):
        contrib = {}
        contrib["date"] = contribution["paid_on"].split(' ')[0]
        contrib["firstname"] = contribution["first_name"]
        contrib["lastname"] = contribution["last_name"]
        contrib["amount"] = contribution["funds"][0]["amount"]
        contrib["fund"] = contribution["funds"][0]["fund_name"]
        contrib["note"] = contribution["note"]
        
        logger.info(f"Getting details for contribution [{index+1}/{len(batch_data)}]: {contrib['firstname']} {contrib['lastname']} (${contrib['amount']})")
        donor = breeze_api.get_person_details(contribution["person_id"])
        contrib["numstreet"] = donor["street_address"]
        contrib["city"] = donor["city"]
        contrib["state"] = donor["state"]
        contrib["zip"] = donor["zip"]
        
        logger.debug(f"Processed contribution {index+1}: {contrib['firstname']} {contrib['lastname']}, ${contrib['amount']}")
        contribution_data.append(contrib)
    
    logger.info(f"Successfully processed {len(contribution_data)} contributions with addresses")
    return contribution_data


if __name__ == "__main__":
    logger.warning("This breeze module is not to be run stand-alone.")