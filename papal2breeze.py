#!/usr/bin/python3

import sys
import csv
from os import path
from decimal import Decimal
import json
sys.path.append('/app/pyBreezeChMS')
sys.path.append('/mnt/y/My Drive/Computer/python/breeze_automation/pyBreezeChMS')
from pyBreezeChMS.breezeapi import add_people_to_breeze
from square2breeze import save_giving
from time import sleep
import logging
# Import the rate limiter
import breeze_rate_limiter

# Configure logging to write to stdout (which Docker will capture)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger('paypal2breeze')

# Get a single rate-limited API instance for the entire script
breeze_api = None


def parse_paypal(filename):
    parsed_data = []
    logger.info(f"Opening PayPal data file: {filename}")
    
    with open(filename, 'r', encoding="cp1252", errors="replace") as data:
        
        for idx, line in enumerate(csv.DictReader(data, delimiter=",")):
            line["date"] = line.pop("Date")
            line["amount"] = Decimal(line.pop("Gross").replace("$","").replace(",",""))
            line["memo"] = ""
            fund = line.pop("Item Title")
            if len(fund) < 1:
                line["fund"] = "unknown"
            else:
                line["fund"] = fund
            line["fundnumber"] = ""
            line["method"] = "Paypal"
            line["name"] = line.pop("Name")
            line["firstname"] = ""
            line["lastname"] = ""
            name = line["name"]
            names = name.split(" ")
            line["checknumber"] = ""
            line["cclastfour"] = ""
            line["batch"] = ""
            line["middlename"] = ""
            line["nickname"] = ""
            line["maidenname"] = ""
            line["gender"] = ""
            line["status"] = ""
            line["maritalstatus"] = ""
            line["birthdate"] = ""
            line["familyid"] = ""
            line["familyrole"] = ""
            line["school"] = ""
            line["grade"] = ""            
            line["occupation"] = ""
            line["phone"] = line.pop("Contact Phone Number")
            line["homephone"] = ""
            line["workphone"] = ""
            line["campus"] = ""            
            line["grade"] = ""
            line["email"] = line.pop("From Email Address")
            line["numstreet"] = line.pop("Address Line 1")
            line["numstreet"] += line.pop("Address Line 2/District/Neighborhood")
            line["city"] = line.pop("Town/City")
            line["state"] = line.pop("State/Province/Region/County/Territory/Prefecture/Republic")
            line["zip"] = line.pop("Zip/Postal Code")
            line["note"] = line.pop("Note")
            if len(names) > 1:
                line["firstname"] = " ".join(names[:-1])
                line["lastname"] = names[-1]
            
            logger.debug(f"Row {idx}: {line['firstname']} {line['lastname']} (${line['amount']})")
            parsed_data.append(line)
    
    logger.info(f"Parsed {len(parsed_data)} contributions from PayPal data")
    return parsed_data

def get_person_id(name):
    """Get person IDs from Breeze by name"""
    global breeze_api
    
    logger.debug(f"Looking up person ID for: {name}")
    # Use the shared API instance
    people = breeze_api.get_people()
    person_ids = []
    for person in people:
        name_parts = name.split(" ")
        name_parts = [name_part.lower() for name_part in name_parts]
        if len(name_parts) > 1:
            first_name = " ".join(name_parts[:-1])  # Join all but the last part as first name
            last_name = name_parts[-1]  # Last part is the last name
            if person['first_name'].lower() == first_name and person['last_name'].lower() == last_name:
                person_ids.append(person['id'])
                logger.debug(f"Found person ID {person['id']} for {name}")
    
    if not person_ids:
        logger.warning(f"No matching person found for {name}")
    
    return person_ids


def get_fund_id(fund):
    """Get fund ID from Breeze"""
    global breeze_api
    
    logger.debug(f"Looking up fund ID for: {fund}")
    # Use the shared API instance
    funds = breeze_api.list_funds()
    for fund in funds:  
        if fund['name'] == fund:
            logger.debug(f"Found fund ID {fund['id']} for {fund['name']}")
            return fund['id']
    
    logger.warning(f"No matching fund found for {fund}")
    return None

def add_giving_to_breeze(contributions):
    """Add PayPal contributions to Breeze with proper rate limiting
    
    Args:
        contributions: List of contribution dictionaries to add to Breeze
        
    Returns:
        List of payment IDs created in Breeze
    """
    global breeze_api
    
    import os
    import datetime
    import time
    
    logger.info(f"Adding {len(contributions)} contributions to Breeze")
    payment_ids = []
    time_of_this_batch = int(time.time())
    processed_count = 0
    skipped_count = 0
    error_count = 0

    for idx, contribution in enumerate(contributions):
        try:
            # Format the date from MM/DD/YYYY to DD-MM-YYYY as required by Breeze API
            date_parts = contribution['date'].split('/')
            if len(date_parts) == 3:
                breeze_date = f"{date_parts[1]}-{date_parts[0]}-{date_parts[2]}"
            else:
                # If date is not in expected format, use current date
                today = datetime.datetime.now()
                breeze_date = f"{today.day}-{today.month}-{today.year}"
            
            # Create funds_json for the contribution
            funds = [{
                "name": "General Fund",
                "amount": str(contribution["amount"])
            }]
            
            contribution["fund"] = "General Fund"
            # Convert funds to JSON string
            funds_json = json.dumps(funds)
            
            # Prepare person information - either use their email or name to match
            name = f"{contribution['firstname']} {contribution['lastname']}"
            if name.strip() == "":
                name = contribution['name']
            
            # Progress log
            logger.info(f"Processing contribution [{idx+1}/{len(contributions)}]: {name} (${contribution['amount']})")
            
            # Get person IDs
            person_ids = get_person_id(name)
            if not person_ids:
                logger.warning(f"No matching person found for {name} - skipping contribution")
                skipped_count += 1
                continue
                
            # Check to see if this contribution is already in Breeze
            existing_contribution = False
            existing_contributions = breeze_api.list_contributions(
                start_date=breeze_date,
                end_date=breeze_date,
                amount_min=str(contribution["amount"]),
                amount_max=str(contribution["amount"]),
            )
            
            for c in existing_contributions:
                if c["person_id"] in person_ids or contribution["firstname"] in c["first_name"] and contribution["lastname"] in c["last_name"]:
                    logger.info(f"Contribution from {name} for ${contribution['amount']} to {contribution['fund']} on {breeze_date} already exists in Breeze")
                    existing_contribution = True
                    skipped_count += 1
                    break
                    
            if existing_contribution:
                continue

            # Add the contribution to Breeze
            payment_id = breeze_api.add_contribution(
                date=breeze_date,
                name=name,
                person_id=person_ids[0],  # Use the first matched person ID
                method=contribution["method"],
                funds_json=funds_json,
                amount=str(contribution["amount"]),
                group=time_of_this_batch,
                batch_name=f"Paypal Import {datetime.datetime.now().strftime('%Y-%m-%d')}"
            )
            
            payment_ids.append(payment_id)
            processed_count += 1
            logger.info(f"Added contribution from {name} for ${contribution['amount']} to {contribution['fund']} - Payment ID: {payment_id}")
            
        except Exception as e:
            error_count += 1
            logger.error(f"Error adding contribution for {contribution.get('name', 'Unknown')}: {str(e)}", exc_info=True)
    
    logger.info(f"Contribution processing complete. Added: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")
    return payment_ids
    


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Missing input file. Usage: python3 paypal2breeze.py paypal.csv")
        print("Usage: python3 paypal2breeze.py paypal.csv")
        sys.exit(1)
    else:
        # Initialize the API once at the start
        logger.info("Initializing rate-limited Breeze API...")
        breeze_api = breeze_rate_limiter.get_rate_limited_breeze_api()
        
        logger.info(f"Processing PayPal data from: {sys.argv[1]}")
        paypal_data = parse_paypal(sys.argv[1])
        logger.info(f"Found {len(paypal_data)} contributions to process")
        
        # Prepare output filename
        dir = path.dirname(sys.argv[1])
        file = path.basename(sys.argv[1])
        givingoutfile = '.'.join(file.split(".")[:-1])+"_giving_ready_for_breeze.csv"
        
        # Save CSV for review
        logger.info(f"Saving contributions to CSV: {givingoutfile}")
        save_giving(paypal_data, givingoutfile)
        
        # Process a test contribution first
        if len(paypal_data) > 0:
            test_data = paypal_data[:1]
            logger.info(f"Test data: {test_data}")
            logger.info("Processing test contribution:")
            
            # Uncomment to test with a single contribution first
            #add_people_to_breeze(test_data)
            #add_giving_to_breeze(test_data)
            
            # Process all contributions
            logger.info(f"Processing all {len(paypal_data)} contributions")
            add_people_to_breeze(paypal_data)
            add_giving_to_breeze(paypal_data)
            
            logger.info("All processing complete!")
        else:
            logger.warning("No contributions found to process")