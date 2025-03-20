#!/usr/bin/python3

import sys
import csv
from os import path
from decimal import Decimal
sys.path.append('/app/pyBreezeChMS')
from pyBreezeChMS.breezeapi import add_people_to_breeze
import copy
import json
import datetime
import time
# Import the rate limiter
import breeze_rate_limiter

# Get a single rate-limited API instance for the entire script
breeze_api = None

def parse_square(filename):
    parsed_data = []
    funds = {
            "Donation (Regular)":"General Fund",
            "Enter Amt. (Regular)":"General Fund",
            "Aroti (Regular)":"General Fund",
            "Sun. Feast (Regular)":"Sunday Feast",
            "Choose $ (Regular)":"General Fund",
            "$151.00 (Regular)":"General Fund",
            "Deity Outfit (Regular)":"General Fund",
            "2 x Donation (Regular)":"General Fund",
            "Custom Amount":"General Fund"      
            }
    with open(filename, 'r', encoding="utf8") as data:
        
        for index, line in enumerate(csv.DictReader(data)):
            line["date"] = line.pop("Date")
            line["amount"] = Decimal(line.pop("Gross Sales").replace("$","").replace(",",""))
            line["memo"] = ""
            fund = line.pop("Description")
            if fund in funds:
                line["fund"] = funds[fund]
            else:
                line["fund"] = "Unknown"
                line["note"] = fund
            line["fundnumber"] = ""
            line["method"] = "Square"
            line["name"] = line.pop("Customer Name")
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
            line["phone"] = ""
            line["homephone"] = ""
            line["workphone"] = ""
            line["campus"] = ""            
            line["grade"] = ""
            line["email"] = ""
            line["numstreet"] = ""
            line["city"] = ""
            line["state"] = ""
            line["zip"] = ""
            print(index, ". " ,"First name: ", line["firstname"], " Last name: ", line["lastname"], " Amount: ", line["amount"])          
            if len(names) > 1:
                line["firstname"] = names[0]
                line["lastname"] = names[1]
            elif "@" in name:                
                line["email"] = name
                id = name.split("@")[0]                
                line["firstname"] = id
                line["lastname"] = id
            if line["amount"] != Decimal(0):  # Only append if amount is not 0
                parsed_data.append(line)
    return parsed_data

def save_giving(data, csvfilename):
    _data = copy.deepcopy(data)
    fieldnames = ["firstname", "lastname",  "date", "fund", "fundnumber", "amount", "method",
                "checknumber", "cclastfour", "note","batch"]
    topline = "First Name,Last Name,Date,Fund,Fund Number,Amount,Method,Check Number,CC Last Four,Notes,Batch Number"
    
    with open(csvfilename, 'w') as f:
        f.write(topline+'\n')

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        for index, line in enumerate(_data):
            for field in list(line.keys()):
                    if field not in fieldnames:
                        del line[field]
            writer.writerow(line)
            print(index, ". Firstname ", line["firstname"], " Lastname ", line["lastname"], " Amount ", line["amount"])

def get_person_id(name):
    """Get person IDs from Breeze by name"""
    global breeze_api
    
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
    return person_ids


def get_fund_id(fund):
    """Get fund ID from Breeze"""
    global breeze_api
    
    # Use the shared API instance
    funds = breeze_api.list_funds()
    for fund in funds:  
        if fund['name'] == fund:
            return fund['id']
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
    
    payment_ids = []
    time_of_this_batch = int(time.time())

    for contribution in contributions:
        try:
            # Format the date from MM/DD/YYYY to DD-MM-YYYY as required by Breeze API
            date_parts = contribution['date'].split('/')
            if len(date_parts) == 3:
                breeze_date = f"{date_parts[1]}-{date_parts[0]}-{date_parts[2]}"
                list_contributions_date = f"{date_parts[2]}-{date_parts[0]}-{date_parts[1]}"
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
            
            # Get person IDs (with rate limiting)
            person_ids = get_person_id(name)
            if not person_ids:
                print(f"No matching person found for {name} - skipping contribution")
                continue
                
            # Check to see if this contribution is already in Breeze
            existing_contribution = False
            existing_contributions = breeze_api.list_contributions(
                start_date=list_contributions_date,
                end_date=list_contributions_date,
                amount_min=str(contribution["amount"]),
                amount_max=str(contribution["amount"]),
            )
            
            for contribution_listed in existing_contributions:
                if contribution_listed["person_id"] in person_ids or contribution["firstname"] in contribution_listed["first_name"] and contribution["lastname"] in contribution_listed["last_name"]:
                    print(f"Contribution from {name} for ${contribution['amount']} to {contribution['fund']} on {breeze_date} already exists in Breeze")
                    existing_contribution = True
                    break
                    
            if existing_contribution:
                continue

            # Add the contribution to Breeze (with rate limiting from the API wrapper)
            payment_id = breeze_api.add_contribution(
                date=breeze_date,
                name=name,
                person_id=person_ids[0],
                uid=contribution.get("Customer ID", ""),
                method=contribution["method"],
                funds_json=funds_json,
                amount=str(contribution["amount"]),
                processor="Square",
                group=time_of_this_batch,
                batch_name=f"Square Import {datetime.datetime.now().strftime('%Y-%m-%d')}"
            )
            
            payment_ids.append(payment_id)
            print(f"Added contribution from {name} for ${contribution['amount']} to {contribution['fund']} - Payment ID: {payment_id}")
            
        except Exception as e:
            print(f"Error adding contribution for {contribution.get('name', 'Unknown')}: {str(e)}")
    
    return payment_ids


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 square2breeze.py square.csv")
    else:
        # Initialize the API once at the start
        print("Initializing rate-limited Breeze API...")
        breeze_api = breeze_rate_limiter.get_rate_limited_breeze_api()
        
        print(f"Processing Square data from: {sys.argv[1]}")
        square_data = parse_square(sys.argv[1])
        print(f"Found {len(square_data)} contributions to process")

        # Prepare output filename
        dir = path.dirname(sys.argv[1])
        file = path.basename(sys.argv[1])
        givingoutfile = '.'.join(file.split(".")[:-1])+"_giving_ready_for_breeze.csv"  
        
        print(f"Saving contributions to CSV: {givingoutfile}")

        save_giving(square_data, givingoutfile)

        # Process a test contribution first
        if len(square_data) > 0:
            test_data = square_data[:1]
            print(test_data)
            print("Processing a test contribution first:")
            
            # Use existing add_people_to_breeze function with rate limiting already applied
            #add_people_to_breeze(test_data)
            #add_giving_to_breeze(test_data)

            add_people_to_breeze(square_data)
            add_giving_to_breeze(square_data)