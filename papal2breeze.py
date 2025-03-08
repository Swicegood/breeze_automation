#!/usr/bin/python3

import sys
import csv
from os import path
from decimal import Decimal
import json
sys.path.append('/app/pyBreezeChMS')
from pyBreezeChMS.breezeapi import add_people_to_breeze
from square2breeze import save_giving
from time import sleep



def parse_paypal(filename):
    parsed_data = []
    with open(filename, 'r', encoding="cp1252", errors="replace") as data:
        
        for line in csv.DictReader(data, delimiter=","):
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
            parsed_data.append(line)
    return parsed_data

def get_person_id(name):
    from breeze import breeze
    import os
    import datetime
    
    # Initialize the Breeze API with credentials (should be stored as environment variables)
    api_key = os.environ.get('API_KEY')
    subdomain = 'https://iskconofnc.breezechms.com'
    breeze_api = breeze.BreezeApi(subdomain, api_key)
    people = breeze_api.get_people()
    for person in people:
        if person['first_name'] == name.split(" ")[0] and len(name.split(" ")) > 1 and person['last_name'] == name.split(" ")[1]:
            return person['id']
    return None


def get_fund_id(fund):
    from breeze import breeze
    import os
    import datetime
    
    # Initialize the Breeze API with credentials (should be stored as environment variables)
    api_key = os.environ.get('API_KEY')
    subdomain = 'https://iskconofnc.breezechms.com'
    breeze_api = breeze.BreezeApi(subdomain, api_key)
    funds = breeze_api.list_funds()
    for fund in funds:  
        if fund['name'] == fund:
            return fund['id']
    return None

def add_giving_to_breeze(contributions):
    """Add PayPal contributions to Breeze
    
    Args:
        contributions: List of contribution dictionaries to add to Breeze
        
    Returns:
        List of payment IDs created in Breeze
    """
    from breeze import breeze
    import os
    import datetime
    
    # Initialize the Breeze API with credentials (should be stored as environment variables)
    api_key = os.environ.get('API_KEY')
    subdomain = 'https://iskconofnc.breezechms.com'
    
    if not api_key or not subdomain:
        print("Error: API_KEY and BREEZE_SUBDOMAIN environment variables must be set")
        return []
    
    breeze_api = breeze.BreezeApi(subdomain, api_key)
    payment_ids = []
    
    for contribution in contributions:
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
            
            # Check to see if this contribution is already in Breeze
            existing_contribution = breeze_api.list_contributions(
                start_date=breeze_date,
                end_date=breeze_date,
                person_id=get_person_id(name),
                amount_min=str(contribution["amount"]),
                amount_max=str(contribution["amount"]),
                fund_ids=get_fund_id("General Fund"),

            )
            if len(existing_contribution) > 0:
                print(f"Contribution from {name} for ${contribution['amount']} to {contribution['fund']} on {breeze_date} already exists in Breeze")
                continue


            # Add the contribution to Breeze
            payment_id = breeze_api.add_contribution(
                date=breeze_date,
                name=name,
                person_id=get_person_id(name),
                method=contribution["method"],
                funds_json=funds_json,
                amount=str(contribution["amount"]),
                processor="Paypal",
                batch_name=f"Paypal Import {datetime.datetime.now().strftime('%Y-%m-%d')}"
            )
            
            payment_ids.append(payment_id)
            print(f"Added contribution from {name} for ${contribution['amount']} to {contribution['fund']} - Payment ID: {payment_id}")

            sleep(3.5)
            
        except Exception as e:
            print(f"Error adding contribution for {contribution.get('name', 'Unknown')}: {str(e)}")
    
    return payment_ids
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 paypal2breeze.py paypal.csv")
    else:
        print(f"Processing PayPal data from: {sys.argv[1]}")
        paypal_data = parse_paypal(sys.argv[1])
        print(f"Found {len(paypal_data)} contributions to process")
        
        # Prepare output filename
        dir = path.dirname(sys.argv[1])
        file = path.basename(sys.argv[1])
        givingoutfile = '.'.join(file.split(".")[:-1])+"_giving_ready_for_breeze.csv"
        
        # Save CSV for review
        print(f"Saving contributions to CSV: {givingoutfile}")
        save_giving(paypal_data, givingoutfile)
        
        # Process a test contribution first
        if len(paypal_data) > 0:
            test_data = paypal_data[:1]
            print("Processing test contribution:")
            add_giving_to_breeze(test_data)
            
            # Uncomment to process all contributions
            print("Processing all contributions:")
            add_people_to_breeze(paypal_data)
            add_giving_to_breeze(paypal_data)
        else:
            print("No contributions found to process")