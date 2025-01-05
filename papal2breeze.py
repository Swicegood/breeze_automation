#!/usr/bin/python3

import sys
import csv
from os import path
from decimal import Decimal
sys.path.append('/app/pyBreezeChMS')
from pyBreezeChMS.breezeapi import add_people_to_breeze
from square2breeze import save_giving



def parse_paypal(filename):
    parsed_data = []
    with open(filename, 'r', encoding="utf-8") as data:
        
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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 paypal2breeze.py paypal.csv")
    else:
        paypal_data = parse_paypal(sys.argv[1])
        dir = path.dirname(sys.argv[1])
        file = path.basename(sys.argv[1])
        givingoutfile = '.'.join(file.split(".")[:-1])+"_giving_ready_for_breeze.csv"   
        save_giving(paypal_data, givingoutfile)
        add_people_to_breeze(paypal_data)