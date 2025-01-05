#!/usr/bin/python3

import sys
import csv
from os import path
from decimal import Decimal
sys.path.append('/app/pyBreezeChMS')
from pyBreezeChMS.breezeapi import add_people_to_breeze
import copy

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 square2breeze.py square.csv")
    else:
        square_data = parse_square(sys.argv[1])
        dir = path.dirname(sys.argv[1])
        file = path.basename(sys.argv[1])
        givingoutfile = '.'.join(file.split(".")[:-1])+"_giving_ready_for_breeze.csv"   
        save_giving(square_data, givingoutfile)
        add_people_to_breeze(square_data)