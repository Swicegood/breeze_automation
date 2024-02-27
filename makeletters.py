import sys
import csv
from os import path
from xlsx2csv import Xlsx2csv
from decimal import Decimal

def parse_giving(filename):
    parsed_data = []
    with open(filename, 'r', encoding="utf8") as data:
        
        for line in csv.DictReader(data):
            line["date"] = line.pop('Date')
            line["amount"] = Decimal(line.pop("Amount").replace("$","").replace(",",""))
            line["firstname"] = line.pop('First Name')
            line["lastname"] = line.pop("Last Name")    
            line["personid"] = line.pop("Person ID")
            line["paymentid"] = line.pop("Payment ID")
            line['fund'] = line.pop("Fund(s)")
            line['note'] = line.pop("Note")
            print(line) 
            parsed_data.append(line)
    return parsed_data

def parse_people(filename):
    parsed_data = []
    with open(filename, 'r', encoding="utf8") as data:
        
        for line in csv.DictReader(data):
            line["addeddate"] = line.pop('Added Date')
            line["firstname"] = line.pop('First Name')
            line["lastname"] = line.pop("Last Name")    
            line["breezeid"] = line.pop('Breeze ID')
            line["numstreet"] = line.pop("Street Address")
            line["city"] = line.pop('City')
            line["state"] = line.pop('State')
            line["zip"] = line.pop('Zip')
            print(line) 
            parsed_data.append(line)
    return parsed_data

def save(batch_data, csvfilename):
    fieldnames = ["date", "firstname", "lastname", "amount","fund", "numstreet", "city", "state", "zip","note"]
    topline = "Date,First,Last,Amt,Fund(s),Address,City,State,Zip,Note"
    
    with open(csvfilename, 'w') as f:
        f.write(topline+'\n')

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        for line in batch_data:
            writer.writerow(line)
        
def mergefilenames(file1, file2):
    f1 = path.basename(file1)
    f2 = path.basename(file2)
    b1 = f1.split(".")[:-1]
    b2 = f2.split(".")[:-1]
    return ".".join(b1)+".".join(b2)+".csv"

def csvfilename(file):
    f1 = path.basename(file)
    b1 = f1.split(".")[:-1]
    return ".".join(b1)+".csv"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 makeletters.py Breeze_giving.xlsx Breeze_people.xlsx")
    else:
        giving_file = csvfilename(sys.argv[1])
        people_file = csvfilename(sys.argv[2])
        Xlsx2csv(sys.argv[1], outputencoding="utf-8").convert(giving_file)
        Xlsx2csv(sys.argv[2], outputencoding="utf-8").convert(people_file)
        giving_data = parse_giving(giving_file)
        people_data = parse_people(people_file)
        outfile = mergefilenames(sys.argv[1], sys.argv[2])        
        save(giving_data, people_data, outfile)
