from breeze import breeze
import copy
import json
import time
import os
api_key = os.environ['API_KEY']

# Create a BreezeApi object and pass in the url and api key

breeze_api = breeze.BreezeApi(
    breeze_url='https://iskconofnc.breezechms.com',
    api_key=api_key)


def add_people_to_breeze(peopledata):
    _data = copy.deepcopy(peopledata)
    fieldnames = ["firstname", "lastname",  "middlename", "nickname", "maidenname", "gender", 
                "status", "maritalstatus", "birthdate","familyid","familyrole", "school", "grade",
                "occupation", "phone","homephone","workphone","campus", "email", "numstreet", 
                "city", "state", "zip",]   
    
      
    for line in _data:
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

        people = breeze_api.get_people()

        for person in people:         
            if line["firstname"].upper() == person["first_name"].upper() and line["lastname"].upper() == person["last_name"].upper():
                time.sleep(3.5) 
                match = True
                updateperson = breeze_api.update_person(person["id"], json.dumps(fields))
                print(updateperson)
        if not match and (line["firstname"] != "" or line["lastname"] != ""):
            time.sleep(3.5)          
            addperson = breeze_api.add_person(line["firstname"], line["lastname"], json.dumps(fields))
            print(addperson)
            print(json.dumps(fields))

def get_batches(batchlist):
    batches = ""
    for i in range(len(batchlist)):
        if i < len(batchlist) - 1:
            batches += str(batchlist[i])+"-"
        else:
            batches += str(batchlist[i])

    contributions = breeze_api.list_contributions(batches={batches})
    return contributions

def contributions_with_addresses(batch_data):
    contribution_data = []
    for index, contribution in enumerate(batch_data):
        contrib = {}
        contrib["date"] = contribution["paid_on"].split(' ')[0]
        contrib["firstname"] = contribution["first_name"]
        contrib["lastname"] = contribution["last_name"]
        contrib["amount"] = contribution["funds"][0]["amount"]
        contrib["fund"] = contribution["funds"][0]["fund_name"]
        contrib["note"] = contribution["note"]
        time.sleep(3.5)
        donor = breeze_api.get_person_details(contribution["person_id"])
        contrib["numstreet"] = donor["street_address"]
        contrib["city"] = donor["city"]
        contrib["state"] = donor["state"]
        contrib["zip"] = donor["zip"]
        # print output in a very pretty way
        print(str(index+1)+".   ")
        print(contrib)
        contribution_data.append(contrib)
    return contribution_data


if __name__ == "__main__":
    print("This breeze module is not to be run stand-alone.")