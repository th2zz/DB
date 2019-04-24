
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS145 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        f1 = open('items.dat', 'a')
        f2 = open('categories.dat', 'a')
        f3 = open('bids.dat', 'a')
        f4 = open('users.dat', 'a')
        for item in items:
            
            # Items Table
            
           

            f1.write(item['ItemID'] + "|\"")
            f1.write(item['Name'].replace('\"', '\"\"') + "\"|")
            f1.write(transformDollar(item['Currently']) + "|")
            f1.write(transformDollar(item['First_Bid']) + "|")
            try:
                f1.write(transformDollar(item['Buy_Price']) + "|")
            except KeyError:
                f1.write("-999" + "|")
            f1.write(item['Number_of_Bids'] + "|\"")
            f1.write(transformDttm(item['Started']) + "\"|\"")
            f1.write(transformDttm(item['Ends']) + "\"|\"")
            seller = item['Seller']
            f1.write(seller['UserID'] + "\"|\"")
            if item['Description'] is not None:
                f1.write(str(item['Description'].replace('\"', '\"\"')) + "\"\n")
            else:
                f1.write("empty" + "\"\n")
            
          
            # Categories Table

            
            for category in item['Category']:
                f2.write(item['ItemID'] + "|\"")
                f2.write(category + "\"\n")
            
            # Bids Table
            
            
            if item['Number_of_Bids'] != "0":
                bids = item['Bids']
                for bidsIterator in bids:
                    f3.write(item['ItemID'] + "|\"")
                    bid = bidsIterator['Bid']
                    bidder = bid['Bidder']
                    f3.write(bidder['UserID'] + "\"|")
                    f3.write(transformDollar(bid['Amount']) + "|\"")
                    f3.write(transformDttm(bid['Time']) + "\"\n")
            
            
            # Users Table - Seller

            
            f4.write("\"" + seller['UserID'] + "\"|")
            f4.write(seller['Rating'] + "|\"")
            f4.write(item['Location'].replace('\"', '\"\"') + "\"|\"")
            f4.write(item['Country'] + "\"\n") 

            # Users Table - Bidder

            if item['Number_of_Bids'] != "0":
                bids = item['Bids']
                for bidsIterator in bids:
                    bid = bidsIterator['Bid']
                    bidder = bid['Bidder']
                    f4.write("\"" + bidder['UserID'] + "\"|")
                    f4.write(bidder['Rating'] + "|\"")
                    try:
                        f4.write(bidder['Location'].replace('\"', '\"\"') + "\"|\"")
                    except KeyError:
                        f4.write("empty" + "\"|\"")
                    try:
                        f4.write(bidder['Country'] + "\"\n")
                    except KeyError:
                        f4.write("empty" + "\"\n")

        f1.close()
        f2.close()
        f3.close()
        f4.close()

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

if __name__ == '__main__':
    main(sys.argv)