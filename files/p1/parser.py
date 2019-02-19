
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
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

#Items String and all Item Arributes
sum_allItems = ""
#Users String and all User Arributes
sum_users = ""
#Bids and all Arributes
sum_Bids = ""
sum_Categories = ""


"""
replace all " with "" in the word
adding two " at each end of the word
"""	
def safe_word(word):
	word = word.replace('\"', '\"\"')
	return '\"' + word + '\"'
	

def parseJson(json_file):
	item_attributes = ['ItemID', 'Name', 'Description', 'Number_of_Bids']
	item_time = ['Ends', 'Started']
	#the attribute following is: SellerID | Buy_Price(optional) | Currently
	
	user_arributes = ['UserID', 'Location', 'Country']
	#the attribute following is: Rating
	
	#bid_arribute = ['ItemID', 'UserID', 'Amount', 'Time']
	
	#itemID | category
	
	global sum_Categories, sum_users, sum_allItems, sum_Bids
	Categories = ""
	users = ""
	allItems = ""
	Bids = ""

	with open(json_file, 'r') as f:
		items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
		for item in items:
			#creat category
			if item['Category'] is not None:
				for cate in item['Category']:
					itemID_str = safe_word(item['ItemID'])
					cate_str = safe_word(cate)
					Categories += itemID_str + columnSeparator + cate_str + '\n'
			
			#create Bid
			if item['Bids'] is not None:
				for bids in item['Bids']:
					Bids += safe_word(item['ItemID']) + columnSeparator
					Bids += safe_word(bids['Bid']['Bidder']['UserID']) + columnSeparator
					Bids += safe_word(transformDollar(bids['Bid']['Amount'])) + columnSeparator
					Bids += safe_word(transformDttm(bids['Bid']['Time'])) + '\n'
					
					#add bidder into users
					for index in user_arributes:
						if index in bids['Bid']['Bidder']: 
							users += safe_word(bids['Bid']['Bidder'][index]) + columnSeparator
						else:
							users += 'null' + columnSeparator
					users += safe_word(bids['Bid']['Bidder']['Rating']) + '\n'

				
			#create Users List from Seller
			users += safe_word(item['Seller']['UserID']) + columnSeparator
			if 'Location' in item:
				users += safe_word(item['Location']) + columnSeparator
			else:
				users += 'null' + columnSeparator
			
			if 'Country' in item:
				users += safe_word(item['Country']) + columnSeparator
			else:
				users += 'null' + columnSeparator
			users += safe_word(item['Seller']['Rating']) + '\n'
	
			#create Items List
			for index in item_attributes:
				if item[index] is None:
					allItems += 'null' + columnSeparator
				else:
					allItems += safe_word(item[index]) + columnSeparator
			
			for index in item_time:
				if item[index] is None:
					allItems += 'null' + columnSeparator
				else:
					allItems += safe_word(transformDttm(item[index])) + columnSeparator
			
			allItems += safe_word(item['Seller']['UserID']) + columnSeparator
			
			if ('Buy_Price' in item):
				if item['Buy_Price'] is None:
					allItems += 'null' + columnSeparator
				else:
					allItems += safe_word(transformDollar(item['Buy_Price'])) + columnSeparator
			else:
				allItems += 'null' + columnSeparator
			
			#last item attribute end up with new line
			if item['Currently'] is None:
				allItems += 'null\n'
			else:
				allItems += safe_word(transformDollar(item['Currently'])) + '\n'
	f.close()
	
	sum_users += users
	sum_Bids += Bids
	sum_Categories += Categories
	sum_allItems += allItems
				

	
				
			
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
			print ("Success parsing " + f)
			
	items_file = open('items.dat','w')
	items_file.write(sum_allItems)
	items_file.close()
	
	bids_file = open('bids.dat', 'w')
	bids_file.write(sum_Bids)
	bids_file.close()
	
	user_file = open('users.dat', 'w')
	user_file.write(sum_users)
	user_file.close()
	
	cate_file = open('categories.dat', 'w')
	cate_file.write(sum_Categories)
	cate_file.close()

	

if __name__ == '__main__':
    main(sys.argv)
