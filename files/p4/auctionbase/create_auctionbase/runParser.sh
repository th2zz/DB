python skeleton_parser.py /usr/class/cs145/project/ebay_data/items-*.json

sort -u items.dat -o itemsUnique.dat
sort -u categories.dat -o categoriesUnique.dat
sort -u bids.dat -o bidsUnique.dat
sort -u users.dat -o usersUnique.dat