rm *.dat
python parser.py ebay_data/items-*.json
sort -t '|' -u -k 1,1 items.dat | uniq > itemsOut.dat
sort -t '|' -k 1,1 users.dat | uniq | awk -F "|" '{if ($1 != prev){print $0}; prev=$1}' > usersOut.dat
sort bids.dat | uniq > bidsOut.dat
sort categories.dat | uniq > categoriesOut.dat



