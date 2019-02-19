echo "Start======================================================="
sh runParser.sh
sqlite3 AuctionDataBase.db < create.sql
sqlite3 AuctionDataBase.db < load.txt
echo "Testing queries============================================="
answers=(13422 80 8365 1046871451 3130 6717 150)
for i in {1..7}
do
  if [ $(sqlite3 ebay.db < query${i}.sql) != ${answers[$(( $i - 1 ))]} ]
  then
    echo "query $i: failure!"
  else
    echo "query $i: success!"
  fi
done
rm *.dat
