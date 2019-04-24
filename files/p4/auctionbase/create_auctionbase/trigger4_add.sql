-- description: Constraint 13

PRAGMA foreign_keys = ON;

drop trigger if exists trigger4;

create trigger trigger4
	after insert on Bids
	for each row
	begin
		UPDATE Items SET Number_of_Bids = Number_of_Bids + 1 WHERE New.ItemID = Items.ItemID;
	end;