-- description: Constraint 11

PRAGMA foreign_keys = ON;

drop trigger if exists trigger6;

create trigger trigger6
	before insert on Bids
	for each row when (NEW.Time > (Select i.Ends from Items i where NEW.ItemID = i.ItemID))
	begin
		SELECT raise(rollback, ‘Trigger6_Failed’);
	end;