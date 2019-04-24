-- description: Constraint 14

PRAGMA foreign_keys = ON;

drop trigger if exists trigger3;

create trigger trigger3
	before insert on Bids
	for each row when (NEW.Amount <= (Select i.Currently from Items i where NEW.ItemID = i.ItemID))
	begin
		SELECT raise(rollback, ‘Trigger3_Failed’);
	end;