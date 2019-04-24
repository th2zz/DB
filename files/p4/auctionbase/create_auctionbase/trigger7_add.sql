-- description: Constraint 11

PRAGMA foreign_keys = ON;

drop trigger if exists trigger7;

create trigger trigger7
	before insert on Bids
	for each row when (NEW.UserID = (Select i.Seller_UserID from Items i where NEW.ItemID = i.ItemID))
	begin
		SELECT raise(rollback, ‘Trigger7_Failed’);
	end;