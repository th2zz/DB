-- description: Constraint 11

PRAGMA foreign_keys = ON;

drop trigger if exists trigger8;

create trigger trigger8
	after insert on Bids
	for each row
	begin
		UPDATE Items SET Currently = New.Amount;
	end;