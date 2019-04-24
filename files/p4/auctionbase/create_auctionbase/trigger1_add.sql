-- description: Constraint 16

PRAGMA foreign_keys = ON;

drop trigger if exists trigger1;

create trigger trigger1
	before update of Time on CurrentTime
	for each row when (NEW.Time <= OLD.Time)
	begin
		SELECT raise(rollback, ‘Trigger1_Failed’);
	end;