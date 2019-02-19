drop table if exists Items;
drop table if exists Users;
drop table if exists Bids;
drop table if exists Categories;

create table Users(
	UserID varchar(255),
	Location varchar(255),
	Country varchar(255),
	Rating int,
	PRIMARY KEY(UserID)
);

create table Items(
	ItemID int,
	Name varchar(255),
	Description varchar(255),
	Number_of_Bids int,
	Ends varchar(255),
	Started varchar(255),
	UserID varchar(255),
	Buy_Price varchar(255),
	Currently int,
	PRIMARY KEY(ItemID),
	FOREIGN KEY(UserID) REFERENCES Users
);

create table Bids(
	ItemID int,
	UserID varchar(255),
	Amount int,
	Time varchar(255),
	PRIMARY KEY(UserID, ItemID, Time),
	FOREIGN KEY(UserID) REFERENCES Users,
	FOREIGN KEY(ItemID) REFERENCES Items
);

create table Categories(
	ItemID int,
	Categories varchar(255),
	PRIMARY KEY(ItemID, Categories),
	FOREIGN KEY(ItemID) REFERENCES Items
);
