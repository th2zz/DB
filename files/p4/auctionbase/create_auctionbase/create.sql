DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS Categories;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS CurrentTime;

CREATE TABLE Items (
   ItemID INTEGER,
   Name TEXT,
   Currently REAL,
   First_Bid REAL,
   Buy_Price REAL,
   Number_of_Bids INTEGER,
   Started TEXT,
   Ends TEXT,
   Seller_UserID TEXT,
   Description TEXT,
   PRIMARY KEY(ItemID),
   FOREIGN KEY(Seller_UserID) REFERENCES Users(UserID) DEFERRABLE INITIALLY DEFERRED,
   CHECK (Ends > Started)
);

CREATE TABLE Categories (
   ItemID INTEGER,
   Category TEXT,
   PRIMARY KEY(ItemID, Category),
   FOREIGN KEY(ItemID) REFERENCES Items(ItemID) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE Bids (
   ItemID INTEGER,
   UserID TEXT,
   Amount REAL,
   Time TEXT,
   PRIMARY KEY(ItemID, UserID, Amount),
   UNIQUE(ItemID, Time),
   FOREIGN KEY(ItemID) REFERENCES Items(ItemID) DEFERRABLE INITIALLY DEFERRED,
   FOREIGN KEY(UserID) REFERENCES Users(UserID) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE Users (
   UserID TEXT,
   Rating INTEGER,
   Location TEXT,
   Country TEXT,
   PRIMARY KEY(UserID)
);

CREATE TABLE CurrentTime (
   Time TEXT
);

SELECT Time FROM CurrentTime;

INSERT into CurrentTime values ('2001-12-20 00:00:01');