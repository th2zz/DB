-- FOREIGN KEY(Seller_UserID) REFERENCES Users(UserID)
Select * from Items where Seller_UserID not in (Select UserID from Users);

-- FOREIGN KEY(ItemID) REFERENCES Items(ItemID)
Select * from Categories c where c.ItemID not in (Select i.ItemID from Items i);

-- FOREIGN KEY(ItemID) REFERENCES Items(ItemID)
Select * from Bids b where b.ItemID not in (Select i.ItemID from Items i);

-- FOREIGN KEY(UserID) REFERENCES Users(UserID)
Select * from Bids b where b.UserID not in (Select UserID from Users);