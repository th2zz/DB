SELECT COUNT(*) FROM (
	SELECT * FROM items INNER JOIN users on items.userID = users.userID 
	WHERE rating > 1000 GROUP BY items.userID
);