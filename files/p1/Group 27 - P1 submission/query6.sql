SELECT COUNT(*) FROM (
    SELECT *
    FROM items INNER JOIN bids ON items.userID = bids.userID
    GROUP BY items.userID
);