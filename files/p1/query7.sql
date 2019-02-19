SELECT COUNT(*) FROM (
	SELECT *
    FROM categories AS "cate", bids AS "bid"
    WHERE cate.itemID = bid.itemID AND bid.amount > 100.0
    GROUP BY cate.categories
    HAVING COUNT(*) >= 1
);