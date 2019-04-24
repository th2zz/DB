WITH holidaysales(store, hol_sales) AS 
(
	SELECT store, SUM(weeklysales) as hol_sales FROM
	(
		(SELECT weekdate FROM hw2.holidays WHERE isholiday = 't') T1 
		NATURAL INNER JOIN 
		(SELECT * FROM hw2.sales) T2
	) AS joinedTableGROUP BY store
) 
SELECT store, hol_sales FROM holidaysales WHERE
hol_sales = (SELECT MIN(hol_sales) FROM holidaysales) OR
hol_sales = (SELECT MAX(hol_sales) FROM holidaysales);