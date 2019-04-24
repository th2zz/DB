
WITH MONTHTYPESUMTABLE AS
(SELECT extract(month from weekdate) as months, type, SUM(weeklysales)
FROM hw2.Sales 
INNER JOIN hw2.Stores ON hw2.Stores.store = hw2.Sales.store
GROUP BY months, type
ORDER BY type, months),

SUMTYPETABLE AS 
(SELECT type, SUM(sum) FROM MONTHTYPESUMTABLE
GROUP BY type)

SELECT T1.months, T1.type, T1.sum, T1.sum*100/T2.sum as "%contribution" FROM
MONTHTYPESUMTABLE T1, SUMTYPETABLE T2 WHERE T1.type = T2.type;


