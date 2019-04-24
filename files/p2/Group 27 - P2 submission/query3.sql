WITH holidaystable AS
(SELECT * FROM (
(SELECT weekdate FROM hw2.holidays WHERE isholiday = 't') AS T1
NATURAL INNER JOIN 
(SELECT * FROM hw2.sales) AS T2)),

nonholidaystable AS
(SELECT * FROM (
(SELECT weekdate FROM hw2.holidays WHERE isholiday = 'f') AS T1
NATURAL INNER JOIN 
(SELECT * FROM hw2.sales) AS T2)),

holidaysales AS
(SELECT weekdate, SUM(weeklysales) as total_sales FROM holidaystable GROUP BY weekdate),

nonholidaysales AS
(SELECT weekdate, SUM(weeklysales) as total_sales FROM nonholidaystable GROUP BY weekdate)

SELECT COUNT(*) FROM nonholidaysales n
  WHERE n.total_sales > (SELECT AVG(total_sales) FROM holidaysales);
