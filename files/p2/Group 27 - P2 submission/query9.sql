WITH store_dept_normsales AS
(SELECT store, dept, sales/size AS normsales FROM
(SELECT store, dept, size, sales FROM (
(SELECT store, dept, SUM(weeklySales) AS sales FROM hw2.Sales GROUP BY 1, 2
) V1 NATURAL INNER JOIN hw2.Stores)) Q1),

Q8FINAL AS
(SELECT dept, sum AS normsales from (
SELECT dept, sum, rank() over(ORDER BY sum DESC) FROM (
SELECT dept, SUM(normsales) FROM store_dept_normsales GROUP BY 1) Q1
) Q2 WHERE rank <= 10),


TEMP AS
(SELECT dept, yr, mo, sum(weeklySales) AS monthlysales FROM(
SELECT store, dept, extract(year from weekdate) AS yr, extract(month from weekdate) AS mo, weeklySales
FROM hw2.Sales WHERE dept = ANY(SELECT dept FROM Q8FINAL)) Q1
GROUP BY 1,2,3)

SELECT dept, yr, mo, monthlysales, cast(contribution as decimal(5,2)), cast(cumulative_sales as decimal(20,2)) FROM(
SELECT dept, yr, mo, monthlysales, 100 * monthlysales/totalsales AS contribution, cumulative_sales FROM (
SELECT dept, yr, mo, monthlysales, totalsales, cumulative_sales FROM
((SELECT dept, SUM(monthlysales) AS totalsales FROM TEMP GROUP BY 1)Q2
NATURAL INNER JOIN
(SELECT dept, yr, mo, monthlysales, SUM(monthlysales) OVER (PARTITION BY dept ORDER BY yr, mo) AS cumulative_sales FROM
TEMP) Q1) Q3) Q4) QFINAL ORDER BY 1,2,3;

