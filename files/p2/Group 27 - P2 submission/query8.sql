WITH store_dept_normsales AS
(SELECT store, dept, sales/size AS normsales FROM
(SELECT store, dept, size, sales FROM (
(SELECT store, dept, SUM(weeklySales) AS sales FROM hw2.Sales GROUP BY 1, 2
) V1 NATURAL INNER JOIN hw2.Stores)) Q1)

SELECT dept, sum AS normsales from (
SELECT dept, sum, rank() over(ORDER BY sum DESC) FROM (
SELECT dept, SUM(normsales) FROM store_dept_normsales GROUP BY 1) Q1
) Q2 WHERE rank <= 10;
