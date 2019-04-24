WITH store_sales_summary AS
(SELECT store, type, dept,weekdate, EXTRACT(QUARTER FROM weekdate) AS quarter,EXTRACT(YEAR FROM weekdate) AS year, weeklysales 
FROM (hw2.Sales NATURAL INNER JOIN hw2.Stores)),

type_quarter_year_sales AS
(SELECT type, quarter, year, SUM(weeklysales) FROM store_sales_summary
GROUP BY quarter, year, type
ORDER BY type, year, quarter),

without_lumpsum AS
(SELECT * FROM (
(SELECT year AS yr, quarter AS qtr, sum AS store_a_sales FROM type_quarter_year_sales
WHERE type = 'A') QA
NATURAL INNER JOIN
(SELECT year AS yr, quarter AS qtr, sum AS store_b_sales FROM type_quarter_year_sales
WHERE type = 'B') QB
) QFINAL),

year_lumpsum AS
(SELECT yr, NULL AS "qtr", SUM(store_a_sales) AS store_a_sales, SUM(store_b_sales) AS store_b_sales FROM without_lumpsum
GROUP BY yr)

SELECT * FROM(
(SELECT yr, CAST(without_lumpsum.qtr AS char(1)),store_a_sales,store_b_sales  FROM without_lumpsum) 
UNION 
(SELECT yr, CAST(year_lumpsum."qtr" AS char(1)),store_a_sales,store_b_sales  FROM year_lumpsum) 
) QFINAL
ORDER BY yr, qtr NULLS LAST;

