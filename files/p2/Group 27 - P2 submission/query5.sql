WITH storedeptyearsmonthscount AS
(SELECT store, dept, years, COUNT(months) as months_count FROM (
SELECT DISTINCT store, dept, extract(month from weekdate) AS months, extract(year from weekdate) AS years FROM hw2.Sales
) T1 WHERE years=2010 OR years=2011 OR years=2012
GROUP BY store, dept, years HAVING COUNT(months) = 12
ORDER BY store, dept, years, months_count),

storedeptcount AS
(SELECT store, COUNT(dept) as dept_count FROM
(SELECT DISTINCT store, dept FROM hw2.Sales) T1
GROUP BY store)

SELECT store FROM storedeptcount s2 WHERE 
(SELECT COUNT(*) FROM storedeptyearsmonthscount s1 WHERE s1.store=s2.store) = dept_count;
