WITH view1 AS
(SELECT  store, dept, sum(weeklysales) as dept_totalsales FROM 
	(SELECT DISTINCT store, dept, weekdate, weeklysales FROM hw2.Sales) AS Q1 GROUP BY store,dept),

view2 AS
(SELECT store, sum(weeklysales) as store_totalsales FROM (SELECT DISTINCT store, dept, weekdate, weeklysales FROM hw2.Sales) AS Q1
GROUP BY store),


store_dept_sales_summary AS 
(SELECT T1.store, T1.dept, T1.dept_totalsales, T1.store_totalsales, T1.dept_totalsales/T1.store_totalsales AS contribution FROM(
	SELECT * FROM(view1 NATURAL INNER JOIN view2)
) AS T1),


satisfiable_dept AS
(SELECT dept FROM (
SELECT dept, contribution FROM  store_dept_sales_summary
WHERE contribution >= 0.05
) AS Q1
GROUP BY dept
HAVING COUNT(contribution) >=3)

SELECT ss.dept, AVG(contribution) FROM store_dept_sales_summary ss, satisfiable_dept sd
WHERE ss.dept = sd.dept
GROUP BY ss.dept;

