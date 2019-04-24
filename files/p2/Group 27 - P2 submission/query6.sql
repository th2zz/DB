WITH ROW_TEMP AS 
(SELECT CAST(Q4.attribute AS varchar(20)), Q4.corr_sign, Q4.correlation FROM (
(SELECT 'Temperature' AS attribute) AS Q1
	CROSS JOIN 
(SELECT (CASE WHEN Q3.correlation < 0 THEN '-' ELSE '+' END) AS corr_sign FROM
(SELECT CORR(Q2.temperature, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS Q3) AS QSIGN 
	CROSS JOIN
(SELECT CORR(Q2.temperature, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS QCORR 
) AS Q4),


ROW_FP AS 
(SELECT CAST(Q4.attribute AS varchar(20)), Q4.corr_sign, Q4.correlation FROM (
(SELECT 'FuelPrice' AS attribute) AS Q1
	CROSS JOIN 
(SELECT (CASE WHEN Q3.correlation < 0 THEN '-' ELSE '+' END) AS corr_sign FROM
(SELECT CORR(Q2.fuelprice, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS Q3) AS QSIGN 
	CROSS JOIN
(SELECT CORR(Q2.fuelprice, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS QCORR 
) AS Q4),


ROW_CPI AS 
(SELECT CAST(Q4.attribute AS varchar(20)), Q4.corr_sign, Q4.correlation FROM (
(SELECT 'CPI' AS attribute) AS Q1
	CROSS JOIN 
(SELECT (CASE WHEN Q3.correlation < 0 THEN '-' ELSE '+' END) AS corr_sign FROM
(SELECT CORR(Q2.CPI, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS Q3) AS QSIGN 
	CROSS JOIN
(SELECT CORR(Q2.CPI, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS QCORR 
) AS Q4),


ROW_UR AS 
(SELECT CAST(Q4.attribute AS varchar(20)), Q4.corr_sign, Q4.correlation FROM (
(SELECT 'UnemploymentRate' AS attribute) AS Q1
	CROSS JOIN 
(SELECT (CASE WHEN Q3.correlation < 0 THEN '-' ELSE '+' END) AS corr_sign FROM
(SELECT CORR(Q2.unemploymentrate, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS Q3) AS QSIGN 
	CROSS JOIN
(SELECT CORR(Q2.unemploymentrate, Q2.weeklysales) AS correlation FROM (SELECT * FROM hw2.Sales NATURAL INNER JOIN hw2.TemporalData) AS Q2) AS QCORR 
) AS Q4)

/*CONSTRUCT Final result*/
SELECT * FROM(

	((SELECT * FROM ROW_TEMP)
	UNION 
	(SELECT * FROM ROW_FP))
	UNION
	((SELECT * FROM ROW_CPI)
	UNION 
	(SELECT * FROM ROW_UR))
	
) QFINAL
ORDER BY
	CASE 
		WHEN attribute = 'Temperature' THEN 1
		WHEN attribute = 'FuelPrice' THEN 2
		WHEN attribute = 'CPI' THEN 3
		ELSE 4
	END
;

