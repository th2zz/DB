SELECT DISTINCT Store FROM hw2.TemporalData WHERE UnemploymentRate > 10
EXCEPT
SELECT DISTINCT Store FROM hw2.TemporalData WHERE FuelPrice > 4;