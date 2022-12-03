CREATE TABLE calendar (
	id BIGINT, 
	cal_dt DATE NOT NULL, 
	dayofweek INTEGER, 
	dow CHARACTER VARYING(25) DEFAULT NULL::character varying, 
	dayofmonth INTEGER, 
	dayofyear INTEGER, 
	yearweek INTEGER, 
	year INTEGER, 
	month INTEGER, 
	mth CHARACTER VARYING(25) DEFAULT NULL::character varying, 
	week INTEGER, 
	holiday_can CHARACTER VARYING(75) DEFAULT NULL::character varying, 
	holiday_usa CHARACTER VARYING(75) DEFAULT NULL::character varying, 
	prv52_dt DATE, PRIMARY KEY (cal_dt)
);

COMMENT ON TABLE calendar IS 'Has details on each date from 2000-01-01 to 2022-01-01';

/*
INSERT INTO Calendar (cal_dt,dayofweek,dow,dayofmonth,dayofyear,yearweek,year,month,mth,week,prv52_dt)
SELECT datum as cal_dt,
   EXTRACT(ISODOW FROM datum) AS dayofweek,
    TO_CHAR(datum, 'Day') AS dow,
    EXTRACT(DAY FROM datum) AS dayofmonth,
    EXTRACT(DOY FROM datum) AS dayofyear,
    CAST(concat(TO_CHAR(datum, 'yyyy'),right(concat('0',EXTRACT(week FROM datum)),2)) as integer) AS yearweek,
    EXTRACT(YEAR FROM datum) AS year,
    cast(to_char(datum,'mm') as integer) AS month,
    TO_CHAR(datum, 'Month') AS mth,
    EXTRACT(week FROM datum) AS week,
    (datum  + INTERVAL '-365 day') as prv52_dt

FROM (
    SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
    GROUP BY SEQUENCE.DAY
    ) L
WHERE datum > cast('2000-12-31' as date)
and datum < cast('2022-01-01' as date);


UPDATE Calendar 
SET id = tmp_cal.new_id
	,dayofweek = CASE
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 1 THEN 2
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 2 THEN 3
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 3 THEN 4
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 4 THEN 5
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 5 THEN 6
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 6 THEN 7
					WHEN cast(EXTRACT('ISODOW' from Calendar.cal_dt) as int) = 7 THEN 1
				END
	,dow = TO_CHAR(Calendar.cal_dt, 'Day')
FROM ( 
	SELECT cal_dt,id, RANK() OVER (ORDER BY cal_dt) as new_id
    FROM calendar
	) tmp_cal  
WHERE Calendar.cal_dt = tmp_cal.cal_dt;
*/