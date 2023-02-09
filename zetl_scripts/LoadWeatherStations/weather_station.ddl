
-- CREATE SCHEMA weather;


DROP TABLE weather.weather_station;
CREATE TABLE weather.weather_station (
	station_id varchar(25) primary key,
	station_name varchar(250),
	province varchar(50),
	webid varchar(50)
);
DROP TABLE weather.station_years ;
CREATE TABLE weather.station_years (
	station_id varchar(25),
	year integer
);
DROP TABLE weather.station_months ;

CREATE TABLE weather.station_months (
	station_id varchar(25),
	month integer
);

TRUNCATE TABLE weather.weather_station;
TRUNCATE TABLE weather.station_years;
TRUNCATE TABLE weather.station_months;

