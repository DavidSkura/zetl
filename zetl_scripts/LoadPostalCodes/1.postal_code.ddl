
-- CREATE SCHEMA weather;

DROP TABLE IF EXISTS weather.postal_codes;
CREATE TABLE weather.postal_codes(
	PostalCode varchar(10),
	FSA varchar(5),
	Latitude float,
	Longitude float,
	Place varchar(250),
	FSA1 varchar(10),
	FSAProvince varchar(10),
	AreaType varchar(50)
);

SELECT count(*)
FROM weather.postal_codes;
