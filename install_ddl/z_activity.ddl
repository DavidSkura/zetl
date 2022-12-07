CREATE TABLE z_activity (
	activity_type varchar(250) Primary key DEFAULT 'Primary', 
	currently varchar(250) DEFAULT 'idle', 
	previously varchar(250) DEFAULT NULL, 
	keyfld varchar(250) DEFAULT '', 
	prvkeyfld varchar(250) DEFAULT '', 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


/*

DELETE FROM z_activity
INSERT INTO z_activity(currently,previously) VALUES ('Running Demo','nothing')

*/