CREATE TABLE _zetl.activity (
	currently varchar(250) DEFAULT 'idle', 
	previously varchar(250) DEFAULT NULL, 
	keyfld varchar(250) DEFAULT '', 
	prvkeyfld varchar(250) DEFAULT '', 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
