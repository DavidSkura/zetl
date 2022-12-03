/*
  Dave Skura, 2021

	DELETE FROM _zetl.z_etl WHERE etl_name='nfl_batch';

	INSERT INTO _zetl.z_etl (etl_name,stepnum,steptablename,sqlfile) VALUES
	('nfl_batch',1,'_consume.homewinrates','1.homewinrates.ddl'),
	('nfl_batch',2,'_consume.winagainsttypes','2.winagainsttypes.ddl'),
	('nfl_batch',3,'_consume.allteams_buyweekwinrate','3.allteams_buyweekwinrate.sql'),
	('nfl_batch',4,'_consume.buyweekwinrate','4.buyweekwinrate.sql'),
	('nfl_batch',5,'_consume.specialteam_scores','5.specialteam_scores.sql'),
	('nfl_batch',6,'_consume.predictions','6.call_week_predictor.sql')
	
	;

	SELECT *
	FROM _zetl.z_etl
	WHERE etl_name='nfl_batch';



	***********************************
	***********************************

	** 2021 data loaded with Load stage table using import_nfl_data_file_to_stage.py
	** 2021 teamstats updated using /zetl/zetl_scripts/nfl_batch/teamstats.ddl
	** Season table Loaded with 2021 and 2022 schedules
	** procedure predicto created using /ddl/predicto.sql
		eg. call predicto(2022,0.1,1,'Jacksonville Jaguars','Las Vegas Raiders');

	** procedure predict_week created using /ddl/predict_week.sql
		eg. call predict_week(0.1);

	
RUN:

python zetl.py nfl_batch

	1) create homewinrates using /zetl/zetl_scripts/nfl_batch/1.homewinrates.ddl
	2) create winagainsttypes using /zetl/zetl_scripts/nfl_batch/3.winagainsttypes.ddl
	3) create allteams_buyweekwinrate using /ddl/allteams_buyweekwinrate.ddl
	4) create buyweekwinrate using /ddl/buyweekwinrate.ddl
	5) create specialteam_scores using /ddl/specialteam_scores.ddl
	6) create predictions table using /ddl/predictions.ddl





SELECT *
FROM predictions
ORDER BY weekgamenbr

*/




