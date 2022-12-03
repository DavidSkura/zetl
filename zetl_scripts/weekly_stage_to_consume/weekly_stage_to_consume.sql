/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS _technical.teamnames;
CREATE TABLE _technical.teamnames as


SELECT distinct A.team as teamcode, B.team
FROM _raw.weekly_data_stage A
    INNER JOIN (SELECT DISTINCT team 
                from _consume.teamstats
                ) B ON (B.team like concat('%',A.team,'%'));

INSERT INTO _technical.teamnames
SELECT distinct A.opponent as teamcode, B.team
FROM _raw.weekly_data_stage A
    INNER JOIN (SELECT DISTINCT team 
                from _consume.teamstats
                ) B ON (B.team like concat('%',A.opponent,'%'));

DELETE FROM _technical.teamnames
WHERE team = 'Washington Football Team';


-- after loading justopponents_wk1

INSERT INTO _technical.teamnames
SELECT distinct A.team as teamcode, B.team
FROM _raw.justopponents_wk1 A
    INNER JOIN (SELECT DISTINCT team 
                from _consume.teamstats
                ) B ON (B.team like concat('%',A.team,'%'));

-- SELECT * FROM _technical.teamnames;
DELETE FROM _consume.teamstats 
WHERE seasonweek = '202201' ;

INSERT INTO _consume.teamstats
SELECT 
    202201 as seasonweek
    ,2022 as season
    ,1 as week
    ,B.team as team
    ,rownumber as gamenumber
    , (CURRENT_DATE - 6) gamedate
    ,opponent
    ,CASE WHEN teampts > opponentpts THEN 'W' ELSE 'L' END as wl
    ,NULL as ot
    ,NULL as running_record
    ,CASE WHEN at = '@' THEN 'Away' ELSE 'Home' END as location
    ,teampts as teamscore
    ,opponentpts as opponentscore
    ,tyards as teamyards
    ,tpassyards as teampassyards
    ,trushyards as teamrushyards
    ,tturnovers as teamturnover
    ,tfirstdowns as teamfirstdowns
    ,oyards as opponentyards
    ,opassyards as opponentpassyards
    ,orushyards as opponentrushyards
    ,ournovers as opponentturnover
    ,ofirstdowns as opponentfirstdowns
    ,offense
    ,defense
    ,sptms as specialteams
FROM _raw.weekly_data_stage A
	INNER JOIN _technical.teamnames B ON (A.team=B.teamcode)
union
SELECT 
    202201 as seasonweek
    ,2022 as season
    ,1 as week
    ,opponent as team
    ,rownumber as gamenumber
    , (CURRENT_DATE - 6) gamedate
    ,B.team  as opponent
    ,CASE WHEN opponentpts > teampts  THEN 'W' ELSE 'L' END as wl
    ,NULL as ot
    ,NULL as running_record
    ,CASE WHEN at = '@' THEN 'Home' ELSE 'Away' END as location

	,opponentpts as teamscore
    ,teampts as opponentscore

    ,oyards as teamyards
    ,opassyards as teampassyards
    ,orushyards as teamrushyards
    ,ournovers as teamturnover
    ,ofirstdowns as teamfirstdowns

    ,tyards as opponentyards
    ,tpassyards as opponentpassyards
    ,trushyards as opponentrushyards
    ,tturnovers as opponentturnover
    ,tfirstdowns as opponentfirstdowns
    ,defense AS offense
    ,offense AS defense
    ,sptms * -1 as specialteams
FROM _raw.weekly_data_stage A
	INNER JOIN _technical.teamnames B ON (A.team=B.teamcode)