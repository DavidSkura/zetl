/*
  -- Dave Skura, 2022

  -- base on season_stage table
  -- which is loaded by running
		python d:\nfl\import_NFL2022Season.py
*/

DROP TABLE IF EXISTS _technical.teamstats2022;
CREATE TABLE _technical.teamstats2022 as 
SELECT 
	concat(2022,right(concat('0',coalesce(0.1,00)),4)) as seasonweek

    ,2022 as Season

    ,awayteam  as team
    ,RANK() OVER (PARTITION BY week ORDER BY rownumber) as weekgamenbr
    ,NULL as gametime        
    ,NULL as gamedate

    ,hometeam as opponent
    ,CASE WHEN awaypts > homepts THEN 'W' ELSE 'L' END as WL
    ,'' as OT
    ,'' as  running_record
    ,CASE WHEN atfld = '@' THEN 'Away' ELSE 'Home' END as location
    ,cast(awaypts as int) as teamscore
    ,cast(homepts as int) as opponentscore
    
    ,'' as teamyards
    ,'' as teampassyards
    ,'' as teamrushyards
    ,'' as teamturnover
    ,'' as teamfirstdowns

    ,'' as opponentyards
    ,'' as opponentpassyards
    ,'' as opponentrushyards
    ,'' as opponentturnover
    ,'' as opponentfirstdowns
    
    ,'' as offense
    ,'' as defense
    ,'' as specialteams

FROM _raw.season_stage A 
WHERE week = 'Pre0';

INSERT INTO _technical.teamstats2022 
SELECT 
	concat(2022,right(concat('0',coalesce(0.2,00)),4)) as seasonweek

    ,2022 as Season

    ,awayteam  as team
    ,RANK() OVER (PARTITION BY week ORDER BY rownumber) as weekgamenbr
    ,NULL as gametime        
    ,NULL as gamedate

    ,hometeam as opponent
    ,CASE WHEN awaypts > homepts THEN 'W' ELSE 'L' END as WL
    ,'' as OT
    ,'' as  running_record
    ,CASE WHEN atfld = '@' THEN 'Away' ELSE 'Home' END as location
    ,cast(awaypts as int) as teamscore
    ,cast(homepts as int) as opponentscore
    
    ,'' as teamyards
    ,'' as teampassyards
    ,'' as teamrushyards
    ,'' as teamturnover
    ,'' as teamfirstdowns

    ,'' as opponentyards
    ,'' as opponentpassyards
    ,'' as opponentrushyards
    ,'' as opponentturnover
    ,'' as opponentfirstdowns
    
    ,'' as offense
    ,'' as defense
    ,'' as specialteams

FROM _raw.season_stage A 
WHERE week = 'Pre1';


INSERT INTO _technical.teamstats2022 
SELECT 
	concat(2022,right(concat('0',coalesce(0.3,00)),4)) as seasonweek

    ,2022 as Season

    ,awayteam  as team
    ,RANK() OVER (PARTITION BY week ORDER BY rownumber) as weekgamenbr
    ,NULL as gametime        
    ,NULL as gamedate

    ,hometeam as opponent
    ,CASE WHEN awaypts > homepts THEN 'W' ELSE 'L' END as WL
    ,'' as OT
    ,'' as  running_record
    ,CASE WHEN atfld = '@' THEN 'Away' ELSE 'Home' END as location
    ,cast(awaypts as int) as teamscore
    ,cast(homepts as int) as opponentscore
    
    ,'' as teamyards
    ,'' as teampassyards
    ,'' as teamrushyards
    ,'' as teamturnover
    ,'' as teamfirstdowns

    ,'' as opponentyards
    ,'' as opponentpassyards
    ,'' as opponentrushyards
    ,'' as opponentturnover
    ,'' as opponentfirstdowns
    
    ,'' as offense
    ,'' as defense
    ,'' as specialteams

FROM _raw.season_stage A 
WHERE week = 'Pre2';

INSERT INTO _technical.teamstats2022 
SELECT 
	concat(2022,right(concat('0',coalesce(0.4,00)),4)) as seasonweek

    ,2022 as Season

    ,awayteam  as team
    ,RANK() OVER (PARTITION BY week ORDER BY rownumber) as weekgamenbr
    ,NULL as gametime        
    ,NULL as gamedate

    ,hometeam as opponent
    ,CASE WHEN awaypts > homepts THEN 'W' ELSE 'L' END as WL
    ,'' as OT
    ,'' as  running_record
    ,CASE WHEN atfld = '@' THEN 'Away' ELSE 'Home' END as location
    ,cast(awaypts as int) as teamscore
    ,cast(homepts as int) as opponentscore
    
    ,'' as teamyards
    ,'' as teampassyards
    ,'' as teamrushyards
    ,'' as teamturnover
    ,'' as teamfirstdowns

    ,'' as opponentyards
    ,'' as opponentpassyards
    ,'' as opponentrushyards
    ,'' as opponentturnover
    ,'' as opponentfirstdowns
    
    ,'' as offense
    ,'' as defense
    ,'' as specialteams

FROM _raw.season_stage A 
WHERE week = 'Pre3';

