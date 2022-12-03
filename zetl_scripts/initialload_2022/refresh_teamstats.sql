/*
  -- Dave Skura, 2022
  uses table teamstats2022 
	which is refreshed with python d:\nfl\reload_teamstats2022.py
	which loads data from D:\nfl\data\2022_teamstats.csv 



UPDATE data in D:\nfl\data\2022_teamstats.csv 
RUN python d:\nfl\reload_teamstats2022.py
RUN SQL D:\zetl\zetl_scripts\2022teamstats\refresh_teamstats.sql

*/

DELETE FROM _consume.teamstats
WHERE season = 2022;

INSERT INTO _consume.teamstats(
    seasonweek,
    Season,
    week,
    team,
    gamenumber,
    gamedate,
    opponent,
    WL,
    location,
    teamscore,
    opponentscore
)
SELECT 
    a.seasonweek
    ,a.Season
    ,cast(RIGHT(a.seasonweek,3) as numeric) as week
    ,a.team
    ,a.weekgamenbr as gamenumber
    ,cast(a.gamedate as timestamp)
    ,a.opponent
    ,a.WL
    ,a.location
    ,a.teamscore
    ,a.opponentscore
FROM _technical.teamstats2022 a
UNION
SELECT 
    b.seasonweek
    ,b.Season
    ,cast(RIGHT(b.seasonweek,3) as numeric) as week
    ,b.opponent as team
    ,b.weekgamenbr
    ,cast(b.gamedate as timestamp)
    ,b.team as opponent
    ,CASE WHEN b.WL = 'W' THEN 'L' ELSE 'W' END as WL
    ,'Home' as location
    ,b.opponentscore as teamscore
    ,b.teamscore as opponentscore
FROM _technical.teamstats2022 b
;



