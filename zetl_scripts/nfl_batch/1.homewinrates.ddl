/*
  -- Dave Skura, 2022
*/
DELETE FROM _consume.homewinrates;

INSERT INTO _consume.homewinrates
SELECT 
    team
    ,concat('(',homewincount,'/',homegamecount,') ',homewinrate) as homewinrate_expr
    ,concat('(',awaywincount,'/',awaygamecount,') ',awaywinrate) as awaywinrate_expr
    ,homewinrate
    ,awaywinrate
FROM (
    SELECT team
        ,max(gamenumber) as gamecount
        ,sum(CASE WHEN location = 'Home' THEN 1.0 ELSE 0 END) as homegamecount
        ,sum(CASE WHEN location = 'Away' THEN 1.0 ELSE 0 END) as awaygamecount
        ,sum(CASE WHEN WL = 'W' AND location = 'Home' THEN 1.0 ELSE 0 END) as homewincount
        ,sum(CASE WHEN WL = 'W' AND location = 'Away' THEN 1.0 ELSE 0 END) as awaywincount
        ,sum(CASE WHEN WL = 'W' THEN 1.0 ELSE 0 END) as wincount
        ,sum(CASE WHEN WL = 'L' THEN 1.0 ELSE 0 END) as losscount
        ,round(sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) / max(gamenumber),2) as winrate
        ,round(sum(CASE WHEN WL = 'W' AND location = 'Home' THEN 1.0 ELSE 0 END) / sum(CASE WHEN location = 'Home' THEN 1.0 ELSE 0 END),2) as homewinrate -- homewins / homegames
        ,round(sum(CASE WHEN WL = 'W' AND location = 'Away' THEN 1.0 ELSE 0 END) / sum(CASE WHEN location = 'Away' THEN 1.0 ELSE 0 END),2) as awaywinrate -- awaywins / awaygames
    FROM _consume.teamstats
    WHERE gamenumber is not null
    group by team
   ) L;