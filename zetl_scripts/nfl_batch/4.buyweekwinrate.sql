/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS _consume.buyweekwinrate;
CREATE TABLE _consume.buyweekwinrate as
SELECT 
    team
    ,count(*) as buyweekgames
    ,sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) as wincount
    ,sum(CASE WHEN WL = 'L' THEN 1 ELSE 0 END) as losscount
    ,round(sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) / count(*),2) as afterbuyweekwinrate
    ,concat('(',sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END),'/',count(*),') ',round(sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) / count(*),2)) as afterbuyweekwinrate_expr
FROM (
    SELECT 
        CASE WHEN LAG(opponent,1) OVER (PARTITION BY team ORDER BY week) = 'Bye Week' THEN 'Game After Buy Week' ELSE '' END as buyweekgame
        ,A.*
    FROM _consume.teamstats A
    ORDER BY team,week
    ) L
WHERE buyweekgame = 'Game After Buy Week'
group by team;
    