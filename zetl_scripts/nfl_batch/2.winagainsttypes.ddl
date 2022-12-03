DROP TABLE IF EXISTS _consume.winagainsttypes;

CREATE TABLE _consume.winagainsttypes as 
SELECT
  team
    ,strgpass_winrate
    ,balanced_winrate
    ,strgrush_winrate
    ,concat('(',strgpass_wins,'/',gmesagainst_strgpass,') ',strgpass_winrate) as strgpass_winrate_expr
    ,concat('(',balanced_wins,'/',gmesagainst_balanced,') ',balanced_winrate) as balanced_winrate_expr
    ,concat('(',strgrush_wins,'/',gmesagainst_strgrush,') ',strgrush_winrate) as strgrush_winrate_expr
FROM (    
    SELECT    
        team
       ,gamecount
       ,wincount
       ,losscount
       ,gmesagainst_strgpass
       ,gmesagainst_balanced
       ,gmesagainst_strgrush
       ,strgpass_wins
       ,balanced_wins
       ,strgrush_wins
       ,CASE WHEN gmesagainst_strgpass = 0 THEN 0 ELSE round((cast(strgpass_wins as numeric(5,1)) / cast(gmesagainst_strgpass as numeric(5,1))),2) END as strgpass_winrate
       ,CASE WHEN gmesagainst_balanced = 0 THEN 0 ELSE round((cast(balanced_wins as numeric(5,1)) / cast(gmesagainst_balanced as numeric(5,1))),2) END as balanced_winrate
       ,CASE WHEN gmesagainst_strgrush = 0 THEN 0 ELSE round((cast(strgrush_wins as numeric(5,1)) / cast(gmesagainst_strgrush as numeric(5,1))),2) END as strgrush_winrate
    FROM (
        SELECT    
            ts.team
            ,max(gamenumber) as gamecount
            ,sum(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) as wincount
            ,sum(CASE WHEN WL = 'L' THEN 1 ELSE 0 END) as losscount
            ,sum(CASE WHEN TeamType = 'Strong Passing' THEN 1 ELSE 0 END) as gmesagainst_strgpass
            ,sum(CASE WHEN TeamType = 'Balanced' THEN 1 ELSE 0 END) as gmesagainst_balanced
            ,sum(CASE WHEN TeamType = 'Strong Rushing' THEN 1 ELSE 0 END) as gmesagainst_strgrush
            ,sum(CASE WHEN WL = 'W' AND TeamType = 'Strong Passing' THEN 1 ELSE 0 END) as strgpass_wins
            ,sum(CASE WHEN WL = 'W' AND TeamType = 'Balanced' THEN 1 ELSE 0 END) as balanced_wins
            ,sum(CASE WHEN WL = 'W' AND TeamType = 'Strong Rushing' THEN 1 ELSE 0 END) as strgrush_wins
            
           FROM _consume.teamstats ts
            LEFT JOIN _consume.dim_teamtypes opponent_type ON (  ts.opponent = opponent_type.team AND
                                                        cast(ts.seasonweek as numeric) BETWEEN opponent_type.effective  and opponent_type.expire)
        WHERE gamenumber is not null
        group by ts.team
        ) L
    ) M