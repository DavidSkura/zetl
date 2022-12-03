/*
  -- Dave Skura, 2022
*/

DELETE FROM _consume.specialteam_scores ;
INSERT INTO _consume.specialteam_scores 
SELECT team,sum(specialteams) as specialteam_score
FROM _consume.teamstats
group by team;
