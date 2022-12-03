/*
  -- Dave Skura, 2022
*/

TRUNCATE TABLE _consume.predictions;

call _consume.predict_week(2);