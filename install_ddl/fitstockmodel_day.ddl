/*
  Dave Skura, May,2020

*/


DROP PROCEDURE IF EXISTS fitstockmodel_day(p_stock text,p_log_dt date);

CREATE or replace PROCEDURE fitstockmodel_day(p_stock text,p_log_dt date)
    AS '
BEGIN

		DELETE
		FROM StockLog_dt_factors
		WHERE stock = p_stock and log_dt = p_log_dt;

		INSERT INTO StockLog_dt_factors
		SELECT stock,log_dt
			,extract(hour FROM log_dtm)::integer as hrs
			,extract(minute FROM log_dtm)::integer as mins
			,CASE WHEN extract(hour FROM log_dtm)::integer = 9 and extract(minute FROM log_dtm)::integer = 30 THEN
				open
			 ELSE
				close
			 END as now_trading_at
			,round(((CASE WHEN extract(hour FROM log_dtm)::integer = 9 and extract(minute FROM log_dtm)::integer = 30 THEN
						open
					 ELSE
						close
					 END
					 - avg_trading_at)/avg_trading_at)*100,2) as factor
			,avg_trading_at
		FROM (
				SELECT *
				FROM stockdata_minute
				WHERE stock = p_stock and log_dt = p_log_dt

			) smh 								
			INNER JOIN (
						SELECT stock,log_dt
							,round(avg(
							CASE WHEN extract(hour FROM log_dtm)::integer = 9 and extract(minute FROM log_dtm)::integer = 30 THEN
								open
							 ELSE
								close
							 END 
							),2) as avg_trading_at
						FROM (

							SELECT *
							FROM stockdata_minute
							WHERE stock = p_stock and log_dt = p_log_dt

							) tmd
						WHERE stock = p_stock and log_dt = p_log_dt
						GROUP BY stock,log_dt
			
						) avgtrade USING (Stock,log_dt)
		WHERE smh.stock = p_stock and smh.log_dt = p_log_dt;


		DELETE 
		FROM latest_model_fit
		WHERE stock = p_stock and log_dt = p_log_dt;


		INSERT INTO latest_model_fit
		SELECT stock,log_dt,fit_stock, fit_log_dt,total_variance,avg_trading_at,''day'' as fit_label
		FROM (
			SELECT stock,log_dt,fit_stock, fit_log_dt,total_variance,avg_trading_at
				,rank() OVER (PARTITION BY stock,log_dt ORDER BY total_variance) var_rnk
			FROM (	SELECT SLDF.stock,SLDF.log_dt,MF.stock as fit_stock, MF.log_dt as fit_log_dt,avg_trading_at
						,sum(abs(SLDF.factor-MF.factor)) as total_variance
					FROM StockLog_dt_factors SLDF
						INNER JOIN day_model_factors_day MF USING (hrs,mins)
					WHERE SLDF.stock = p_stock and SLDF.log_dt = p_log_dt
					GROUP BY 1,2,3,4,5
				) modelvars
			WHERE stock = p_stock and log_dt = p_log_dt

			) subqry
		WHERE var_rnk < 6;
   
END;
' 
 LANGUAGE plpgsql;

