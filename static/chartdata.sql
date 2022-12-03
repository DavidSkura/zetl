
		WITH CTE as (
					SELECT var_rnk,stock,log_dt,fit_stock,fit_log_dt,model_name
					FROM (
							SELECT stock,log_dt,fit_stock,fit_log_dt
									,concat(fit_stock,':',fit_log_dt,':',total_variance) as model_name
									,RANK() OVER (PARTITION BY Stock ORDER BY total_variance,fit_stock,fit_log_dt ) var_rnk
									
							FROM latest_model_fit
							WHERE stock='ibm' and log_dt='2021-04-23' 
							) L
					WHERE var_rnk < 4
					ORDER BY var_rnk    

				)

				SELECT 
						round((hrs::numeric + mins::numeric/100),2) as time
						,round(avg_trading_at_1value + (SDF.factor/100 * avg_trading_at_1value),2) as "IBM avg()=142.56"
						,round(avg_trading_at_1value + (DM1.factor/100 * avg_trading_at_1value),2) as "db:2021-02-22:2.58"
						,round(avg_trading_at_1value + (DM2.factor/100 * avg_trading_at_1value),2) as "rs:2021-03-01:2.70"
						,round(avg_trading_at_1value + (DM3.factor/100 * avg_trading_at_1value),2) as "stor:2021-02-22:2.80"

				FROM 
						day_model_factors_day DM1
						LEFT JOIN (
												SELECT *
												FROM StockLog_dt_factors
												WHERE stock= 'ibm' AND log_dt= '2021-04-23'                     
						 AND  hrs::integer < extract(hour FROM CURRENT_TIMESTAMP)::integer 
		
										 ) SDF USING (hrs,mins)

						INNER JOIN CTE CTE1 ON (DM1.stock = CTE1.fit_stock AND DM1.log_dt = CTE1.fit_log_dt AND CTE1.var_rnk=1)
						
						INNER JOIN day_model_factors_day DM2 USING (hrs,mins)
						INNER JOIN CTE CTE2 ON (DM2.stock = CTE2.fit_stock AND DM2.log_dt = CTE2.fit_log_dt AND CTE2.var_rnk=2)
					
						INNER JOIN day_model_factors_day DM3 USING (hrs,mins)
						INNER JOIN CTE CTE3 ON (DM3.stock = CTE3.fit_stock AND DM3.log_dt = CTE3.fit_log_dt AND CTE3.var_rnk=3)
						,(  SELECT max(avg_trading_at) as avg_trading_at_1value
                            FROM StockLog_dt_factors
                            WHERE stock= 'ibm' AND log_dt= '2021-04-23'                     
        						  ) avgt							
				ORDER BY time
		