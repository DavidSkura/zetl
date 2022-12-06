/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS _keydemo.sales_total;

CREATE TABLE _keydemo.sales_total AS
SELECT customer_id,sum(saleamt) as sale_total
FROM _keydemo.sales
GROUP BY customer_id;