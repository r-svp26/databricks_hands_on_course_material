-- Databricks notebook source
SHOW TABLES;

-- while running this in other spark sessions; all the temp views will not display 

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * 
FROM global_temp.global_temp_view_latest_phones; 

-- Note: It'll exists until cluster is running. 
