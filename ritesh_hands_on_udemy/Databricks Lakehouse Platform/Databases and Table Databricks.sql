-- Databricks notebook source
SELECT 'Welcome to Databricks Database' AS message

-- COMMAND ----------

USE CATALOG hive_metastore;

-- COMMAND ----------

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE TABLE managed_default;

-- COMMAND ----------

SELECT * FROM managed_default;

-- COMMAND ----------

-- metadata informations
DESCRIBE EXTENDED managed_default

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

-- metadata informations
DESCRIBE EXTENDED external_default;

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- not exists
-- %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'
-- MAGIC

-- COMMAND ----------

-- new database
CREATE SCHEMA new_default


-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default


-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);


-- COMMAND ----------

-- DESCRIBE EXTENDED managed_new_default


-- COMMAND ----------

-- DESCRIBE EXTENDED external_new_default


-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- Creating schema in custom location 
CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE SCHEMA custom;

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'
