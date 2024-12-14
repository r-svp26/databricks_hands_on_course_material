# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- database path - not valid 
# MAGIC
# MAGIC SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### orders_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
# MAGIC COMMENT "The raw books orders, ingested from orders-raw"
# MAGIC AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
# MAGIC                              map("cloudFiles.inferColumnTypes", "true"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC COMMENT "The customers lookup table, ingested from customers-json"
# MAGIC AS SELECT * FROM json.`${datasets.path}/customers-json`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Silver Layer Tables
# MAGIC
# MAGIC #### orders_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
# MAGIC   CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "The cleaned books orders with valid order_id"
# MAGIC AS
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
# MAGIC          c.profile:address:country as country
# MAGIC   FROM STREAM(LIVE.orders_raw) o
# MAGIC   LEFT JOIN LIVE.customers c
# MAGIC     ON o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC >> Constraint violation
# MAGIC
# MAGIC | **`ON VIOLATION`** | Behavior |
# MAGIC | --- | --- |
# MAGIC | **`DROP ROW`** | Discard records that violate constraints |
# MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
# MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
# MAGIC COMMENT "Daily number of books per customer in China"
# MAGIC AS
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM LIVE.orders_cleaned
# MAGIC   WHERE country = "China"
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

# COMMAND ----------


