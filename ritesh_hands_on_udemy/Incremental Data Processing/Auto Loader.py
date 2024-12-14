# Databricks notebook source
# MAGIC %md-sandbox ### Bookstore Dataset
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# list files

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files) 

# COMMAND ----------

# streaming process 

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing New Files

# COMMAND ----------

# To lead the new data files into the streaming process

load_new_data()

# COMMAND ----------

# view newly added files

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files) 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- validate the data
# MAGIC
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Up 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE orders_updates 

# COMMAND ----------

#  checkpoint cleanup

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
