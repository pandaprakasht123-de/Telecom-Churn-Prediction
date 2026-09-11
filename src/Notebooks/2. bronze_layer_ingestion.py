# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = spark.read.csv('sttm_source_data.csv', header= True)

# COMMAND ----------

df = df.withColumn("_ingestion_timestamp", current_timestamp())

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("churn_catalog.bronze.customers_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from churn_catalog.bronze.customers_raw