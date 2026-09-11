# Databricks notebook source
from pyspark.sql.functions import col, when, trim, expr, current_timestamp

# COMMAND ----------

df_bronze = spark.table("churn_catalog.bronze.customers_raw")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_silver = (df_bronze
    .withColumn("TotalCharges", expr("try_cast(TotalCharges AS double)"))
    .withColumn("MonthlyCharges", col("MonthlyCharges").cast("double"))
    .withColumn("tenure", col("tenure").cast("int"))
    .withColumn("Churn", when(col("Churn") == "Yes", True).otherwise(False))
    .dropDuplicates(["customerID"])
    .filter(col("customerID").isNotNull())
)

# COMMAND ----------

df_silver = df_silver.withColumn("_ingestion_timestamp", current_timestamp())

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable("churn_catalog.silver.customers_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from churn_catalog.silver.customers_clean