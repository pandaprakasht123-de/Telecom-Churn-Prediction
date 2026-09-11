# Databricks notebook source
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

df = spark.table("churn_catalog.gold.customer_features")

# Encode categorical column
contract_indexer = StringIndexer(inputCol="Contract", outputCol="ContractIndex")

# Assemble features
assembler = VectorAssembler(
    inputCols=["tenure", "MonthlyCharges", "ContractIndex"],
    outputCol="features",
    handleInvalid="skip"
)

lr = LogisticRegression(featuresCol="features", labelCol="churn_flag")

pipeline = Pipeline(stages=[contract_indexer, assembler, lr])

train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)
predictions = model.transform(test)

evaluator = BinaryClassificationEvaluator(labelCol="churn_flag")
auc = evaluator.evaluate(predictions)
print(f"AUC: {auc}")

# Save predictions for business team
predictions.select("customerID", "churn_flag", "prediction", "probability") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("churn_catalog.gold.churn_predictions")
