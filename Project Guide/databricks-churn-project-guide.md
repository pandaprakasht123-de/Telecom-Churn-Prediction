# Beginner Project: Telecom Customer Churn Prediction
### Databricks Free Edition + Spark + Medallion Architecture + ML

---

## 1. Business Problem

A telecom company is losing customers every month (churn) but doesn't know **who** is likely to leave or **why**. Marketing spends money on retention offers randomly instead of targeting at-risk customers. This wastes budget and doesn't reduce churn.

**Problem statement:** "The business needs a way to identify customers likely to churn in the next cycle, so the retention team can proactively target them with offers, reducing revenue loss."

---

## 2. Solution Approach

Build an end-to-end pipeline on Databricks that:

1. Ingests raw customer data (Bronze layer)
2. Cleans and standardizes it (Silver layer)
3. Builds business-ready aggregated/feature tables (Gold layer)
4. Trains a simple ML classification model to predict churn (Yes/No)
5. Outputs predictions to a table the business team can query/report on

**Tech stack:** Databricks Free Edition, PySpark, Delta Lake, MLflow (optional), scikit-learn or Spark MLlib (Logistic Regression / Random Forest).

**Dataset:** Use the public "Telco Customer Churn" dataset (Kaggle, ~7,000 rows, one CSV). It's small, free, and perfect for a beginner project. Columns include customerID, tenure, contract type, monthly charges, total charges, internet service, payment method, and a `Churn` (Yes/No) label.

---

## 3. Step-by-Step: How to Start

### Step 1 — Environment setup
- Sign up for **Databricks Free Edition** (community/free tier).
- Create a new **Workspace** and a **Serverless/Free cluster**.
- Create a folder structure: `/Workspace/churn_project/notebooks`.

### Step 2 — Get the data
- Download the Telco Customer Churn CSV from Kaggle.
- Upload it via **Catalog > Add Data > Upload File**, or drop it into a Unity Catalog Volume (e.g., `/Volumes/main/churn_project/raw_data/`).

### Step 3 — Create catalog/schema structure
```sql
CREATE CATALOG IF NOT EXISTS churn_catalog;
CREATE SCHEMA IF NOT EXISTS churn_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS churn_catalog.silver;
CREATE SCHEMA IF NOT EXISTS churn_catalog.gold;
```

### Step 4 — Build Bronze notebook
Read raw CSV as-is, add metadata columns, write as Delta table (no transformation logic).

### Step 5 — Build Silver notebook
Clean data: handle nulls, fix data types, remove duplicates, standardize text (Yes/No, categories), apply business rules.

### Step 6 — Build Gold notebook
Create aggregated/feature tables ready for reporting and ML — e.g., churn rate by contract type, tenure buckets, average charges.

### Step 7 — Build ML notebook
Train a classification model on the Gold feature table to predict churn. Evaluate with accuracy/precision/recall. Save predictions to a Gold "predictions" table.

### Step 8 — Document everything
Write the BRD and STTM (below) — this is what separates a "notebook exercise" from a real project you can show in interviews.

### Step 9 — Optional: Dashboard
Use Databricks SQL to build a simple dashboard on the Gold layer (churn rate by segment, high-risk customer list).

---

## 4. BRD (Business Requirement Document) — Preparation Steps

A BRD explains the **business need**, not the technical solution. Structure it like this:

| Section | What to Write |
|---|---|
| **Project Title** | Customer Churn Prediction for Retention Targeting |
| **Business Objective** | Reduce customer churn by identifying at-risk customers early |
| **Stakeholders** | Marketing/Retention team, Data team, Business Sponsor |
| **Scope** | In scope: churn prediction using historical customer data. Out of scope: real-time streaming, automated offer generation |
| **Current Process (As-Is)** | No predictive process exists; retention offers sent broadly/manually |
| **Proposed Process (To-Be)** | Automated pipeline scores each customer with a churn probability weekly/monthly |
| **Functional Requirements** | FR1: Ingest customer data daily. FR2: Clean and validate data. FR3: Generate churn probability score per customer. FR4: Provide list of top N at-risk customers to retention team |
| **Non-Functional Requirements** | Data refresh frequency, data retention period, access control (who can see customer PII) |
| **Success Criteria / KPIs** | Model precision/recall thresholds, % reduction in churn after 3 months of using the tool |
| **Assumptions & Constraints** | Historical data covers at least 12 months; free-tier compute limits apply |

**How to prepare it in practice:**
1. Interview/imagine a "business stakeholder" (retention manager) — what do they need to know?
2. Write objective in plain English, no technical jargon.
3. List functional requirements as numbered, testable statements (FR1, FR2...).
4. Define what "success" looks like in measurable terms.
5. Get sign-off (in a real job) before technical design starts.

---

## 5. STTM (Source-to-Target Mapping) — Preparation Steps

STTM is the technical bridge between BRD and code — it maps **every source field to where it lands and how it's transformed**, layer by layer.

**How to prepare it:**
1. List every source column and its data type.
2. For each layer (Bronze → Silver → Gold), decide: keep as-is, transform, drop, or derive new column.
3. Write the transformation logic in plain language (this becomes your Silver/Gold notebook code).
4. Review with a "business" or "QA" lens: does the mapping satisfy the BRD's functional requirements?

**Sample STTM table (abbreviated):**

| Source Column | Bronze | Silver | Gold | Transformation Logic |
|---|---|---|---|---|
| customerID | customerID (string) | customerID (string, PK) | customerID | No change, used as unique key |
| tenure | tenure (string) | tenure (int) | tenure_bucket (string) | Cast to int in Silver; bucket into "0-12mo", "13-24mo", "25mo+" in Gold |
| MonthlyCharges | MonthlyCharges (string) | MonthlyCharges (double) | avg_monthly_charges | Cast to double; aggregated by segment in Gold |
| TotalCharges | TotalCharges (string, has blanks) | TotalCharges (double, nulls handled) | — | Cast to double; replace blank/invalid with null, then impute or drop |
| Contract | Contract (string) | Contract (string, standardized) | contract_type | Trim/standardize casing (e.g., "month-to-month" → "Month-to-Month") |
| Churn | Churn (string "Yes"/"No") | Churn (boolean) | churn_flag (0/1) | Convert Yes/No to boolean, then 1/0 for ML label |
| _ingestion_date | added in Bronze only | — | — | Technical metadata column, not from source |

---

## 6. Business Logic Per Layer

### 🥉 Bronze Layer — "Raw, as-is"
- **Purpose:** Preserve source data exactly as received, for traceability/auditing.
- **Logic:** No transformations. Just add metadata (`_ingestion_timestamp`, `_source_file_name`).
- **Example (PySpark):**
```python
df = spark.read.option("header", True).csv("/Volumes/main/churn_project/raw_data/telco_churn.csv")
df = df.withColumn("_ingestion_timestamp", current_timestamp())
df.write.format("delta").mode("overwrite").saveAsTable("churn_catalog.bronze.customers_raw")
```

### 🥈 Silver Layer — "Clean, validated, standardized"
- **Purpose:** Fix data quality issues so the data is trustworthy and consistently typed.
- **Logic:**
  - Cast `TotalCharges`, `MonthlyCharges` to double; handle blank strings as null
  - Cast `tenure` to integer
  - Drop exact duplicate `customerID` rows
  - Standardize categorical text (trim spaces, consistent casing)
  - Convert `Churn` Yes/No to boolean
  - Filter out records with null `customerID` (invalid records)
- **Example:**
```python
from pyspark.sql.functions import col, when, trim

df_bronze = spark.table("churn_catalog.bronze.customers_raw")

df_silver = (df_bronze
    .withColumn("TotalCharges", when(trim(col("TotalCharges")) == "", None)
                .otherwise(col("TotalCharges").cast("double")))
    .withColumn("MonthlyCharges", col("MonthlyCharges").cast("double"))
    .withColumn("tenure", col("tenure").cast("int"))
    .withColumn("Churn", when(col("Churn") == "Yes", True).otherwise(False))
    .dropDuplicates(["customerID"])
    .filter(col("customerID").isNotNull())
)
df_silver.write.format("delta").mode("overwrite").saveAsTable("churn_catalog.silver.customers_clean")
```

### 🥇 Gold Layer — "Business-ready, aggregated, feature-engineered"
- **Purpose:** Data shaped for reporting AND ready as ML input features.
- **Logic:**
  - Bucket `tenure` into groups
  - Create `churn_flag` (0/1) for ML label
  - One-hot/encode categorical features if needed for ML (or leave for Spark MLlib pipeline)
  - Build a reporting table: churn rate by contract type, tenure bucket, payment method
- **Example:**
```python
from pyspark.sql.functions import when, col

df_silver = spark.table("churn_catalog.silver.customers_clean")

df_gold = (df_silver
    .withColumn("tenure_bucket",
        when(col("tenure") <= 12, "0-12mo")
        .when(col("tenure") <= 24, "13-24mo")
        .otherwise("25mo+"))
    .withColumn("churn_flag", when(col("Churn") == True, 1).otherwise(0))
)
df_gold.write.format("delta").mode("overwrite").saveAsTable("churn_catalog.gold.customer_features")

# Reporting aggregate table
df_gold.groupBy("Contract", "tenure_bucket").agg(
    {"churn_flag": "avg"}
).withColumnRenamed("avg(churn_flag)", "churn_rate") \
 .write.format("delta").mode("overwrite").saveAsTable("churn_catalog.gold.churn_rate_by_segment")
```

---

## 7. Simple ML Prediction (Beginner-Friendly)

Use **Logistic Regression** (simplest, most interpretable classifier — perfect for a first ML project) via Spark MLlib.

```python
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
```

**Why Logistic Regression first:** it's fast, interpretable (you can explain *why* a customer is flagged), and a great baseline before trying Random Forest or Gradient Boosted Trees.

---

## 8. Suggested Project Deliverables (for a portfolio/resume)

1. `BRD.md` — the business requirement document
2. `STTM.xlsx` or `.md` — the source-to-target mapping
3. 4 notebooks: `01_bronze_ingestion`, `02_silver_cleaning`, `03_gold_features`, `04_ml_model`
4. A short README explaining architecture with a diagram (Source CSV → Bronze → Silver → Gold → ML → Predictions table)
5. Optional: a Databricks SQL dashboard showing churn rate by segment and top at-risk customers

---

## 9. Natural Next Steps (once comfortable)

- Swap Logistic Regression for Random Forest/GBT and compare AUC
- Add MLflow tracking to log experiments
- Schedule the pipeline as a Databricks Job (Bronze → Silver → Gold → ML) running daily/weekly
- Add data quality checks (e.g., row count validation, null checks) between layers
