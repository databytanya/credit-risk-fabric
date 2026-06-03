# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a137bce7-2dda-4e8b-9a31-83a0a08e0244",
# META       "default_lakehouse_name": "lh_credit_risk",
# META       "default_lakehouse_workspace_id": "d99cd2ea-4b94-4e1c-8d34-4a8d427881ac",
# META       "known_lakehouses": [
# META         {
# META           "id": "a137bce7-2dda-4e8b-9a31-83a0a08e0244"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1fbd03cd-5e4f-9e7f-4fd9-d0702c0250f1",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

df = spark.table("bronze_loans_raw")
print(f"Loaded {df.count()} rows from bronze_loans_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, regexp_replace

# int_rate comes in as '13.56%' -> strip % and cast to float
df = df.withColumn("int_rate",
    regexp_replace(col("int_rate"), "%", "").cast("float"))

# dti is usually already numeric; .cast("string") first makes the %-strip safe either way
df = df.withColumn("dti",
    regexp_replace(col("dti").cast("string"), "%", "").cast("float"))  # ⬅ FIXED (safe cast)

df.select("int_rate", "dti").show(5)
print("✅ Numeric types fixed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, year

# ⬅ FIXED: 'issue_d' is text like 'Dec-2018'. Without this, time charts sort
# alphabetically (Apr, Aug, Dec...) instead of chronologically.
df = df.withColumn("issue_dt", to_date(col("issue_d"), "MMM-yyyy"))
df = df.withColumn("issue_year", year("issue_dt"))

df.select("issue_d", "issue_dt", "issue_year").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when

df = df.withColumn("loan_status_std",
    when(col("loan_status").isin([
        "Charged Off",
        "Default",
        "Does not meet the credit policy. Status:Charged Off"
    ]), "Defaulted")
    .when(col("loan_status").isin([
        "Fully Paid",
        "Does not meet the credit policy. Status:Fully Paid"   # ⬅ FIXED: also count this as paid
    ]), "Paid Off")
    .otherwise("Current"))

df.groupBy("loan_status_std").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FICO midpoint
df = df.withColumn("fico_mid",
    (col("fico_range_low") + col("fico_range_high")) / 2)

# Risk band from FICO
df = df.withColumn("risk_band",
    when(col("fico_mid") >= 750, "Low")
    .when(col("fico_mid") >= 680, "Medium")
    .otherwise("High"))

# Target: 1 if defaulted
df = df.withColumn("is_default",
    when(col("loan_status_std") == "Defaulted", 1).otherwise(0))

# ⬅ FIXED: flag loans whose outcome is actually KNOWN (finished).
# Use this so default rate is calculated over completed loans only, not 'Current' ones.
df = df.withColumn("is_completed",
    when(col("loan_status_std").isin(["Defaulted", "Paid Off"]), 1).otherwise(0))

df.groupBy("risk_band").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("silver_loans_enriched")
print("✅ Silver table created: silver_loans_enriched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_credit_risk.dbo.silver_loans_enriched LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
