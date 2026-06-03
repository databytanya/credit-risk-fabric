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

df = spark.table("silver_loans_enriched")
print(f"Loaded {df.count()} rows from silver_loans_enriched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# One row per loan — main fact table for Power BI
df.write.format("delta").mode("overwrite").saveAsTable("gold_loan_summary")
print("✅ Gold table created: gold_loan_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_credit_risk.dbo.gold_loan_summary LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import count, sum, avg, round

monthly = (df.groupBy("issue_dt", "grade")          # ⬅ FIXED: group by the DATE, not the text
    .agg(
        count("*").alias("loan_count"),
        round(sum("funded_amnt"), 2).alias("total_funded"),
        round(avg("int_rate"), 4).alias("avg_int_rate"),
        sum("is_default").alias("default_count"),
        sum("is_completed").alias("completed_count")  # ⬅ FIXED: enables a correct default rate
    )
    .orderBy("issue_dt", "grade"))

monthly.write.format("delta").mode("overwrite").saveAsTable("gold_portfolio_monthly")
print("✅ Gold table created: gold_portfolio_monthly")
monthly.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

state = (df.groupBy("addr_state", "risk_band").agg(
    count("*").alias("loan_count"),
    round(avg("dti"), 4).alias("avg_dti"),
    round(avg("fico_mid"), 1).alias("avg_fico"),
    round(sum("recoveries"), 2).alias("total_recoveries")
))

state.write.format("delta").mode("overwrite").saveAsTable("gold_state_risk")
print("✅ Gold table created: gold_state_risk")
state.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
