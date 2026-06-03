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

# ⬅ FIXED: read the header, then SELECT by name.
# An explicit .schema() with header=True maps fields BY POSITION, which silently
# misaligns every column unless the file order exactly matches. Selecting by name
# works regardless of column order or how many extra columns the file has.

raw = (spark.read
       .option("header", True)
       .option("inferSchema", True)
       .csv("Files/raw/lending_club/lending_club_sample.csv"))

cols = ["loan_amnt", "funded_amnt", "int_rate", "grade", "sub_grade", "emp_length",
        "annual_inc", "loan_status", "purpose", "dti", "fico_range_low",
        "fico_range_high", "issue_d", "addr_state", "installment",
        "total_pymnt", "recoveries"]

df = raw.select(*cols)   # ⬅ FIXED: pick columns by NAME

print(f"Row count: {df.count()}")
print(f"Column count: {len(df.columns)}")

# Sanity check — int_rate should look like '13.56%', grade like 'C', loan_status text.
# If grade shows numbers or numeric cols are all null, your CSV columns are misnamed.
df.select("int_rate", "grade", "loan_status", "loan_amnt").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("bronze_loans_raw")
print("✅ Bronze table created: bronze_loans_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_credit_risk.dbo.bronze_loans_raw LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
