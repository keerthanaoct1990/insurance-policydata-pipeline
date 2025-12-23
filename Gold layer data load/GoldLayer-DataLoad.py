# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month: </b>
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.sales_by_policy_type_and_month
# MAGIC (
# MAGIC   policy_type STRING,
# MAGIC   sale_month STRING,
# MAGIC   total_premium INT,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION "/mnt/gold/sales_by_policy_type_and_month"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sales_by_policy_type_and_month AS (
# MAGIC   SELECT policy_type, month(start_date) AS sale_month, SUM(premium) AS total_premium, current_timestamp() AS updated_timestamp
# MAGIC FROM silver.policy
# MAGIC GROUP BY policy_type, sale_month
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.sales_by_policy_type_and_month AS T USING sales_by_policy_type_and_month AS S ON T.policy_type = S.policy_type WHEN MATCHED THEN UPDATE SET T.sale_month = S.sale_month, T.total_premium = S.total_premium, T.updated_timestamp = current_timestamp() WHEN NOT MATCHED THEN INSERT (policy_type, sale_month, total_premium, updated_timestamp) VALUES (S.policy_type, S.sale_month, S.total_premium, current_timestamp())
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Claims By Policy Type and Status:</b>
# MAGIC  This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.claims_by_policy_type_and_status(
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   total_claims INT,
# MAGIC   total_claim_amount INT,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION "/mnt/gold/claims_by_policy_type_and_status"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.claim

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW claims_by_policy_type_and_status AS
# MAGIC   SELECT policy.policy_type, 
# MAGIC     claim_status, COUNT(*) AS total_claims, 
# MAGIC     SUM(claim_amount) AS total_claim_amount
# MAGIC     FROM silver.policy policy INNER JOIN silver.claim 
# MAGIC     ON policy.policy_id = claim.policy_id
# MAGIC     GROUP BY policy.policy_type, claim_status
# MAGIC     HAVING policy.policy_type IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.claims_by_policy_type_and_status AS T 
# MAGIC   USING claims_by_policy_type_and_status AS S 
# MAGIC     ON T.policy_type = S.policy_type AND T.claim_status = S.claim_status 
# MAGIC     WHEN MATCHED THEN 
# MAGIC     UPDATE 
# MAGIC       SET T.total_claims = S.total_claims, T.total_claim_amount = S.total_claim_amount, T.updated_timestamp = current_timestamp()
# MAGIC     WHEN NOT MATCHED THEN 
# MAGIC     INSERT (policy_type, claim_status, total_claims, total_claim_amount,updated_timestamp) 
# MAGIC     VALUES (S.policy_type, S.claim_status, S.total_claims, S.total_claim_amount, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.claims_analysis(
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   avg_claim_amount INT,
# MAGIC   max_claim_amount INT,
# MAGIC   min_claim_amount INT,
# MAGIC   total_claims INT,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION "/mnt/gold/claims_analysis"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_claim_analysis AS
# MAGIC SELECT p.policy_type,
# MAGIC        claim_status,
# MAGIC       AVG(claim_amount) AS avg_claim_amount,
# MAGIC   MAX(claim_amount) AS max_claim_amount,
# MAGIC   MIN(claim_amount) AS min_claim_amount,
# MAGIC   COUNT(*) AS total_claims
# MAGIC FROM silver.policy p INNER JOIN silver.claim c ON p.policy_id = c.policy_id
# MAGIC GROUP BY p.policy_type, c.claim_status
# MAGIC HAVING p.policy_type IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.claims_analysis AS T 
# MAGIC   USING vw_claim_analysis AS S 
# MAGIC     ON T.policy_type = S.policy_type AND T.claim_status = S.claim_status 
# MAGIC     WHEN MATCHED THEN 
# MAGIC     UPDATE SET
# MAGIC       T.avg_claim_amount = S.avg_claim_amount,
# MAGIC       T.max_claim_amount = S.max_claim_amount,
# MAGIC       T.min_claim_amount = S.min_claim_amount,
# MAGIC       T.total_claims = S.total_claims,
# MAGIC       T.updated_timestamp = current_timestamp()
# MAGIC     WHEN NOT MATCHED 
# MAGIC     THEN INSERT (policy_type, claim_status, avg_claim_amount, max_claim_amount, min_claim_amount, total_claims, updated_timestamp) 
# MAGIC     VALUES (S.policy_type, S.claim_status, S.avg_claim_amount, S.max_claim_amount, S.min_claim_amount, S.total_claims, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.claims_analysis
