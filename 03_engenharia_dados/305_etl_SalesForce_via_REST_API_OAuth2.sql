-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC <!-- IMAGENS -->
-- MAGIC <!-- etl_parquet.png -->
-- MAGIC <!-- etl_oracle.png -->
-- MAGIC <!-- etl_postgre_jdbc.png -->
-- MAGIC <!-- etl_postgre_fed.png -->
-- MAGIC <!-- etl_salesforce.png -->
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/etl_salesforce.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Salesforce Rest API to Databricks
-- MAGIC
-- MAGIC * https://medium.com/@rganesh0203/salesforce-rest-api-to-databricks-9e8f835c1a32
-- MAGIC *  https://github.com/simple-salesforce/simple-salesforce
-- MAGIC * https://github.com/simple-salesforce/simple-salesforce/blob/master/docs/user_guide/examples.rst
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import requests
-- MAGIC import json
-- MAGIC from pyspark.sql import SparkSession

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Salesforce credentials
-- MAGIC client_id = '<Your Consumer Key>'
-- MAGIC client_secret = '<Your Consumer Secret>'
-- MAGIC token = '<Your Security Token>'
-- MAGIC login_url = 'https://login.salesforce.com/services/oauth2/token'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from simple_salesforce import Salesforce
-- MAGIC
-- MAGIC sf = Salesforce(instance='na1.salesforce.com', session_id='', client_id='MY_CONNECTED_APP_ID', client_secret='MY_CONNECTED_APP_SECRET', refresh_token='REFRESH_TOKEN_PROVIDED_DURING_A_PRIOR_AUTH')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert JSON to Spark DataFrame
-- MAGIC records = data['records']
-- MAGIC spark = SparkSession.builder.appName('SalesforceData').getOrCreate()
-- MAGIC df = spark.createDataFrame(records)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Show data
-- MAGIC df.display()
