-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Salesforce Rest API to Databricks
-- MAGIC
-- MAGIC * https://medium.com/@rganesh0203/salesforce-rest-api-to-databricks-9e8f835c1a32
-- MAGIC * https://github.com/simple-salesforce/simple-salesforce
-- MAGIC
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
-- MAGIC username = '<Your Salesforce Username>'
-- MAGIC password = '<Your Salesforce Password>'
-- MAGIC token = '<Your Security Token>'
-- MAGIC login_url = 'https://login.salesforce.com/services/oauth2/token'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Step 1: Authenticate and get access token
-- MAGIC auth_data = {
-- MAGIC     'grant_type': 'password',
-- MAGIC     'client_id': client_id,
-- MAGIC     'client_secret': client_secret,
-- MAGIC     'username': username,
-- MAGIC     'password': password + token
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC response = requests.post(login_url, data=auth_data)
-- MAGIC response_data = response.json()
-- MAGIC access_token = response_data['access_token']
-- MAGIC instance_url = response_data['instance_url']
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Step 2: Fetch data from Salesforce
-- MAGIC object_name = 'Account'  # Example object
-- MAGIC query = f"SELECT Id, Name FROM {object_name} LIMIT 100"
-- MAGIC
-- MAGIC headers = {
-- MAGIC     'Authorization': f'Bearer {access_token}',
-- MAGIC     'Content-Type': 'application/json'
-- MAGIC }
-- MAGIC
-- MAGIC response = requests.get(f'{instance_url}/services/data/v52.0/query/?q={query}', headers=headers)
-- MAGIC
-- MAGIC data = response.json()
-- MAGIC
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
