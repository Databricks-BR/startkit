# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conexão com SALES FORCE utilizando LIB

# COMMAND ----------

pip install simple-salesforce

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parametros de conexão passados por Secrets
# MAGIC
# MAGIC Para testar, pode atribuir os valores direto nas variáveis de conexão 

# COMMAND ----------

username = dbutils.secrets.get(scope="fieldeng", key="hh-sdfc-username")
password = dbutils.secrets.get(scope="fieldeng", key="hh-sdfc-password")
consumer_key = dbutils.secrets.get(scope="fieldeng", key="hh-sdfc-consumer-key")
consumer_secret = dbutils.secrets.get(scope="fieldeng", key="hh-sdfc-consumer-secret")
security_token = dbutils.secrets.get(scope="fieldeng", key="hh-sdfc-security-token")

# COMMAND ----------

from simple_salesforce import Salesforce
sf = Salesforce(username=username, password=password, instance='databricks-2d-dev-ed.develop.my.salesforce.com', consumer_key=consumer_key, consumer_secret=consumer_secret, security_token=security_token)

# COMMAND ----------

import pandas as pd

data = sf.bulk.Account.query("SELECT Id, Name, BillingCity, BillingPostalCode FROM Account WHERE LastModifiedDate<=LAST_N_DAYS:30")
if data:
  pd_df = pd.DataFrame.from_dict(data,orient='columns').drop('attributes',axis=1)
  display(pd_df)

# COMMAND ----------

df = spark.createDataFrame(pd_df)
df.write.mode("overwrite").format("delta").saveAsTable("Salesforce_Account")
