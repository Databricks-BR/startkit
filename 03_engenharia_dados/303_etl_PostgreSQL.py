# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC <!-- IMAGENS -->
# MAGIC <!-- etl_parquet.png -->
# MAGIC <!-- etl_oracle.png -->
# MAGIC <!-- etl_postgre_jdbc.png -->
# MAGIC <!-- etl_postgre_fed.png -->
# MAGIC <!-- etl_salesforce.png -->
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/etl_postgre_jdbc.png">

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/flights'))

# COMMAND ----------

# DBTITLE 1,Create trivial dataset for testing
df = spark.read.option("header","true").csv('dbfs:/databricks-datasets/flights/departuredelays.csv')
df.display()

# COMMAND ----------

hostname = dbutils.secrets.get(scope="daiwt_munich", key="hostname")
port = dbutils.secrets.get(scope="daiwt_munich", key="port")
database = dbutils.secrets.get(scope="daiwt_munich", key="database")
username = dbutils.secrets.get(scope="daiwt_munich", key="username")
password = dbutils.secrets.get(scope="daiwt_munich", key="password")

# COMMAND ----------

# DBTITLE 1,Construct Postgres JDBC URL
postgres_url = f"jdbc:postgresql://{hostname}:{port}/{database}?user={username}&password={password}"

# COMMAND ----------

# DBTITLE 1,Write to Postgres
(df.write
  .format("postgresql")
  .option("dbtable", "flights_departuredelays")
  .option("host", hostname)
  .option("port", port) 
  .option("database", database)
  .option("user", username)
  .option("password", password)
  .mode("overwrite")
  .save())

# COMMAND ----------

df_res = (spark.read
  .format("postgresql")
  .option("query", "delete from public.flights_departuredelays")
  .option("host", hostname)
  .option("port", port) 
  .option("database", database)
  .option("user", username)
  .option("password", password)
  )
