# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 15-OUT-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | Item | Descrição |
# MAGIC | --- | --- |
# MAGIC | **Objetivo Pipeline** | Ingestão BD Oracle via JDBC |
# MAGIC | **Camada** | Bronze |
# MAGIC | **Databricks Run Time** | DBR 15.4 LTS |
# MAGIC | **Linguagem** | Python, Pyspark |
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- IMAGENS -->
# MAGIC <!-- etl_parquet.png -->
# MAGIC <!-- etl_oracle.png -->
# MAGIC <!-- etl_postgre_jdbc.png -->
# MAGIC <!-- etl_postgre_fed.png -->
# MAGIC <!-- etl_salesforce.png -->
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/lcpassuncao/public/main/images/etl_oracle.png">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 1 - Instalação do DRIVER JDBC
# MAGIC
# MAGIC * Install the Databricks JDBC driver in a Java project
# MAGIC * https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver
# MAGIC * https://www.databricks.com/spark/jdbc-drivers-download

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 2 - Leitura usando o JDBC (spark.read)
# MAGIC
# MAGIC ##### Referência:
# MAGIC * https://docs.databricks.com/external-data/jdbc.html#query-databases-using-jdbc
# MAGIC

# COMMAND ----------

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
    .option("dbtable", "hr.emp") \
    .option("user", "db_user_name") \
    .option("password", "password") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use fetchsize to boost reading speed
# MAGIC Yet another JDBC parameter which controls the number of rows fetched per iteration from a remote JDBC database.
# MAGIC It defaults to low fetch size (e.g. Oracle with 10 rows).
# MAGIC
# MAGIC Aumentar para 100 reduz o número total de consultas que precisam ser executadas por um fator de 10. Os resultados do JDBC são tráfego de rede, portanto, evite números muito grandes, mas os valores ideais podem estar na casa dos milhares para muitos conjuntos de dados.
# MAGIC
# MAGIC ##### Referência:
# MAGIC * https://docs.databricks.com/external-data/jdbc.html#control-number-of-rows-fetched-per-query
# MAGIC * https://luminousmen.com/post/spark-tips-optimizing-jdbc-data-source-reads
# MAGIC * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# MAGIC

# COMMAND ----------

df = spark.read \
	.format("jdbc") \
	.option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
  .option("dbtable", "db.table") \
	.option("user", "user")\
	.option("password", "pass") \
	.option("fetchsize","1000") \
	.option("queryTimeout","0") \
	.load()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query Mode
# MAGIC
# MAGIC ##### Referência:
# MAGIC * https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_Oracle_JDBC_Howto.md

# COMMAND ----------

db_user = "system"
db_connect_string = "localhost:1521/XEPDB1" // dbserver:port/service_name
db_pass = "oracle"
myquery = "select rownum as id from dual connect by level<=10"

df = spark.read.format("jdbc").
           option("url", s"jdbc:oracle:thin:@$db_connect_string").
           option("driver", "oracle.jdbc.driver.OracleDriver").
           option("query", myquery).
           // option("dbtable", "(select * ....)"). // enclosing the query in parenthesis it's like query mode
           // option("dbtable", "myschema.mytable"). // use this to simply extract a given table 
           option("user", db_user).
           option("password", db_pass).
           option("fetchsize", 10000).
           load()

df.printSchema
df.show(5)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Data from Autonomous Database
# MAGIC
# MAGIC ##### Referência:
# MAGIC * https://docs.oracle.com/en-us/iaas/data-flow/using/spark_oracle_ds_examples.htm

# COMMAND ----------

# Loading data from autonomous database at root compartment.
# Note you don't have to provide driver class name and jdbc url.

oracle_df = spark.read \
    .format("oracle") \
    .option("adbId","ocid1.autonomousdatabase.<REALM>.[REGION][.FUTURE USE].<UNIQUE ID>") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
