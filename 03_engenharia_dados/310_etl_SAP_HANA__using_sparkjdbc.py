# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### REFERENCE:
# MAGIC
# MAGIC * https://www.databricks.com/blog/fastest-way-access-sap-hana-data-databricks-using-sap-sparkjdbc
# MAGIC
# MAGIC * https://tools.eu1.hana.ondemand.com/#hanatools

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/db-802-blog-imgs-1.png?v=1701957709">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Important: Configure the SAP HANA JDBC jar to the cluster (ngdbc.jar)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform a spark read using the SAP HANA JDBC connection information

# COMMAND ----------


df_vbap_table = (spark.read
  .format("jdbc")
  .option("driver","com.sap.db.jdbc.Driver")
  .option("url", "jdbc:sap://20.XX.XXX.XXX:39015/?autocommit=false")
  .option("dbtable", "ECC_DATA.VBAP")
  .option("user", "SYSTEM"  ## should be using databricks secrets instead of putting credentials in code
  .option("password", "*********")  ## should be using databricks secrets instead of putting credentials in code
  .load()
)

# COMMAND ----------

vbap_table.printSchema

# COMMAND ----------

display(vbap_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter pushdown to SAP HANA using SQL query

# COMMAND ----------


df_vbap_table_2 = (spark.read
  .format("jdbc")
  .option("driver","com.sap.db.jdbc.Driver")
  .option("url", "jdbc:sap://20.XX.XXX.XX:39015/?autocommit=false")
  .option("dbtable", "(select country, sales from ECC_DATA.VBAP where country = 'USA')")
  .option("user", "SYSTEM")  ## should be using databricks secrets instead of putting credentials in code
  .option("password", "********")  ## should be using databricks secrets instead of putting credentials in code
  .load()
)

# COMMAND ----------

display(df_vbap_table_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading data from SAP HANA Calculation View

# COMMAND ----------

calc_view = '"_SYS_BIC"."ecc-data-cv/CV_VBAP"'

df_cv_vbap_table = (spark.read
  .format("jdbc")
  .option("driver","com.sap.db.jdbc.Driver")
  .option("url", "jdbc:sap://20.XXX.XXX.X:39015/?autocommit=false")
  .option("dbtable", calc_view)
  .option("user", "SYSTEM")  ## should be using databricks secrets instead of putting credentials in code
  .option("password", "*********")  ## should be using databricks secrets instead of putting credentials in code
  .load()
)

# COMMAND ----------

display(df_cv_vbap_table)

# COMMAND ----------


