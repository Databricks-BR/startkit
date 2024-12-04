# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks DEMOS - Tutoriais
# MAGIC
# MAGIC * https://www.databricks.com/resources/demos/tutorials

# COMMAND ----------

# MAGIC %pip install dbdemos
# MAGIC import dbdemos

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('mlops-end2end')

# COMMAND ----------

dbdemos.install('lakehouse-fsi-credit')

# COMMAND ----------

# Data Governance (UC)
dbdemos.install('delta-sharing-airlines', path='./Data Governance (UC)/', overwrite = True)
dbdemos.install('uc-01-acl', path='./Data Governance (UC)/', overwrite = True)
dbdemos.install('uc-02-external-location', path='./Data Governance (UC)/', overwrite = True)
dbdemos.install('uc-03-data-lineage', path='./Data Governance (UC)/', overwrite = True)
dbdemos.install('uc-04-system-tables', path='./Data Governance (UC)/', overwrite = True)
dbdemos.install('uc-05-upgrade', path='./Data Governance (UC)/', overwrite = True)

# COMMAND ----------

dbdemos.install('delta-lake',overwrite = True)

# COMMAND ----------

dbdemos.install('feature-store',overwrite = True)

# COMMAND ----------

dbdemos.install('pandas-on-spark',overwrite = True)

# COMMAND ----------

dbdemos.install('lakehouse-iot-platform',overwrite = True)

# COMMAND ----------

dbdemos.install('mlops-end2end',overwrite = True)
