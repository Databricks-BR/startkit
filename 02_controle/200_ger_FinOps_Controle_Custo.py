# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 10-JUN-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referências:
# MAGIC
# MAGIC * https://www.databricks.com/resources/demos/tutorials/governance/system-tables?itm_data=demo_center
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/uc/system_tables/uc-system-tables-explorer.png">

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando as System Tables (Unity Catalog)
# MAGIC
# MAGIC **CASO NÃO ESTEJAM HABILITADAS**
# MAGIC
# MAGIC Para habilitar as tabelas do sistema, executar o Notebook:
# MAGIC
# MAGIC https://github.com/flaviomalavazi/databricks_helpers/blob/main/notebooks/Enable%20system%20tables.py
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('uc-04-system-tables', catalog='main', schema='billing_forecast')

# COMMAND ----------

# MAGIC %md
# MAGIC https://notebooks.databricks.com/demos/uc-04-system-tables/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC
