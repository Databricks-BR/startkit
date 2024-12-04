-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Controle de Versão do Código (Pipeline)
-- MAGIC
-- MAGIC | versão | data | autor | e-mail | alterações |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 1.0 | 15-OUT-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC | Item | Descrição |
-- MAGIC | --- | --- |
-- MAGIC | **Objetivo Pipeline** | Entrada de Parâmetros |
-- MAGIC | **Camada** | Bronze |
-- MAGIC | **Databricks Run Time** | DBR 15.4 LTS |
-- MAGIC | **Linguagem** | Python, Pyspark |
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #dbutils.widgets.text("system", "salesforce")
-- MAGIC #dbutils.widgets.text("environment", "dev")
-- MAGIC #dbutils.widgets.text("layer", "bronze")
-- MAGIC #dbutils.widgets.text("catalog", "salesforce")
-- MAGIC #dbutils.widgets.text("schema", "salesforce")
-- MAGIC #dbutils.widgets.text("table", "account")
-- MAGIC #dbutils.widgets.text("dt_proc", "")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC par_system = dbutils.widgets.getArgument("system")
-- MAGIC par_env = dbutils.widgets.getArgument("environment")
-- MAGIC par_layer = dbutils.widgets.getArgument("layer")
-- MAGIC par_catalog = dbutils.widgets.getArgument("catalog")
-- MAGIC par_schema = dbutils.widgets.getArgument("schema")
-- MAGIC par_table = dbutils.widgets.getArgument("table")
-- MAGIC par_dt_proc = dbutils.widgets.getArgument("dt_proc")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC # import delta

-- COMMAND ----------

#parametros
load_path = f'{par_path}/stg/{par_system}}/{par_table}'
delta_path = f'{par_path}/bronze/{par_system}/{par_table}'

-- COMMAND ----------


