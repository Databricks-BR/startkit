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
# MAGIC ## Data Lineage (Linhagem de Dados)
# MAGIC
# MAGIC * https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## SYSTEM TABLES.  (system.access.table_lineage)
# MAGIC
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/administration-guide/system-tables/lineage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Que tabelas são originadas desta tabela?
# MAGIC
# MAGIC SELECT DISTINCT target_table_full_name
# MAGIC
# MAGIC FROM system.access.table_lineage
# MAGIC
# MAGIC WHERE source_table_full_name = "CATALOG.SCHEMA.TABLE";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Quais consultas de usuário leem desta tabela?
# MAGIC
# MAGIC SELECT DISTINCT entity_type, entity_id, source_table_full_name
# MAGIC
# MAGIC FROM system.access.table_lineage
# MAGIC
# MAGIC WHERE source_table_full_name =  "CATALOG.SCHEMA.TABLE";;
